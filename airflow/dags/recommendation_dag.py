from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, json
import duckdb
import redis as redis_lib
from kafka import KafkaConsumer

default_args = {
    "owner": "student",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/warehouse.duckdb")

def read_aggregated_clicks(**context):
    consumer = KafkaConsumer(
        "aggregated_clicks",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"airflow-reco-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",          # fijo
        auto_offset_reset="earliest",     # lee desde el inicio si no hay offsets guardados
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=5000,
    )
    agg = {}
    for msg in consumer:
        pid = int(msg.value["product_id"])
        cnt = int(msg.value["count"])
        agg[pid] = agg.get(pid, 0) + cnt
    consumer.close()
    context["ti"].xcom_push(key="aggregates", value=agg)
    print("Read aggregates:", len(agg))

def join_with_orders_duckdb(**context):
    agg = context["ti"].xcom_pull(task_ids="read_kafka", key="aggregates") or {}

    con = duckdb.connect(DUCKDB_PATH)

    # Asegurar orders cargada (si ya existe y tiene datos, no pasa nada)
    con.execute("""
    CREATE TABLE IF NOT EXISTS orders AS
    SELECT * FROM read_csv_auto('/opt/airflow/data/orders.csv');
    """)

    # 1) Crear una tabla temporal con los agregados (pid, clicks)
    con.execute("DROP TABLE IF EXISTS agg_clicks;")
    con.execute("CREATE TABLE agg_clicks(pid BIGINT, click_count BIGINT);")
    if agg:
        con.executemany(
            "INSERT INTO agg_clicks VALUES (?, ?);",
            [(int(pid), int(clicks)) for pid, clicks in agg.items()]
        )

    # 2) Agregar orders en SQL + LEFT JOIN con agg_clicks
    rows = con.execute("""
      WITH orders_agg AS (
        SELECT
          CAST(product_id AS BIGINT) AS pid,
          SUM(CAST(quantity AS BIGINT)) AS total_orders,
          AVG(CAST(price AS DOUBLE)) AS avg_price
        FROM orders
        GROUP BY 1
      )
      SELECT
        a.pid,
        a.click_count,
        COALESCE(o.total_orders, 0) AS total_orders,
        COALESCE(o.avg_price, 0.0) AS avg_price,
        (a.click_count * 0.7 + COALESCE(o.total_orders, 0) * 0.3) AS score
      FROM agg_clicks a
      LEFT JOIN orders_agg o USING(pid)
      ORDER BY score DESC
      LIMIT 10
    """).fetchall()

    top10 = [
        {
            "product_id": int(r[0]),
            "click_count": int(r[1]),
            "total_orders": int(r[2]),
            "avg_price": float(r[3]),
            "score": float(r[4]),
        }
        for r in rows
    ]

    context["ti"].xcom_push(key="top10", value=top10)
    print("Top10:", top10[:3])


def write_outputs(**context):

    top10 = context["ti"].xcom_pull(task_ids="join_orders", key="top10") or []
    if not top10:
        # No hay datos -> no escribimos nada y salimos OK
        return
    # Redis directo
    r = redis_lib.Redis(host=os.getenv("REDIS_HOST", "redis"),
                        port=int(os.getenv("REDIS_PORT", "6379")),
                        decode_responses=True)
    # Limpia resultados anteriores para que el hash sea EXACTAMENTE el top10 actual
    r.delete("top_products")
    r.hset("top_products", mapping={str(i["product_id"]): json.dumps(i) for i in top10})

    # DuckDB
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
      CREATE TABLE IF NOT EXISTS top_products(
        product_id BIGINT,
        click_count BIGINT,
        total_orders BIGINT,
        avg_price DOUBLE,
        score DOUBLE,
        updated_at TIMESTAMP
      );
    """)
    for i in top10:
        con.execute("INSERT INTO top_products VALUES (?, ?, ?, ?, ?, now());",
                    [i["product_id"], i["click_count"], i["total_orders"], i["avg_price"], i["score"]])

with DAG(
    "recommendation_pipeline",
    default_args=default_args,
    schedule_interval=None,  # manual para probar
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="read_kafka", python_callable=read_aggregated_clicks)
    t2 = PythonOperator(task_id="join_orders", python_callable=join_with_orders_duckdb)
    t3 = PythonOperator(task_id="write_outputs", python_callable=write_outputs)

    t1 >> t2 >> t3