import csv, random, datetime

random.seed(1)
start = datetime.datetime(2024, 1, 1)

with open("data/orders.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["order_id","product_id","quantity","price","order_date"])
    
    for oid in range(1, 2001):
        pid = random.randint(1,100)
        qty = random.randint(1,5)
        price = round(random.uniform(5,150),2)
        dt = start + datetime.timedelta(days=random.randint(0,200))
        w.writerow([oid,pid,qty,price,dt.isoformat()])

print("OK: data/orders.csv creado")