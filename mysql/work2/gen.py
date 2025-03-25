import os
import mysql.connector
from faker import Faker
import random
import time
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.abspath(__file__))
SSB_DATA_DIR = os.path.join(ROOT, "ssb-data/")

def generate_data(record_count):
    fake = Faker()

    if not os.path.exists(SSB_DATA_DIR):
        os.makedirs(SSB_DATA_DIR)

    # Генерация part.tbl
    with open(os.path.join(SSB_DATA_DIR, "part.tbl"), "w") as f:
        for i in range(1, record_count + 1):
            p_partkey = i
            p_name = fake.word()
            p_mfgr = fake.word()[:7]
            p_category = fake.word()[:8]
            p_brand = fake.word()[:10]
            p_size = random.randint(1, 50)
            p_container = fake.word()[:11]
            f.write(f"{p_partkey}|{p_name}|{p_mfgr}|{p_category}|{p_brand}|{p_size}|{p_container}\n")

    # Генерация date.tbl
    with open(os.path.join(SSB_DATA_DIR, "date.tbl"), "w") as f:
        for i in range(1, record_count + 1):
            d_datekey = i
            d_date = fake.date_between(start_date="-10y", end_date="today")
            d_dayofweek = fake.day_of_week()
            d_month = fake.month_name()
            d_year = d_date.year
            d_yearmonthnum = d_date.month
            d_yearmonth = f"{d_date.year}-{d_date.month:02}"
            d_daynuminweek = d_date.isoweekday()
            d_daynuminmonth = d_date.day
            d_daynuminyear = d_date.timetuple().tm_yday
            d_monthnuminyear = d_date.month
            d_weeknuminyear = d_date.isocalendar()[1]
            d_sellingseason = fake.word()[:14]
            d_lastdayinweekfl = random.randint(0, 1)
            d_lastdayinmonthfl = random.randint(0, 1)
            d_holidayfl = random.randint(0, 1)
            d_weekdayfl = random.randint(0, 1)
            f.write(f"{d_datekey}|{d_date}|{d_dayofweek}|{d_month}|{d_year}|{d_yearmonthnum}|{d_yearmonth}|{d_daynuminweek}|{d_daynuminmonth}|{d_daynuminyear}|{d_monthnuminyear}|{d_weeknuminyear}|{d_sellingseason}|{d_lastdayinweekfl}|{d_lastdayinmonthfl}|{d_holidayfl}|{d_weekdayfl}\n")

    # Генерация supplier.tbl
    with open(os.path.join(SSB_DATA_DIR, "supplier.tbl"), "w") as f:
        for i in range(1, record_count + 1):
            s_suppkey = i
            s_name = fake.company()[:26]
            s_address = fake.address()[:26]
            s_city = fake.city()[:11]
            s_nation = fake.country()[:16]
            s_region = fake.state()[:13]
            s_phone = fake.phone_number()[:16]
            f.write(f"{s_suppkey}|{s_name}|{s_address}|{s_city}|{s_nation}|{s_region}|{s_phone}\n")

    # Генерация customer.tbl
    with open(os.path.join(SSB_DATA_DIR, "customer.tbl"), "w") as f:
        for i in range(1, record_count + 1):
            c_custkey = i
            c_name = fake.name()[:26]
            c_address = fake.address()[:41]
            c_city = fake.city()[:11]
            c_nation = fake.country()[:16]
            c_region = fake.state()[:13]
            c_phone = fake.phone_number()[:16]
            c_mktsegment = fake.word()[:11]
            f.write(f"{c_custkey}|{c_name}|{c_address}|{c_city}|{c_nation}|{c_region}|{c_phone}|{c_mktsegment}\n")

    # Генерация lineorder.tbl
    with open(os.path.join(SSB_DATA_DIR, "lineorder.tbl"), "w") as f:
        for i in range(1, record_count * 10):  # Увеличение количества записей lineorder
            lo_orderkey = i
            lo_linenumber = random.randint(1, 7)
            lo_custkey = random.randint(1, record_count)
            lo_partkey = random.randint(1, record_count)
            lo_suppkey = random.randint(1, record_count)
            lo_orderdate = random.randint(1, record_count)
            lo_orderpriority = fake.word()[:16]
            lo_shippriority = random.randint(1, 7)
            lo_quantity = random.randint(1, 50)
            lo_extendedprice = random.randint(1, 10000)
            lo_ordtotalprice = random.randint(1, 10000)
            lo_discount = random.randint(0, 10)
            lo_revenue = random.randint(1, 10000)
            lo_supplycost = random.randint(1, 10000)
            lo_tax = random.randint(0, 10)
            lo_commitdate = random.randint(1, record_count)
            lo_shipmode = fake.word()[:11]
            f.write(f"{lo_orderkey}|{lo_linenumber}|{lo_custkey}|{lo_partkey}|{lo_suppkey}|{lo_orderdate}|{lo_orderpriority}|{lo_shippriority}|{lo_quantity}|{lo_extendedprice}|{lo_ordtotalprice}|{lo_discount}|{lo_revenue}|{lo_supplycost}|{lo_tax}|{lo_commitdate}|{lo_shipmode}\n")

def load_data():
    connection = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="123",
        database="ssb2"
    )
    cursor = connection.cursor()

    # Очистка таблиц перед загрузкой новых данных
    cursor.execute("DELETE FROM part")
    cursor.execute("DELETE FROM dates")
    cursor.execute("DELETE FROM supplier")
    cursor.execute("DELETE FROM customer")
    cursor.execute("DELETE FROM lineorder")

    print("Загрузка данных в таблицу: part")
    with open(os.path.join(SSB_DATA_DIR, "part.tbl"), "r") as f:
        data = f.read().splitlines()
        for line in data:
            fields = line.split("|")
            if len(fields) != 7:  # Проверка на корректное количество столбцов
                print(f"Ошибка в part.tbl: {fields}")
                continue
            run_sql(cursor, f"""
INSERT INTO part (p_partkey, p_name, p_mfgr, p_category, p_brand, p_size, p_container)
VALUES ({', '.join([f'"{field}"' for field in fields])});
""")
    connection.commit()

    print("Загрузка данных в таблицу: dates")
    with open(os.path.join(SSB_DATA_DIR, "date.tbl"), "r") as f:
        data = f.read().splitlines()
        for line in data:
            fields = line.split("|")
            if len(fields) != 17:  # Проверка на корректное количество столбцов
                print(f"Ошибка в date.tbl: {fields}")
                continue
            run_sql(cursor, f"""
INSERT INTO dates (d_datekey, d_date, d_dayofweek, d_month, d_year, d_yearmonthnum, d_yearmonth, d_daynuminweek, d_daynuminmonth, d_daynuminyear, d_monthnuminyear, d_weeknuminyear, d_sellingseason, d_lastdayinweekfl, d_lastdayinmonthfl, d_holidayfl, d_weekdayfl)
VALUES ({', '.join([f'"{field}"' for field in fields])});
""")
    connection.commit()

    print("Загрузка данных в таблицу: supplier")
    with open(os.path.join(SSB_DATA_DIR, "supplier.tbl"), "r") as f:
        data = f.read().splitlines()
        for line in data:
            fields = line.split("|")
            if len(fields) != 7:  # Проверка на корректное количество столбцов
                print(f"Ошибка в supplier.tbl: {fields}")
                continue
            run_sql(cursor, f"""
INSERT INTO supplier (s_suppkey, s_name, s_address, s_city, s_nation, s_region, s_phone)
VALUES ({', '.join([f'"{field}"' for field in fields])});
""")
    connection.commit()

    print("Загрузка данных в таблицу: customer")
    with open(os.path.join(SSB_DATA_DIR, "customer.tbl"), "r") as f:
        data = f.read().splitlines()
        for line in data:
            fields = line.split("|")
            if len(fields) != 8:  # Проверка на корректное количество столбцов
                print(f"Ошибка в customer.tbl: {fields}")
                continue
            run_sql(cursor, f"""
INSERT INTO customer (c_custkey, c_name, c_address, c_city, c_nation, c_region, c_phone, c_mktsegment)
VALUES ({', '.join([f'"{field}"' for field in fields])});
""")
    connection.commit()

    print("Загрузка данных в таблицу: lineorder")
    with open(os.path.join(SSB_DATA_DIR, "lineorder.tbl"), "r") as f:
        data = f.read().splitlines()
        for line in data:
            fields = line.split("|")
            if len(fields) != 17:  # Проверка на корректное количество столбцов
                print(f"Ошибка в lineorder.tbl: {fields}")
                continue
            run_sql(cursor, f"""
INSERT INTO lineorder (lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice, lo_ordtotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode)
VALUES ({', '.join([f'"{field}"' for field in fields])});
""")
    connection.commit()

    cursor.close()
    connection.close()

def run_sql(cursor, sql):
    print(sql)
    cursor.execute(sql)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--generate", action="store_true", help="генерация файлов данных")
    parser.add_argument("--load", action="store_true", help="загрузка данных в базу данных")
    parser.add_argument("--records", type=int, default=100, help="количество записей для генерации")
    args = parser.parse_args()

    if args.generate:
        generate_data(args.records)
    if args.load:
        load_data()
