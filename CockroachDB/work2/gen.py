import os
import csv
import psycopg2
from faker import Faker
import random

ROOT = os.path.dirname(os.path.abspath(__file__))
SSB_DATA_DIR = os.path.join(ROOT, "ssb-data/")

def generate_data(record_count):
    fake = Faker()

    if not os.path.exists(SSB_DATA_DIR):
        os.makedirs(SSB_DATA_DIR)

    # Генерация part.csv
    with open(os.path.join(SSB_DATA_DIR, "part.csv"), "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['p_partkey', 'p_name', 'p_mfgr', 'p_category', 'p_brand', 'p_size', 'p_container'])
        for i in range(1, record_count + 1):
            p_partkey = i
            p_name = fake.word()
            p_mfgr = fake.word()[:7]
            p_category = fake.word()[:8]
            p_brand = fake.word()[:10]
            p_size = random.randint(1, 50)
            p_container = fake.word()[:11]
            writer.writerow([p_partkey, p_name, p_mfgr, p_category, p_brand, p_size, p_container])

    # Генерация date.csv
    with open(os.path.join(SSB_DATA_DIR, "date.csv"), "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['d_datekey', 'd_date', 'd_dayofweek', 'd_month', 'd_year', 'd_yearmonthnum', 'd_yearmonth', 'd_daynuminweek', 'd_daynuminmonth', 'd_daynuminyear', 'd_monthnuminyear', 'd_weeknuminyear', 'd_sellingseason', 'd_lastdayinweekfl', 'd_lastdayinmonthfl', 'd_holidayfl', 'd_weekdayfl'])
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
            writer.writerow([d_datekey, d_date, d_dayofweek, d_month, d_year, d_yearmonthnum, d_yearmonth, d_daynuminweek, d_daynuminmonth, d_daynuminyear, d_monthnuminyear, d_weeknuminyear, d_sellingseason, d_lastdayinweekfl, d_lastdayinmonthfl, d_holidayfl, d_weekdayfl])

    # Генерация supplier.csv
    with open(os.path.join(SSB_DATA_DIR, "supplier.csv"), "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['s_suppkey', 's_name', 's_address', 's_city', 's_nation', 's_region', 's_phone'])
        for i in range(1, record_count + 1):
            s_suppkey = i
            s_name = fake.company()[:26]
            s_address = fake.address().replace('\n', ' ')[:26]
            s_city = fake.city()[:11]
            s_nation = fake.country()[:16]
            s_region = fake.state()[:13]
            s_phone = fake.phone_number()[:16]
            writer.writerow([s_suppkey, s_name, s_address, s_city, s_nation, s_region, s_phone])

    # Генерация customer.csv
    with open(os.path.join(SSB_DATA_DIR, "customer.csv"), "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['c_custkey', 'c_name', 'c_address', 'c_city', 'c_nation', 'c_region', 'c_phone', 'c_mktsegment'])
        for i in range(1, record_count + 1):
            c_custkey = i
            c_name = fake.name()[:26]
            c_address = fake.address().replace('\n', ' ')[:41]
            c_city = fake.city()[:11]
            c_nation = fake.country()[:16]
            c_region = fake.state()[:13]
            c_phone = fake.phone_number()[:16]
            c_mktsegment = fake.word()[:11]
            writer.writerow([c_custkey, c_name, c_address, c_city, c_nation, c_region, c_phone, c_mktsegment])

    # Генерация lineorder.csv
    with open(os.path.join(SSB_DATA_DIR, "lineorder.csv"), "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['lo_orderkey', 'lo_linenumber', 'lo_custkey', 'lo_partkey', 'lo_suppkey', 'lo_orderdate', 'lo_orderpriority', 'lo_shippriority', 'lo_quantity', 'lo_extendedprice', 'lo_ordtotalprice', 'lo_discount', 'lo_revenue', 'lo_supplycost', 'lo_tax', 'lo_commitdate', 'lo_shipmode'])
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
            writer.writerow([lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice, lo_ordtotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode])

def load_data():
    connection = psycopg2.connect(
            host="localhost",
            user="root",  
            password="",
            database="ssb2",
            port='26257'
    )
    cursor = connection.cursor()

    # Очистка таблиц перед загрузкой новых данных
    cursor.execute("TRUNCATE TABLE part")
    cursor.execute("TRUNCATE TABLE dates")
    cursor.execute("TRUNCATE TABLE supplier")
    cursor.execute("TRUNCATE TABLE customer")
    cursor.execute("TRUNCATE TABLE lineorder")

    def load_csv_to_table(filename, table_name, columns):
        print(f"Загрузка данных в таблицу: {table_name}")
        with open(os.path.join(SSB_DATA_DIR, filename), 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Пропустить заголовок
            for row in reader:
                placeholders = ','.join(['%s'] * len(row))
                insert_query = f'INSERT INTO {table_name} ({",".join(columns)}) VALUES ({placeholders})'
                cursor.execute(insert_query, row)
        connection.commit()

    load_csv_to_table('part.csv', 'part', ['p_partkey', 'p_name', 'p_mfgr', 'p_category', 'p_brand', 'p_size', 'p_container'])
    load_csv_to_table('date.csv', 'dates', ['d_datekey', 'd_date', 'd_dayofweek', 'd_month', 'd_year', 'd_yearmonthnum', 'd_yearmonth', 'd_daynuminweek', 'd_daynuminmonth', 'd_daynuminyear', 'd_monthnuminyear', 'd_weeknuminyear', 'd_sellingseason', 'd_lastdayinweekfl', 'd_lastdayinmonthfl', 'd_holidayfl', 'd_weekdayfl'])
    load_csv_to_table('supplier.csv', 'supplier', ['s_suppkey', 's_name', 's_address', 's_city', 's_nation', 's_region', 's_phone'])
    load_csv_to_table('customer.csv', 'customer', ['c_custkey', 'c_name', 'c_address', 'c_city', 'c_nation', 'c_region', 'c_phone', 'c_mktsegment'])
    load_csv_to_table('lineorder.csv', 'lineorder', ['lo_orderkey', 'lo_linenumber', 'lo_custkey', 'lo_partkey', 'lo_suppkey', 'lo_orderdate', 'lo_orderpriority', 'lo_shippriority', 'lo_quantity', 'lo_extendedprice', 'lo_ordtotalprice', 'lo_discount', 'lo_revenue', 'lo_supplycost', 'lo_tax', 'lo_commitdate', 'lo_shipmode'])

    cursor.close()
    connection.close()

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
