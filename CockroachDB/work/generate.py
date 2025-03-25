import os
import csv
from faker import Faker
import random
import argparse

ROOT = os.path.dirname(os.path.abspath(__file__))
SSB_DATA_DIR = os.path.join(ROOT, "ssb-data/")

def generate_data(record_count):
    fake = Faker()

    if not os.path.exists(SSB_DATA_DIR):
        os.makedirs(SSB_DATA_DIR)

    def generate_and_write(filename, headers, data_generator):
        with open(os.path.join(SSB_DATA_DIR, filename), "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(data_generator())

    # Генерация данных для part.csv
    def part_data():
        for i in range(1, record_count + 1):
            yield [i, fake.word(), fake.word()[:7], fake.word()[:8], fake.word()[:10], random.randint(1, 50), fake.word()[:11]]
    generate_and_write("part.csv", ['p_partkey', 'p_name', 'p_mfgr', 'p_category', 'p_brand', 'p_size', 'p_container'], part_data)

    # Генерация данных для date.csv
    def date_data():
        for i in range(1, record_count + 1):
            d_date = fake.date_between(start_date="-10y", end_date="today")
            yield [i, d_date, fake.day_of_week(), fake.month_name(), d_date.year, d_date.month, f"{d_date.year}-{d_date.month:02}", d_date.isoweekday(), d_date.day, d_date.timetuple().tm_yday, d_date.month, d_date.isocalendar()[1], fake.word()[:14], random.randint(0, 1), random.randint(0, 1), random.randint(0, 1), random.randint(0, 1)]
    generate_and_write("date.csv", ['d_datekey', 'd_date', 'd_dayofweek', 'd_month', 'd_year', 'd_yearmonthnum', 'd_yearmonth', 'd_daynuminweek', 'd_daynuminmonth', 'd_daynuminyear', 'd_monthnuminyear', 'd_weeknuminyear', 'd_sellingseason', 'd_lastdayinweekfl', 'd_lastdayinmonthfl', 'd_holidayfl', 'd_weekdayfl'], date_data)

    # Генерация данных для supplier.csv
    def supplier_data():
        for i in range(1, record_count + 1):
            yield [i, fake.company()[:26], fake.address().replace('\n', ' ')[:26], fake.city()[:11], fake.country()[:16], fake.state()[:13], fake.phone_number()[:16]]
    generate_and_write("supplier.csv", ['s_suppkey', 's_name', 's_address', 's_city', 's_nation', 's_region', 's_phone'], supplier_data)

    # Генерация данных для customer.csv
    def customer_data():
        for i in range(1, record_count + 1):
            yield [i, fake.name()[:26], fake.address().replace('\n', ' ')[:41], fake.city()[:11], fake.country()[:16], fake.state()[:13], fake.phone_number()[:16], fake.word()[:11]]
    generate_and_write("customer.csv", ['c_custkey', 'c_name', 'c_address', 'c_city', 'c_nation', 'c_region', 'c_phone', 'c_mktsegment'], customer_data)

    # Генерация данных для lineorder.csv
    def lineorder_data():
        for i in range(1, record_count * 10):
            yield [i, random.randint(1, 7), random.randint(1, record_count), random.randint(1, record_count), random.randint(1, record_count), random.randint(1, record_count), fake.word()[:16], random.randint(1, 7), random.randint(1, 50), random.randint(1, 10000), random.randint(1, 10000), random.randint(0, 10), random.randint(1, 10000), random.randint(1, 10000), random.randint(0, 10), random.randint(1, record_count), fake.word()[:11]]
    generate_and_write("lineorder.csv", ['lo_orderkey', 'lo_linenumber', 'lo_custkey', 'lo_partkey', 'lo_suppkey', 'lo_orderdate', 'lo_orderpriority', 'lo_shippriority', 'lo_quantity', 'lo_extendedprice', 'lo_ordtotalprice', 'lo_discount', 'lo_revenue', 'lo_supplycost', 'lo_tax', 'lo_commitdate', 'lo_shipmode'], lineorder_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=100, help="количество записей для генерации")
    args = parser.parse_args()

    generate_data(args.records)