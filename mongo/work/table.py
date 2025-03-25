from pymongo import MongoClient

def create_collections():
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["ssb2"]

        # Create collections
        db.create_collection("part")
        db.create_collection("dates")
        db.create_collection("supplier")
        db.create_collection("customer")
        db.create_collection("lineorder")

        # Create indexes for the collections
        db.part.create_index("p_partkey", unique=True)
        db.dates.create_index("d_datekey", unique=True)
        db.supplier.create_index("s_suppkey", unique=True)
        db.customer.create_index("c_custkey", unique=True)
        db.lineorder.create_index([("lo_orderkey", 1), ("lo_linenumber", 1)], unique=True)

        print("Collections and indexes created successfully.")

    except Exception as e:
        print("Error while connecting to MongoDB:", e)

def insert_sample_data():
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["ssb2"]

        # Sample data for 'part' collection
        part_data = [
            {
                "p_partkey": 1,
                "p_name": "Part1",
                "p_mfgr": "Manufacturer1",
                "p_category": "Category1",
                "p_brand": "Brand1",
                "p_size": 10,
                "p_container": "Container1"
            }
            # Add more documents as needed
        ]

        # Sample data for 'dates' collection
        dates_data = [
            {
                "d_datekey": 1,
                "d_date": "2024-01-01",
                "d_dayofweek": "Monday",
                "d_month": "January",
                "d_year": 2024,
                "d_yearmonthnum": 202401,
                "d_yearmonth": "2024-01",
                "d_daynuminweek": 1,
                "d_daynuminmonth": 1,
                "d_daynuminyear": 1,
                "d_monthnuminyear": 1,
                "d_weeknuminyear": 1,
                "d_sellingseason": "Q1",
                "d_lastdayinweekfl": 0,
                "d_lastdayinmonthfl": 0,
                "d_holidayfl": 1,
                "d_weekdayfl": 1
            }
            # Add more documents as needed
        ]

        # Sample data for 'supplier' collection
        supplier_data = [
            {
                "s_suppkey": 1,
                "s_name": "Supplier1",
                "s_address": "Address1",
                "s_city": "City1",
                "s_nation": "Nation1",
                "s_region": "Region1",
                "s_phone": "123-456-7890"
            }
            # Add more documents as needed
        ]

        # Sample data for 'customer' collection
        customer_data = [
            {
                "c_custkey": 1,
                "c_name": "Customer1",
                "c_address": "Address1",
                "c_city": "City1",
                "c_nation": "Nation1",
                "c_region": "Region1",
                "c_phone": "123-456-7890",
                "c_mktsegment": "Segment1"
            }
            # Add more documents as needed
        ]

        # Sample data for 'lineorder' collection
        lineorder_data = [
            {
                "lo_orderkey": 1,
                "lo_linenumber": 1,
                "lo_custkey": 1,
                "lo_partkey": 1,
                "lo_suppkey": 1,
                "lo_orderdate": 1,
                "lo_orderpriority": "High",
                "lo_shippriority": 1,
                "lo_quantity": 100,
                "lo_extendedprice": 1000,
                "lo_ordtotalprice": 1100,
                "lo_discount": 10,
                "lo_revenue": 900,
                "lo_supplycost": 500,
                "lo_tax": 50,
                "lo_commitdate": 1,
                "lo_shipmode": "Air"
            }
            # Add more documents as needed
        ]

        # Insert sample data into collections
        db.part.insert_many(part_data)
        db.dates.insert_many(dates_data)
        db.supplier.insert_many(supplier_data)
        db.customer.insert_many(customer_data)
        db.lineorder.insert_many(lineorder_data)

        print("Sample data inserted successfully.")

    except Exception as e:
        print("Error while inserting data into MongoDB:", e)

if __name__ == "__main__":
    create_collections()
    insert_sample_data()
