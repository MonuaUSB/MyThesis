import psycopg2

def create_tables():
    try:
        connection = psycopg2.connect(
            host="localhost",
            user="root",  
            password="",
            database="ssb",
            port='26257'
        )
        cursor = connection.cursor()

        create_part_table = """
        CREATE TABLE IF NOT EXISTS part (
          p_partkey INT PRIMARY KEY,
          p_name VARCHAR(23) NOT NULL,
          p_mfgr VARCHAR(7) NOT NULL,
          p_category VARCHAR(8) NOT NULL,
          p_brand VARCHAR(10) NOT NULL,
          p_size INT NOT NULL,
          p_container VARCHAR(11) NOT NULL
        );
        """

        create_dates_table = """
        CREATE TABLE IF NOT EXISTS dates (
          d_datekey INT PRIMARY KEY,
          d_date VARCHAR(20) NOT NULL,
          d_dayofweek VARCHAR(10) NOT NULL,
          d_month VARCHAR(11) NOT NULL,
          d_year INT NOT NULL,
          d_yearmonthnum INT NOT NULL,
          d_yearmonth VARCHAR(9) NOT NULL,
          d_daynuminweek INT NOT NULL,
          d_daynuminmonth INT NOT NULL,
          d_daynuminyear INT NOT NULL,
          d_monthnuminyear INT NOT NULL,
          d_weeknuminyear INT NOT NULL,
          d_sellingseason VARCHAR(14) NOT NULL,
          d_lastdayinweekfl INT NOT NULL,
          d_lastdayinmonthfl INT NOT NULL,
          d_holidayfl INT NOT NULL,
          d_weekdayfl INT NOT NULL
        );
        """

        create_supplier_table = """
        CREATE TABLE IF NOT EXISTS supplier (
          s_suppkey INT PRIMARY KEY,
          s_name VARCHAR(26) NOT NULL,
          s_address VARCHAR(26) NOT NULL,
          s_city VARCHAR(11) NOT NULL,
          s_nation VARCHAR(16) NOT NULL,
          s_region VARCHAR(13) NOT NULL,
          s_phone VARCHAR(16) NOT NULL
        );
        """

        create_customer_table = """
        CREATE TABLE IF NOT EXISTS customer (
          c_custkey INT PRIMARY KEY,
          c_name VARCHAR(26) NOT NULL,
          c_address VARCHAR(41) NOT NULL,
          c_city VARCHAR(11) NOT NULL,
          c_nation VARCHAR(16) NOT NULL,
          c_region VARCHAR(13) NOT NULL,
          c_phone VARCHAR(16) NOT NULL,
          c_mktsegment VARCHAR(11) NOT NULL
        );
        """

        create_lineorder_table = """
        CREATE TABLE IF NOT EXISTS lineorder (
          lo_orderkey INT NOT NULL,
          lo_linenumber INT NOT NULL,
          lo_custkey INT NOT NULL,
          lo_partkey INT NOT NULL,
          lo_suppkey INT NOT NULL,
          lo_orderdate INT NOT NULL,
          lo_orderpriority VARCHAR(16) NOT NULL,
          lo_shippriority INT NOT NULL,
          lo_quantity INT NOT NULL,
          lo_extendedprice INT NOT NULL,
          lo_ordtotalprice INT NOT NULL,
          lo_discount INT NOT NULL,
          lo_revenue INT NOT NULL,
          lo_supplycost INT NOT NULL,
          lo_tax INT NOT NULL,
          lo_commitdate INT NOT NULL,
          lo_shipmode VARCHAR(11) NOT NULL,
          PRIMARY KEY (lo_orderkey, lo_linenumber)
        );
        """

        cursor.execute(create_part_table)
        cursor.execute(create_dates_table)
        cursor.execute(create_supplier_table)
        cursor.execute(create_customer_table)
        cursor.execute(create_lineorder_table)

        connection.commit()
        cursor.close()
        connection.close()

    except psycopg2.Error as e:
        print("Error while connecting to PostgreSQL:", e)

if __name__ == "__main__":
    create_tables()
