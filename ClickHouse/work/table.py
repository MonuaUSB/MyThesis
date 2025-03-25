from clickhouse_driver import Client

def create_tables():
    client = Client(host='127.0.0.1')

    create_part_table = """
    CREATE TABLE IF NOT EXISTS ssb.part (
      p_partkey Int32,
      p_name String,
      p_mfgr String,
      p_category String,
      p_brand String,
      p_size Int32,
      p_container String,
      PRIMARY KEY (p_partkey)
    ) ENGINE = MergeTree()
    ORDER BY p_partkey;
    """

    create_dates_table = """
    CREATE TABLE IF NOT EXISTS ssb.dates (
      d_datekey Int32,
      d_date String,
      d_dayofweek String,
      d_month String,
      d_year Int32,
      d_yearmonthnum Int32,
      d_yearmonth String,
      d_daynuminweek Int32,
      d_daynuminmonth Int32,
      d_daynuminyear Int32,
      d_monthnuminyear Int32,
      d_weeknuminyear Int32,
      d_sellingseason String,
      d_lastdayinweekfl Int32,
      d_lastdayinmonthfl Int32,
      d_holidayfl Int32,
      d_weekdayfl Int32,
      PRIMARY KEY (d_datekey)
    ) ENGINE = MergeTree()
    ORDER BY d_datekey;
    """

    create_supplier_table = """
    CREATE TABLE IF NOT EXISTS ssb.supplier (
      s_suppkey Int32,
      s_name String,
      s_address String,
      s_city String,
      s_nation String,
      s_region String,
      s_phone String,
      PRIMARY KEY (s_suppkey)
    ) ENGINE = MergeTree()
    ORDER BY s_suppkey;
    """

    create_customer_table = """
    CREATE TABLE IF NOT EXISTS ssb.customer (
      c_custkey Int32,
      c_name String,
      c_address String,
      c_city String,
      c_nation String,
      c_region String,
      c_phone String,
      c_mktsegment String,
      PRIMARY KEY (c_custkey)
    ) ENGINE = MergeTree()
    ORDER BY c_custkey;
    """

    create_lineorder_table = """
    CREATE TABLE IF NOT EXISTS ssb.lineorder (
      lo_orderkey Int32,
      lo_linenumber Int32,
      lo_custkey Int32,
      lo_partkey Int32,
      lo_suppkey Int32,
      lo_orderdate Int32,
      lo_orderpriority String,
      lo_shippriority Int32,
      lo_quantity Int32,
      lo_extendedprice Int32,
      lo_ordtotalprice Int32,
      lo_discount Int32,
      lo_revenue Int32,
      lo_supplycost Int32,
      lo_tax Int32,
      lo_commitdate Int32,
      lo_shipmode String,
      PRIMARY KEY (lo_orderkey, lo_linenumber)
    ) ENGINE = MergeTree()
    ORDER BY (lo_orderkey, lo_linenumber);
    """

    client.execute(create_part_table)
    client.execute(create_dates_table)
    client.execute(create_supplier_table)
    client.execute(create_customer_table)
    client.execute(create_lineorder_table)

if __name__ == "__main__":
    create_tables()
