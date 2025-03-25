import mysql.connector

def create_tables():
    connection = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="123",
        database="ssb"
    )
    cursor = connection.cursor()

    create_part_table = """
    CREATE TABLE IF NOT EXISTS `part` (
      `p_partkey` int(11) NOT NULL,
      `p_name` varchar(23) NOT NULL,
      `p_mfgr` varchar(7) NOT NULL,
      `p_category` varchar(8) NOT NULL,
      `p_brand` varchar(10) NOT NULL,
      `p_size` int(11) NOT NULL,
      `p_container` varchar(11) NOT NULL,
      PRIMARY KEY (`p_partkey`)
    ) ENGINE=InnoDB;
    """

    create_dates_table = """
    CREATE TABLE IF NOT EXISTS `dates` (
      `d_datekey` int(11) NOT NULL,
      `d_date` varchar(20) NOT NULL,
      `d_dayofweek` varchar(10) NOT NULL,
      `d_month` varchar(11) NOT NULL,
      `d_year` int(11) NOT NULL,
      `d_yearmonthnum` int(11) NOT NULL,
      `d_yearmonth` varchar(9) NOT NULL,
      `d_daynuminweek` int(11) NOT NULL,
      `d_daynuminmonth` int(11) NOT NULL,
      `d_daynuminyear` int(11) NOT NULL,
      `d_monthnuminyear` int(11) NOT NULL,
      `d_weeknuminyear` int(11) NOT NULL,
      `d_sellingseason` varchar(14) NOT NULL,
      `d_lastdayinweekfl` int(11) NOT NULL,
      `d_lastdayinmonthfl` int(11) NOT NULL,
      `d_holidayfl` int(11) NOT NULL,
      `d_weekdayfl` int(11) NOT NULL,
      PRIMARY KEY (`d_datekey`)
    ) ENGINE=InnoDB;
    """

    create_supplier_table = """
    CREATE TABLE IF NOT EXISTS `supplier` (
      `s_suppkey` int(11) NOT NULL,
      `s_name` varchar(26) NOT NULL,
      `s_address` varchar(26) NOT NULL,
      `s_city` varchar(11) NOT NULL,
      `s_nation` varchar(16) NOT NULL,
      `s_region` varchar(13) NOT NULL,
      `s_phone` varchar(16) NOT NULL,
      PRIMARY KEY (`s_suppkey`)
    ) ENGINE=InnoDB;
    """

    create_customer_table = """
    CREATE TABLE IF NOT EXISTS `customer` (
      `c_custkey` int(11) NOT NULL,
      `c_name` varchar(26) NOT NULL,
      `c_address` varchar(41) NOT NULL,
      `c_city` varchar(11) NOT NULL,
      `c_nation` varchar(16) NOT NULL,
      `c_region` varchar(13) NOT NULL,
      `c_phone` varchar(16) NOT NULL,
      `c_mktsegment` varchar(11) NOT NULL,
      PRIMARY KEY (`c_custkey`)
    ) ENGINE=InnoDB;
    """

    create_lineorder_table = """
    CREATE TABLE IF NOT EXISTS `lineorder` (
      `lo_orderkey` int(11) NOT NULL,
      `lo_linenumber` int(11) NOT NULL,
      `lo_custkey` int(11) NOT NULL,
      `lo_partkey` int(11) NOT NULL,
      `lo_suppkey` int(11) NOT NULL,
      `lo_orderdate` int(11) NOT NULL,
      `lo_orderpriority` varchar(16) NOT NULL,
      `lo_shippriority` int(11) NOT NULL,
      `lo_quantity` int(11) NOT NULL,
      `lo_extendedprice` int(11) NOT NULL,
      `lo_ordtotalprice` int(11) NOT NULL,
      `lo_discount` int(11) NOT NULL,
      `lo_revenue` int(11) NOT NULL,
      `lo_supplycost` int(11) NOT NULL,
      `lo_tax` int(11) NOT NULL,
      `lo_commitdate` int(11) NOT NULL,
      `lo_shipmode` varchar(11) NOT NULL,
      PRIMARY KEY (`lo_orderkey`, `lo_linenumber`)
    ) ENGINE=InnoDB;
    """

    cursor.execute(create_part_table)
    cursor.execute(create_dates_table)
    cursor.execute(create_supplier_table)
    cursor.execute(create_customer_table)
    cursor.execute(create_lineorder_table)

    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":
    create_tables()
