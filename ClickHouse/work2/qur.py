import os
from clickhouse_driver import Client
import time
import csv
import matplotlib.pyplot as plt
import psutil

# Параметры подключения к базе данных
DB_CONFIG = {
    'host': '127.0.0.1',
    'database': 'ssb2'
}

# Папка для сохранения результатов
RESULTS_DIR = 'results'

# Создание папки для результатов, если не существует
if not os.path.exists(RESULTS_DIR):
    os.makedirs(RESULTS_DIR)

QUERIES = [
    {
        "name": "Query 1",
        "sql": """
        SELECT COUNT(*), AVG(lo_quantity)
        FROM lineorder
        WHERE lo_orderdate BETWEEN 19930101 AND 19931231
        GROUP BY lo_orderpriority;
        """
    },
    {
        "name": "Query 2",
        "sql": """
        SELECT p_brand, SUM(lo_revenue)
        FROM lineorder
        JOIN part ON lineorder.lo_partkey = part.p_partkey
        GROUP BY p_brand
        ORDER BY SUM(lo_revenue) DESC;
        """
    },
    {
        "name": "Query 3",
        "sql": """
        SELECT lo_orderdate, SUM(lo_revenue)
        FROM lineorder
        GROUP BY lo_orderdate
        ORDER BY lo_orderdate;
        """
    },
    {
        "name": "Query 4",
        "sql": """
        SELECT SUM(lo_revenue), dates.d_year, part.p_brand
        FROM lineorder
        JOIN dates ON lineorder.lo_orderdate = dates.d_datekey
        JOIN part ON lineorder.lo_partkey = part.p_partkey
        JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
        WHERE part.p_brand BETWEEN 'MFGR#2221' AND 'MFGR#2228'
          AND supplier.s_region = 'ASIA'
        GROUP BY dates.d_year, part.p_brand
        ORDER BY dates.d_year, part.p_brand;
        """
    },
    {
        "name": "Query 5",
        "sql": """
        SELECT SUM(lo_revenue), dates.d_year, part.p_brand
        FROM lineorder
        JOIN dates ON lineorder.lo_orderdate = dates.d_datekey
        JOIN part ON lineorder.lo_partkey = part.p_partkey
        JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
        WHERE part.p_brand = 'MFGR#2239'
          AND supplier.s_region = 'EUROPE'
        GROUP BY dates.d_year, part.p_brand
        ORDER BY dates.d_year, part.p_brand;
        """
    },
    {
        "name": "Query 6",
        "sql": """
        SELECT s_region, COUNT(DISTINCT lo_orderkey)
        FROM lineorder
        JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
        GROUP BY s_region
        ORDER BY COUNT(DISTINCT lo_orderkey) DESC;
        """
    },
    {
        "name": "Query 7",
        "sql": """
        SELECT SUM(lo_extendedprice * lo_discount) AS REVENUE
        FROM lineorder
        JOIN dates ON lineorder.lo_orderdate = dates.d_datekey
        WHERE dates.d_weeknuminyear = 6
          AND dates.d_year = 1994
          AND lo_discount BETWEEN 5 AND 7
          AND lo_quantity BETWEEN 26 AND 35;
        """
    },
    {
        "name": "Query 8",
        "sql": """
        SELECT SUM(lo_extendedprice * lo_discount) AS REVENUE
        FROM lineorder
        JOIN dates ON lineorder.lo_orderdate = dates.d_datekey
        WHERE dates.d_yearmonth = '1994-01'
          AND lo_discount BETWEEN 4 AND 6
          AND lo_quantity BETWEEN 26 AND 35;
        """
    },
    {
        "name": "Query 9",
        "sql": """
        SELECT SUM(lo_revenue), dates.d_year, part.p_brand
        FROM lineorder
        JOIN dates ON lineorder.lo_orderdate = dates.d_datekey
        JOIN part ON lineorder.lo_partkey = part.p_partkey
        JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
        WHERE part.p_category = 'MFGR#12'
          AND supplier.s_region = 'AMERICA'
        GROUP BY dates.d_year, part.p_brand
        ORDER BY part.p_brand;
        """
    },
    {
        "name": "Query 10",
        "sql": """
        SELECT SUM(lo_extendedprice * lo_discount) AS REVENUE
        FROM lineorder
        JOIN dates ON lineorder.lo_orderdate = dates.d_datekey
        WHERE dates.d_year = 1993
          AND lo_discount BETWEEN 1 AND 3
          AND lo_quantity < 25;
        """
    },
    {
        "name": "Query 11",
        "sql": """
        SELECT d_year, SUM(lo_revenue) AS TOTAL_REVENUE
        FROM lineorder
        JOIN dates ON lineorder.lo_orderdate = dates.d_datekey
        GROUP BY d_year
        ORDER BY d_year;
        """
    }
]

# Функция для выполнения запроса и измерения времени выполнения
def execute_query(client, query):
    start_time = time.time()
    try:
        rows = client.execute(query)
    except Exception as err:
        print(f"Error: {err}")
        rows = []
    end_time = time.time()
    return end_time - start_time, rows

# Функция для мониторинга системных ресурсов
def monitor_resources():
    cpu_percent = psutil.cpu_percent(interval=1)
    mem_info = psutil.virtual_memory()
    return cpu_percent, mem_info.percent

# Функция для выполнения всех запросов и записи результатов в файл
def run_queries():
    client = Client(host=DB_CONFIG['host'], database=DB_CONFIG['database'])

    results = []
    data_rows = []
    resource_usage = []

    for q in QUERIES:
        print(f"Executing {q['name']}...")
        cpu_start, mem_start = monitor_resources()
        exec_time, rows = execute_query(client, q['sql'])
        cpu_end, mem_end = monitor_resources()
        results.append((q['name'], exec_time))
        print(f"{q['name']} executed in {exec_time:.2f} seconds")
        for row in rows:
            data_rows.append([q['name'], exec_time] + list(row))
        resource_usage.append([q['name'], exec_time, cpu_start, cpu_end, mem_start, mem_end])

    # Сохранение результатов в CSV файл
    with open(os.path.join(RESULTS_DIR, 'query_results.csv'), 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Query Name', 'Execution Time (s)'])
        for result in results:
            writer.writerow(result)

    # Сохранение использования ресурсов в CSV файл
    with open(os.path.join(RESULTS_DIR, 'resource_usage.csv'), 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Query Name', 'Execution Time (s)', 'CPU Start (%)', 'CPU End (%)', 'Memory Start (%)', 'Memory End (%)'])
        writer.writerows(resource_usage)

    return results, resource_usage

# Функция для создания графиков
def create_plots(results, resource_usage):
    query_names = [r[0] for r in results]
    exec_times = [r[1] for r in results]

    # График времени выполнения запросов
    plt.figure(figsize=(10, 5))
    plt.barh(query_names, exec_times, color='skyblue')
    plt.xlabel('Время выполнения (секунды)')
    plt.ylabel('Запросы')
    plt.title('Время выполнения запросов')
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'query_execution_time.png'))

    # График использования CPU
    cpu_start = [r[2] for r in resource_usage]
    cpu_end = [r[3] for r in resource_usage]
    plt.figure(figsize=(10, 5))
    plt.plot(query_names, cpu_start, marker='o', label='CPU Начало (%)')
    plt.plot(query_names, cpu_end, marker='o', label='CPU Конец (%)')
    plt.xlabel('Запросы')
    plt.ylabel('Использование CPU (%)')
    plt.title('Использование CPU по запросам')
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'cpu_usage.png'))

    # График использования памяти
    mem_start = [r[4] for r in resource_usage]
    mem_end = [r[5] for r in resource_usage]
    plt.figure(figsize=(10, 5))
    plt.plot(query_names, mem_start, marker='o', label='Память Начало (%)')
    plt.plot(query_names, mem_end, marker='o', label='Память Конец (%)')
    plt.xlabel('Запросы')
    plt.ylabel('Использование памяти (%)')
    plt.title('Использование памяти по запросам')
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'memory_usage.png'))

# Главная функция
if __name__ == "__main__":
    results, resource_usage = run_queries()
    create_plots(results, resource_usage)

    # Сохранение результатов в файл
    with open(os.path.join(RESULTS_DIR, 'query_results.txt'), 'w') as f:
        for name, exec_time in results:
            f.write(f"{name}: {exec_time:.2f} seconds\n")
