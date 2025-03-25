import os
import time
import csv
import matplotlib.pyplot as plt
import psutil
from pymongo import MongoClient

# Параметры подключения к базе данных
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 27017,
    'db_name': 'ssb2'
}

# Папка для сохранения результатов
RESULTS_DIR = 'results'

# Создание папки для результатов, если не существует
if not os.path.exists(RESULTS_DIR):
    os.makedirs(RESULTS_DIR)

QUERIES = [
    {
        "name": "Query 1",
        "pipeline": [
            {"$match": {"lo_orderdate": {"$gte": 19930101, "$lte": 19931231}}},
            {"$group": {"_id": "$lo_orderpriority", "count": {"$sum": 1}, "avg_quantity": {"$avg": "$lo_quantity"}}}
        ]
    },
    {
        "name": "Query 2",
        "pipeline": [
            {"$lookup": {"from": "part", "localField": "lo_partkey", "foreignField": "p_partkey", "as": "part"}},
            {"$unwind": "$part"},
            {"$group": {"_id": "$part.p_brand", "total_revenue": {"$sum": "$lo_revenue"}}},
            {"$sort": {"total_revenue": -1}}
        ]
    },
    {
        "name": "Query 3",
        "pipeline": [
            {"$group": {"_id": "$lo_orderdate", "total_revenue": {"$sum": "$lo_revenue"}}},
            {"$sort": {"_id": 1}}
        ]
    },
    {
        "name": "Query 4",
        "pipeline": [
            {"$lookup": {"from": "dates", "localField": "lo_orderdate", "foreignField": "d_datekey", "as": "dates"}},
            {"$unwind": "$dates"},
            {"$lookup": {"from": "part", "localField": "lo_partkey", "foreignField": "p_partkey", "as": "part"}},
            {"$unwind": "$part"},
            {"$lookup": {"from": "supplier", "localField": "lo_suppkey", "foreignField": "s_suppkey", "as": "supplier"}},
            {"$unwind": "$supplier"},
            {"$match": {"part.p_brand": {"$gte": "MFGR#2221", "$lte": "MFGR#2228"}, "supplier.s_region": "ASIA"}},
            {"$group": {"_id": {"year": "$dates.d_year", "brand": "$part.p_brand"}, "total_revenue": {"$sum": "$lo_revenue"}}},
            {"$sort": {"_id.year": 1, "_id.brand": 1}}
        ]
    },
    {
        "name": "Query 5",
        "pipeline": [
            {"$lookup": {"from": "dates", "localField": "lo_orderdate", "foreignField": "d_datekey", "as": "dates"}},
            {"$unwind": "$dates"},
            {"$lookup": {"from": "part", "localField": "lo_partkey", "foreignField": "p_partkey", "as": "part"}},
            {"$unwind": "$part"},
            {"$lookup": {"from": "supplier", "localField": "lo_suppkey", "foreignField": "s_suppkey", "as": "supplier"}},
            {"$unwind": "$supplier"},
            {"$match": {"part.p_brand": "MFGR#2239", "supplier.s_region": "EUROPE"}},
            {"$group": {"_id": {"year": "$dates.d_year", "brand": "$part.p_brand"}, "total_revenue": {"$sum": "$lo_revenue"}}},
            {"$sort": {"_id.year": 1, "_id.brand": 1}}
        ]
    },
    {
        "name": "Query 6",
        "pipeline": [
            {"$lookup": {"from": "supplier", "localField": "lo_suppkey", "foreignField": "s_suppkey", "as": "supplier"}},
            {"$unwind": "$supplier"},
            {"$group": {"_id": "$supplier.s_region", "order_count": {"$sum": 1}}},
            {"$sort": {"order_count": -1}}
        ]
    },
    {
        "name": "Query 7",
        "pipeline": [
            {"$lookup": {"from": "dates", "localField": "lo_orderdate", "foreignField": "d_datekey", "as": "dates"}},
            {"$unwind": "$dates"},
            {"$match": {"dates.d_weeknuminyear": 6, "dates.d_year": 1994, "lo_discount": {"$gte": 5, "$lte": 7}, "lo_quantity": {"$gte": 26, "$lte": 35}}},
            {"$group": {"_id": None, "revenue": {"$sum": {"$multiply": ["$lo_extendedprice", "$lo_discount"]}}}}
        ]
    },
    {
        "name": "Query 8",
        "pipeline": [
            {"$lookup": {"from": "dates", "localField": "lo_orderdate", "foreignField": "d_datekey", "as": "dates"}},
            {"$unwind": "$dates"},
            {"$match": {"dates.d_yearmonth": "1994-01", "lo_discount": {"$gte": 4, "$lte": 6}, "lo_quantity": {"$gte": 26, "$lte": 35}}},
            {"$group": {"_id": None, "revenue": {"$sum": {"$multiply": ["$lo_extendedprice", "$lo_discount"]}}}}
        ]
    },
    {
        "name": "Query 9",
        "pipeline": [
            {"$lookup": {"from": "dates", "localField": "lo_orderdate", "foreignField": "d_datekey", "as": "dates"}},
            {"$unwind": "$dates"},
            {"$lookup": {"from": "part", "localField": "lo_partkey", "foreignField": "p_partkey", "as": "part"}},
            {"$unwind": "$part"},
            {"$lookup": {"from": "supplier", "localField": "lo_suppkey", "foreignField": "s_suppkey", "as": "supplier"}},
            {"$unwind": "$supplier"},
            {"$match": {"part.p_category": "MFGR#12", "supplier.s_region": "AMERICA"}},
            {"$group": {"_id": {"year": "$dates.d_year", "brand": "$part.p_brand"}, "total_revenue": {"$sum": "$lo_revenue"}}},
            {"$sort": {"_id.brand": 1}}
        ]
    },
    {
        "name": "Query 10",
        "pipeline": [
            {"$lookup": {"from": "dates", "localField": "lo_orderdate", "foreignField": "d_datekey", "as": "dates"}},
            {"$unwind": "$dates"},
            {"$match": {"dates.d_year": 1993, "lo_discount": {"$gte": 1, "$lte": 3}, "lo_quantity": {"$lt": 25}}},
            {"$group": {"_id": None, "revenue": {"$sum": {"$multiply": ["$lo_extendedprice", "$lo_discount"]}}}}
        ]
    },
    {
        "name": "Query 11",
        "pipeline": [
            {"$lookup": {"from": "dates", "localField": "lo_orderdate", "foreignField": "d_datekey", "as": "dates"}},
            {"$unwind": "$dates"},
            {"$group": {"_id": "$dates.d_year", "total_revenue": {"$sum": "$lo_revenue"}}},
            {"$sort": {"_id": 1}}
        ]
    }
]

# Функция для выполнения запроса и измерения времени выполнения
def execute_query(collection, pipeline):
    start_time = time.time()
    try:
        rows = list(collection.aggregate(pipeline))
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
    client = MongoClient(DB_CONFIG['host'], DB_CONFIG['port'])
    db = client[DB_CONFIG['db_name']]
    collection = db['lineorder']

    results = []
    data_rows = []
    resource_usage = []

    for q in QUERIES:
        print(f"Executing {q['name']}...")
        cpu_start, mem_start = monitor_resources()
        exec_time, rows = execute_query(collection, q['pipeline'])
        cpu_end, mem_end = monitor_resources()
        results.append((q['name'], exec_time))
        print(f"{q['name']} executed in {exec_time:.2f} seconds")
        for row in rows:
            data_rows.append([q['name'], exec_time] + list(row.values()))
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
