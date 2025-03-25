from clickhouse_driver import Client
import time
import matplotlib.pyplot as plt
import csv
import os

# Параметры подключения к базе данных ClickHouse
DB_CONFIG = {
    'host': '127.0.0.1',
    'database': 'ssb'
}

# Функция для выполнения тестов
def run_ch_tests():
    client = Client(host=DB_CONFIG['host'], database=DB_CONFIG['database'])

    # Создание таблицы, если она не существует, и очистка данных
    client.execute("DROP TABLE IF EXISTS test")
    client.execute("""
        CREATE TABLE test (
            id Int32,
            value String
        ) ENGINE = MergeTree()
        ORDER BY id;
    """)

    результаты = []

    # Тест вставки данных по одному запросу
    start_time = time.time()
    for i in range(1, 100001):
        client.execute("INSERT INTO test (id, value) VALUES", [(i, 'значение')])
    duration = time.time() - start_time
    print(f"ClickHouse Тест вставки по одному запросу: {duration} секунд")
    результаты.append(["Вставка по одному запросу", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест пакетной вставки данных
    start_time = time.time()
    batch_size = 1000
    for batch_start in range(1, 100001, batch_size):
        batch_end = min(batch_start + batch_size, 100001)
        data = [(i, 'значение') for i in range(batch_start, batch_end)]
        client.execute("INSERT INTO test (id, value) VALUES", data)
    duration = time.time() - start_time
    print(f"ClickHouse Тест пакетной вставки: {duration} секунд")
    результаты.append(["Пакетная вставка", duration])
    
    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест чтения данных
    start_time = time.time()
    for i in range(1, 100001):
        client.execute(f"SELECT * FROM test WHERE id = {i}")
    duration = time.time() - start_time
    print(f"ClickHouse Тест чтения: {duration} секунд")
    результаты.append(["Чтение", duration])
    
    time.sleep(2)
    
    # Тест обновления данных
    start_time = time.time()
    for i in range(1, 100001):
        client.execute(f"ALTER TABLE test UPDATE value = 'обновленное_значение' WHERE id = {i}")
    duration = time.time() - start_time
    print(f"ClickHouse Тест обновления: {duration} секунд")
    результаты.append(["Обновление", duration])
    
    time.sleep(2)
    
    # Тест удаления данных
    start_time = time.time()
    for i in range(1, 100001):
        client.execute(f"ALTER TABLE test DELETE WHERE id = {i}")
    duration = time.time() - start_time
    print(f"ClickHouse Тест удаления: {duration} секунд")
    результаты.append(["Удаление", duration])
    
    # Сохранение результатов в CSV
    csv_file = "ch_results.csv"
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Тест", "Длительность (с)"])
        writer.writerows(результаты)
    
    # Построение графика
    tests = [result[0] for result in результаты]
    durations = [result[1] for result in результаты]

    plt.figure(figsize=(10, 5))
    plt.bar(tests, durations, color='red')
    plt.xlabel('Тест')
    plt.ylabel('Длительность (с)')
    plt.title('ClickHouse Тесты производительности')
    plt.savefig("ch_performance.png")
    plt.show()

# Запуск тестов ClickHouse
run_ch_tests()
