import clickhouse_driver
import time
import matplotlib.pyplot as plt
import csv
from io import StringIO

# Параметры подключения к базе данных ClickHouse
DB_CONFIG = {
    'host': '127.0.0.1',
    'database': 'ssb'
}

def run_clickhouse_tests():
    conn = clickhouse_driver.connect(**DB_CONFIG)
    cursor = conn.cursor()

    results = []

    # Тест вставки одной строки
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("""
        CREATE TABLE test (
            id UInt64,
            value String
        ) ENGINE = MergeTree ORDER BY id
    """)
    conn.commit()

    start_time = time.time()
    for i in range(1, 100001):
        cursor.execute(f"INSERT INTO test (id, value) VALUES ({int(i)}, 'значение')")
    conn.commit()
    duration = time.time() - start_time
    print(f"ClickHouse Тест вставки одной строки: {duration} секунд")
    results.append(["Вставка одной строки", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест пакетной вставки данных
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("""
        CREATE TABLE test (
            id UInt64,
            value String
        ) ENGINE = MergeTree ORDER BY id
    """)
    conn.commit()

    start_time = time.time()
    batch_size = 1000
    for i in range(1, 100001, batch_size):
        values = ','.join([f"({j}, 'значение')" for j in range(i, i + batch_size)])
        cursor.execute(f"INSERT INTO test (id, value) VALUES {values}")
    conn.commit()
    duration = time.time() - start_time
    print(f"ClickHouse Тест пакетной вставки: {duration} секунд")
    results.append(["Пакетная вставка", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест выборки строк
    start_time = time.time()
    selection_size = 1000
    for i in range(1, 100001, selection_size):
        cursor.execute(f"SELECT * FROM test WHERE id BETWEEN {i} AND {i + selection_size - 1}")
        cursor.fetchall()
    duration = time.time() - start_time
    print(f"ClickHouse Тест выборки строк: {duration} секунд")
    results.append(["Выборка строк", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест обновления строк
    start_time = time.time()
    update_size = 1000
    for i in range(1, 100001, update_size):
        cursor.execute(f"""
            ALTER TABLE test UPDATE value = 'обновленное_значение' WHERE id BETWEEN {i} AND {i + update_size - 1}
        """)
    conn.commit()
    duration = time.time() - start_time
    print(f"ClickHouse Тест обновления строк: {duration} секунд")
    results.append(["Обновление строк", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест удаления строк
    start_time = time.time()
    delete_size = 1000
    for i in range(1, 100001, delete_size):
        cursor.execute(f"""
            ALTER TABLE test DELETE WHERE id BETWEEN {i} AND {i + delete_size - 1}
        """)
    conn.commit()
    cursor.execute("OPTIMIZE TABLE test")
    duration = time.time() - start_time
    print(f"ClickHouse Тест удаления строк: {duration} секунд")
    results.append(["Удаление строк", duration])

    # Сохранение результатов в CSV
    csv_file = "clickhouse_results.csv"
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Тест", "Длительность (с)"])
        writer.writerows(results)

    # Построение графика
    tests = [result[0] for result in results]
    durations = [result[1] for result in results]

    plt.figure(figsize=(10, 5))
    plt.bar(tests, durations, color='blue')
    plt.xlabel('Тест')
    plt.ylabel('Длительность (с)')
    plt.title('ClickHouse Тесты производительности')
    plt.savefig("clickhouse_performance.png")
    plt.show()

    cursor.close()
    conn.close()

# Запуск тестов ClickHouse
run_clickhouse_tests()
