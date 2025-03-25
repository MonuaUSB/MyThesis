import psycopg2
import time
import matplotlib.pyplot as plt
import csv
import os
from io import StringIO

# Параметры подключения к базе данных PostgreSQL
DB_CONFIG_PG = {
    'host': '127.0.0.1',
    'user': 'postgres',
    'password': '123',
    'dbname': 'ssb'
}

def run_postgresql_tests():
    conn = psycopg2.connect(**DB_CONFIG_PG)
    cursor = conn.cursor()

    results = []

    # Тест вставки одной строки
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("""
        CREATE TABLE test (
            id SERIAL PRIMARY KEY,
            value VARCHAR(255)
        )
    """)
    conn.commit()

    start_time = time.time()
    cursor.execute("START TRANSACTION")
    for i in range(1, 100001):
        cursor.execute("INSERT INTO test (value) VALUES (%s)", ('значение',))
    cursor.execute("COMMIT")
    duration = time.time() - start_time
    print(f"PostgreSQL Тест вставки одной строки: {duration} секунд")
    results.append(["Вставка одной строки", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест пакетной вставки данных с использованием команды COPY
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("""
        CREATE TABLE test (
            id SERIAL PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()

    start_time = time.time()
    output = StringIO()
    for i in range(1, 100001):
        output.write(f"{i}\tзначение\n")
    output.seek(0)

    cursor.copy_from(output, 'test', columns=('id', 'value'))
    conn.commit()
    duration = time.time() - start_time
    print(f"PostgreSQL Тест пакетной вставки: {duration} секунд")
    results.append(["Пакетная вставка", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест выборки строк
    start_time = time.time()
    selection_size = 1000
    for i in range(1, 100001, selection_size):
        cursor.execute("SELECT * FROM test WHERE id BETWEEN %s AND %s", (i, i + selection_size - 1))
        cursor.fetchall()
    duration = time.time() - start_time
    print(f"PostgreSQL Тест выборки строк: {duration} секунд")
    results.append(["Выборка строк", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест обновления строк
    start_time = time.time()
    cursor.execute("START TRANSACTION")
    update_size = 1000
    for i in range(1, 100001, update_size):
        cursor.execute("UPDATE test SET value = %s WHERE id BETWEEN %s AND %s", ('обновленное_значение', i, i + update_size - 1))
    cursor.execute("COMMIT")
    duration = time.time() - start_time
    print(f"PostgreSQL Тест обновления строк: {duration} секунд")
    results.append(["Обновление строк", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест удаления строк
    start_time = time.time()
    cursor.execute("START TRANSACTION")
    delete_size = 1000
    for i in range(1, 100001, delete_size):
        cursor.execute("DELETE FROM test WHERE id BETWEEN %s AND %s", (i, i + delete_size - 1))
    cursor.execute("COMMIT")
    cursor.execute("VACUUM FULL test")
    cursor.execute("ANALYZE test")
    duration = time.time() - start_time
    print(f"PostgreSQL Тест удаления строк: {duration} секунд")
    results.append(["Удаление строк", duration])

    # Сохранение результатов в CSV
    csv_file = "postgresql_results.csv"
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
    plt.title('PostgreSQL Тесты производительности')
    plt.savefig("postgresql_performance.png")
    plt.show()

    cursor.close()
    conn.close()

# Запуск тестов PostgreSQL
run_postgresql_tests()