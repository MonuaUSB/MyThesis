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

# Функция для выполнения тестов
def run_pg_tests():
    conn = psycopg2.connect(**DB_CONFIG_PG)
    cursor = conn.cursor()

    результаты = []

    # Тест вставки данных по одному запросу
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("""
        CREATE TABLE test (
            id SERIAL PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()

    start_time = time.time()
    for i in range(1, 100001):
        cursor.execute("INSERT INTO test (id, value) VALUES (%s, %s)", (i, 'значение'))
    conn.commit()
    duration = time.time() - start_time
    print(f"PostgreSQL Тест вставки по одному запросу: {duration} секунд")
    результаты.append(["Вставка по одному запросу", duration])

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
    результаты.append(["Пакетная вставка", duration])
    
    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест чтения данных
    start_time = time.time()
    for i in range(1, 100001):
        cursor.execute("SELECT * FROM test WHERE id = %s", (i,))
        cursor.fetchone()
    duration = time.time() - start_time
    print(f"PostgreSQL Тест чтения: {duration} секунд")
    результаты.append(["Чтение", duration])
    
    time.sleep(2)
    
    # Тест обновления данных
    start_time = time.time()
    for i in range(1, 100001):
        cursor.execute("UPDATE test SET value = %s WHERE id = %s", ('обновленное_значение', i))
    conn.commit()
    duration = time.time() - start_time
    print(f"PostgreSQL Тест обновления: {duration} секунд")
    результаты.append(["Обновление", duration])
    
    time.sleep(2)
    
    # Тест удаления данных
    start_time = time.time()
    for i in range(1, 100001):
        cursor.execute("DELETE FROM test WHERE id = %s", (i,))
    conn.commit()
    duration = time.time() - start_time
    print(f"PostgreSQL Тест удаления: {duration} секунд")
    результаты.append(["Удаление", duration])
    
    # Сохранение результатов в CSV
    csv_file = "pg_results.csv"
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Тест", "Длительность (с)"])
        writer.writerows(результаты)
    
    # Построение графика
    tests = [result[0] for result in результаты]
    durations = [result[1] for result in результаты]

    plt.figure(figsize=(10, 5))
    plt.bar(tests, durations, color='blue')
    plt.xlabel('Тест')
    plt.ylabel('Длительность (с)')
    plt.title('PostgreSQL Тесты производительности')
    plt.savefig("pg_performance.png")
    plt.show()

    cursor.close()
    conn.close()

# Запуск тестов PostgreSQL
run_pg_tests()
