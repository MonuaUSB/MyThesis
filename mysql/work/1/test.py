import mysql.connector
import time
import matplotlib.pyplot as plt
import csv
import os

# Параметры подключения к базе данных MySQL
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '123',
    'database': 'ssb'
}

def run_mysql_tests():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    results = []

    # Тест вставки одной строки
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("""
        CREATE TABLE test (
            id INT PRIMARY KEY,
            value VARCHAR(255)
        )
    """)
    conn.commit()

    start_time = time.time()
    cursor.execute("START TRANSACTION")
    for i in range(1, 100001):
        cursor.execute("INSERT INTO test (id, value) VALUES (%s, %s)", (i, 'значение'))
    cursor.execute("COMMIT")
    duration = time.time() - start_time
    print(f"MySQL Тест вставки одной строки: {duration} секунд")
    results.append(["Вставка одной строки", duration])

# Добавляем паузу между тестами
    time.sleep(2)

    # Тест пакетной вставки строк
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("""
        CREATE TABLE test (
            id INT PRIMARY KEY,
            value VARCHAR(255)
        )
    """)
    conn.commit()

    start_time = time.time()
    cursor.execute("START TRANSACTION")
    batch_size = 1000
    for batch_start in range(1, 100001, batch_size):
        batch_end = min(batch_start + batch_size, 100001)
        data = [(i, 'значение') for i in range(batch_start, batch_end)]
        cursor.executemany("INSERT INTO test (id, value) VALUES (%s, %s)", data)
    cursor.execute("COMMIT")
    duration = time.time() - start_time
    print(f"MySQL Тест пакетной вставки строк: {duration} секунд")
    results.append(["Пакетная вставка строк", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест выборки строк
    start_time = time.time()
    selection_size = 1000
    for i in range(1, 100001, selection_size):
        cursor.execute("SELECT * FROM test WHERE id BETWEEN %s AND %s", (i, i + selection_size - 1))
        cursor.fetchall()
    duration = time.time() - start_time
    print(f"MySQL Тест выборки строк: {duration} секунд")
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
    print(f"MySQL Тест обновления строк: {duration} секунд")
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
    cursor.execute("OPTIMIZE TABLE test")
    duration = time.time() - start_time
    print(f"MySQL Тест удаления строк: {duration} секунд")
    results.append(["Удаление строк", duration])

    # Сохранение результатов в CSV
    csv_file = "mysql_results.csv"
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
    plt.title('MySQL Тесты производительности')
    plt.savefig("mysql_performance.png")
    plt.show()

    cursor.close()
    conn.close()

# Запуск тестов MySQL
run_mysql_tests()