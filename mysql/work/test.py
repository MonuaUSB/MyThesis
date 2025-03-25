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

# Функция для выполнения тестов
def run_mysql_tests():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    результаты = []

    # Тест вставки данных по одному запросу
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("""
        CREATE TABLE test (
            id INT PRIMARY KEY,
            value VARCHAR(255)
        )
    """)
    conn.commit()

    start_time = time.time()
    for i in range(1, 100001):
        cursor.execute("INSERT INTO test (id, value) VALUES (%s, %s)", (i, 'значение'))
    conn.commit()
    duration = time.time() - start_time
    print(f"MySQL Тест вставки по одному запросу: {duration} секунд")
    результаты.append(["Вставка по одному запросу", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест пакетной вставки данных
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("""
        CREATE TABLE test (
            id INT PRIMARY KEY,
            value VARCHAR(255)
        )
    """)
    conn.commit()

    start_time = time.time()
    batch_size = 1000
    for batch_start in range(1, 100001, batch_size):
        batch_end = min(batch_start + batch_size, 100001)
        data = [(i, 'значение') for i in range(batch_start, batch_end)]
        cursor.executemany("INSERT INTO test (id, value) VALUES (%s, %s)", data)
    conn.commit()
    duration = time.time() - start_time
    print(f"MySQL Тест пакетной вставки: {duration} секунд")
    результаты.append(["Пакетная вставка", duration])
    
    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест чтения данных
    start_time = time.time()
    for i in range(1, 100001):
        cursor.execute("SELECT * FROM test WHERE id = %s", (i,))
        cursor.fetchone()
    duration = time.time() - start_time
    print(f"MySQL Тест чтения: {duration} секунд")
    результаты.append(["Чтение", duration])
    
    time.sleep(2)
    
    # Тест обновления данных
    start_time = time.time()
    for i in range(1, 100001):
        cursor.execute("UPDATE test SET value = %s WHERE id = %s", ('обновленное_значение', i))
    conn.commit()
    duration = time.time() - start_time
    print(f"MySQL Тест обновления: {duration} секунд")
    результаты.append(["Обновление", duration])
    
    time.sleep(2)
    
    # Тест удаления данных
    start_time = time.time()
    for i in range(1, 100001):
        cursor.execute("DELETE FROM test WHERE id = %s", (i,))
    conn.commit()
    duration = time.time() - start_time
    print(f"MySQL Тест удаления: {duration} секунд")
    результаты.append(["Удаление", duration])
    
    # Сохранение результатов в CSV
    csv_file = "mysql_results.csv"
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
    plt.title('MySQL Тесты производительности')
    plt.savefig("mysql_performance.png")
    plt.show()

    cursor.close()
    conn.close()

# Запуск тестов MySQL
run_mysql_tests()