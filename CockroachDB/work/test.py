import psycopg2
import time
import matplotlib.pyplot as plt
import csv
import os

# Параметры подключения к базе данных CockroachDB
DB_CONFIG_CRDB = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '',
    'dbname': 'ssb',
    'port': '26257'
}

# Функция для выполнения тестов
def run_crdb_tests():
    conn = psycopg2.connect(**DB_CONFIG_CRDB)
    cursor = conn.cursor()

    # Создание таблицы, если она не существует, и очистка данных
    cursor.execute("""
        DROP TABLE IF EXISTS test;
        CREATE TABLE test (
            id INT PRIMARY KEY,
            value STRING
        );
    """)
    conn.commit()
    
    результаты = []

    # Тест вставки данных
    start_time = time.time()
    for i in range(1, 100001):
        cursor.execute("INSERT INTO test (id, value) VALUES (%s, %s)", (i, 'значение'))
    conn.commit()
    duration = time.time() - start_time
    print(f"CockroachDB Тест вставки: {duration} секунд")
    результаты.append(["Вставка", duration])
    
    # Добавляем паузу между тестами
    time.sleep(2)
    
    # Тест чтения данных
    start_time = time.time()
    for i in range(1, 100001):
        cursor.execute("SELECT * FROM test WHERE id = %s", (i,))
        cursor.fetchone()
    duration = time.time() - start_time
    print(f"CockroachDB Тест чтения: {duration} секунд")
    результаты.append(["Чтение", duration])
    
    time.sleep(2)
    
    # Тест обновления данных
    start_time = time.time()
    for i in range(1, 100001):
        cursor.execute("UPDATE test SET value = %s WHERE id = %s", ('обновленное_значение', i))
    conn.commit()
    duration = time.time() - start_time
    print(f"CockroachDB Тест обновления: {duration} секунд")
    результаты.append(["Обновление", duration])
    
    time.sleep(2)
    
    # Тест удаления данных
    start_time = time.time()
    for i in range(1, 100001):
        cursor.execute("DELETE FROM test WHERE id = %s", (i,))
    conn.commit()
    duration = time.time() - start_time
    print(f"CockroachDB Тест удаления: {duration} секунд")
    результаты.append(["Удаление", duration])
    
    time.sleep(2)
    
    # Тест транзакций
    start_time = time.time()
    try:
        cursor.execute("BEGIN")
        for i in range(1, 100001):
            cursor.execute("INSERT INTO test (id, value) VALUES (%s, %s)", (i, 'значение'))
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        print("Транзакция не удалась:", e)
    duration = time.time() - start_time
    print(f"CockroachDB Тест транзакции: {duration} секунд")
    результаты.append(["Транзакция", duration])
    
    conn.close()
    
    # Сохранение результатов в CSV
    csv_file = "crdb_results.csv"
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Тест", "Длительность (с)"])
        writer.writerows(результаты)
    
    # Построение графика
    tests = [result[0] for result in результаты]
    durations = [result[1] for result in результаты]

    plt.figure(figsize=(10, 5))
    plt.bar(tests, durations, color='green')
    plt.xlabel('Тест')
    plt.ylabel('Длительность (с)')
    plt.title('CockroachDB Тесты производительности')
    plt.savefig("crdb_performance.png")
    plt.show()

# Запуск тестов CockroachDB
run_crdb_tests()
