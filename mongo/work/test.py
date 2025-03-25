from pymongo import MongoClient
import time
import matplotlib.pyplot as plt
import csv
import os

# Параметры подключения к базе данных MongoDB
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 27017,
    'db_name': 'ssb2'
}

# Функция для выполнения тестов
def run_mongodb_tests():
    client = MongoClient(DB_CONFIG['host'], DB_CONFIG['port'])
    db = client[DB_CONFIG['db_name']]
    collection = db['test']

    результаты = []

    # Тест вставки данных по одному запросу
    collection.drop()
    start_time = time.time()
    for i in range(1, 100001):
        collection.insert_one({'_id': i, 'value': 'значение'})
    duration = time.time() - start_time
    print(f"MongoDB Тест вставки по одному запросу: {duration} секунд")
    результаты.append(["Вставка по одному запросу", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест пакетной вставки данных
    collection.drop()
    start_time = time.time()
    batch_size = 1000
    for batch_start in range(1, 100001, batch_size):
        batch_end = min(batch_start + batch_size, 100001)
        data = [{'_id': i, 'value': 'значение'} for i in range(batch_start, batch_end)]
        collection.insert_many(data)
    duration = time.time() - start_time
    print(f"MongoDB Тест пакетной вставки: {duration} секунд")
    результаты.append(["Пакетная вставка", duration])
    
    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест чтения данных
    start_time = time.time()
    for i in range(1, 100001):
        collection.find_one({'_id': i})
    duration = time.time() - start_time
    print(f"MongoDB Тест чтения: {duration} секунд")
    результаты.append(["Чтение", duration])
    
    time.sleep(2)
    
    # Тест обновления данных
    start_time = time.time()
    for i in range(1, 100001):
        collection.update_one({'_id': i}, {'$set': {'value': 'обновленное_значение'}})
    duration = time.time() - start_time
    print(f"MongoDB Тест обновления: {duration} секунд")
    результаты.append(["Обновление", duration])
    
    time.sleep(2)
    
    # Тест удаления данных
    start_time = time.time()
    for i in range(1, 100001):
        collection.delete_one({'_id': i})
    duration = time.time() - start_time
    print(f"MongoDB Тест удаления: {duration} секунд")
    результаты.append(["Удаление", duration])
    
    # Сохранение результатов в CSV
    csv_file = "mongodb_results.csv"
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
    plt.title('MongoDB Тесты производительности')
    plt.savefig("mongodb_performance.png")
    plt.show()

    client.close()

# Запуск тестов MongoDB
run_mongodb_tests()
