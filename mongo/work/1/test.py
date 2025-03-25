import pymongo
import time
import matplotlib.pyplot as plt
import csv

# Параметры подключения к базе данных MongoDB
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 27017,
    'db_name': 'ssb2'
}

def run_mongodb_tests():
    client = pymongo.MongoClient(DB_CONFIG['host'], DB_CONFIG['port'])
    db = client[DB_CONFIG['db_name']]
    collection = db['test']

    results = []

    # Тест вставки одной строки
    collection.drop()
    start_time = time.time()
    for i in range(1, 100001):
        collection.insert_one({"id": i, "value": "значение"})
    duration = time.time() - start_time
    print(f"MongoDB Тест вставки одной строки: {duration} секунд")
    results.append(["Вставка одной строки", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест пакетной вставки данных
    collection.drop()
    start_time = time.time()
    batch_size = 1000
    for i in range(1, 100001, batch_size):
        batch = [{"id": j, "value": "значение"} for j in range(i, i + batch_size)]
        collection.insert_many(batch)
    duration = time.time() - start_time
    print(f"MongoDB Тест пакетной вставки: {duration} секунд")
    results.append(["Пакетная вставка", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест выборки строк
    start_time = time.time()
    selection_size = 1000
    for i in range(1, 100001, selection_size):
        cursor = collection.find({"id": {"$gte": i, "$lt": i + selection_size}})
        list(cursor)
    duration = time.time() - start_time
    print(f"MongoDB Тест выборки строк: {duration} секунд")
    results.append(["Выборка строк", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест обновления строк
    start_time = time.time()
    update_size = 1000
    for i in range(1, 100001, update_size):
        collection.update_many(
            {"id": {"$gte": i, "$lt": i + update_size}},
            {"$set": {"value": "обновленное_значение"}}
        )
    duration = time.time() - start_time
    print(f"MongoDB Тест обновления строк: {duration} секунд")
    results.append(["Обновление строк", duration])

    # Добавляем паузу между тестами
    time.sleep(2)

    # Тест удаления строк
    start_time = time.time()
    delete_size = 1000
    for i in range(1, 100001, delete_size):
        collection.delete_many({"id": {"$gte": i, "$lt": i + delete_size}})
    duration = time.time() - start_time
    print(f"MongoDB Тест удаления строк: {duration} секунд")
    results.append(["Удаление строк", duration])

    # Сохранение результатов в CSV
    csv_file = "mongodb_results.csv"
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
    plt.title('MongoDB Тесты производительности')
    plt.savefig("mongodb_performance.png")
    plt.show()

    client.close()

# Запуск тестов MongoDB
run_mongodb_tests()
