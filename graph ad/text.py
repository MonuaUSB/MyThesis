import pandas as pd
import matplotlib.pyplot as plt

# Чтение данных из CSV-файлов с указанием кодировки
postgresql_results = pd.read_csv('postgresql_results.csv', encoding='cp1251')
mysql_results = pd.read_csv('mysql_results.csv', encoding='cp1251')
mongodb_results = pd.read_csv('mongodb_results.csv', encoding='cp1251')
clickhouse_results = pd.read_csv('clickhouse_results.csv', encoding='cp1251')

# Объединение данных в один DataFrame
all_results = pd.DataFrame({
    'Тест': postgresql_results['Тест'],
    'PostgreSQL': postgresql_results['Длительность (с)'],
    'MySQL': mysql_results['Длительность (с)'],
    'MongoDB': mongodb_results['Длительность (с)'],
    'ClickHouse': clickhouse_results['Длительность (с)']
})

# Установка индекса для более удобного построения графиков
all_results.set_index('Тест', inplace=True)

# Построение графиков с логарифмической шкалой по оси Y
fig, ax = plt.subplots(figsize=(10, 6))
all_results.plot(kind='bar', ax=ax, logy=True)
plt.title('Сравнение производительности различных СУБД')
plt.xlabel('Тесты')
plt.ylabel('Длительность (с) (логарифмическая шкала)')
plt.legend(title='СУБД')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()

# Сохранение графика в файл
plt.savefig('database_performance_comparison_log.png')

# Отображение графика
plt.show()
