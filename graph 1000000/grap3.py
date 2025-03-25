import pandas as pd
import matplotlib.pyplot as plt
import os

# Загрузка данных из файлов
mysql_data = pd.read_csv('mysql/query_results.csv')
postgres_data = pd.read_csv('postgres/query_results.csv')
mongodb_data = pd.read_csv('mongodb/query_results.csv')
clickhouse_data = pd.read_csv('clickhouse/query_results.csv')

mysql_resource_data = pd.read_csv('mysql/resource_usage.csv')
postgres_resource_data = pd.read_csv('postgres/resource_usage.csv')
mongodb_resource_data = pd.read_csv('mongodb/resource_usage.csv')
clickhouse_resource_data = pd.read_csv('clickhouse/resource_usage.csv')

# Создание папки 'result', если она не существует
if not os.path.exists('result'):
    os.makedirs('result')

# Функция для настройки внешнего вида графика
def beautify_plot(ax, title, xlabel, ylabel, description):
    ax.set_title(title, fontsize=16)
    ax.set_xlabel(xlabel, fontsize=14)
    ax.set_ylabel(ylabel, fontsize=14)
    ax.legend(fontsize=12)
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=45, ha='right')
    for spine in ax.spines.values():
        spine.set_visible(False)
    
    # Добавление описания
    plt.figtext(0.5, -0.1, description, wrap=True, horizontalalignment='center', fontsize=12)

# Создание фигуры и оси для графика производительности
fig, ax = plt.subplots(figsize=(14, 7))

# Построение графиков для каждой базы данных с разными цветами и очень толстыми линиями
ax.plot(mysql_data['Query Name'], mysql_data['Execution Time (s)'], label='MySQL', marker='o', color='blue', linewidth=4, markersize=8)
ax.plot(postgres_data['Query Name'], postgres_data['Execution Time (s)'], label='Postgres', marker='o', color='green', linewidth=4, markersize=8)
ax.plot(mongodb_data['Query Name'], mongodb_data['Execution Time (s)'], label='MongoDB', marker='o', color='red', linewidth=4, markersize=8)
ax.plot(clickhouse_data['Query Name'], clickhouse_data['Execution Time (s)'], label='ClickHouse', marker='o', color='purple', linewidth=4, markersize=8)

# Установка логарифмической шкалы на ось Y
ax.set_yscale('log')

# Настройка внешнего вида графика
beautify_plot(ax, 'Сравнение производительности СУБД', 'Название запроса', 'Время выполнения (с)', 'График показывает время выполнения запросов для MySQL, Postgres, MongoDB и ClickHouse. Высокие значения указывают на более длительное время выполнения.')

# Аннотация значений на графике
for i, txt in enumerate(mysql_data['Execution Time (s)']):
    ax.annotate(f'{txt:.4f}', (mysql_data['Query Name'][i], mysql_data['Execution Time (s)'][i]), fontsize=8, color='blue', xytext=(5, 5), textcoords='offset points')
for i, txt in enumerate(postgres_data['Execution Time (s)']):
    ax.annotate(f'{txt:.4f}', (postgres_data['Query Name'][i], postgres_data['Execution Time (s)'][i]), fontsize=8, color='green', xytext=(5, 5), textcoords='offset points')
for i, txt in enumerate(mongodb_data['Execution Time (s)']):
    ax.annotate(f'{txt:.4f}', (mongodb_data['Query Name'][i], mongodb_data['Execution Time (s)'][i]), fontsize=8, color='red', xytext=(5, 5), textcoords='offset points')
for i, txt in enumerate(clickhouse_data['Execution Time (s)']):
    ax.annotate(f'{txt:.4f}', (clickhouse_data['Query Name'][i], clickhouse_data['Execution Time (s)'][i]), fontsize=8, color='purple', xytext=(5, 5), textcoords='offset points')

# Сохранение графика производительности в файл
plt.tight_layout()
plt.savefig('result/performance_comparison.png')

# Вычисление среднего использования CPU и памяти для каждого запроса
mysql_resource_data['CPU Avg (%)'] = (mysql_resource_data['CPU Start (%)'] + mysql_resource_data['CPU End (%)']) / 2
postgres_resource_data['CPU Avg (%)'] = (postgres_resource_data['CPU Start (%)'] + postgres_resource_data['CPU End (%)']) / 2
mongodb_resource_data['CPU Avg (%)'] = (mongodb_resource_data['CPU Start (%)'] + mongodb_resource_data['CPU End (%)']) / 2
clickhouse_resource_data['CPU Avg (%)'] = (clickhouse_resource_data['CPU Start (%)'] + clickhouse_resource_data['CPU End (%)']) / 2

mysql_resource_data['Memory Avg (%)'] = (mysql_resource_data['Memory Start (%)'] + mysql_resource_data['Memory End (%)']) / 2
postgres_resource_data['Memory Avg (%)'] = (postgres_resource_data['Memory Start (%)'] + postgres_resource_data['Memory End (%)']) / 2
mongodb_resource_data['Memory Avg (%)'] = (mongodb_resource_data['Memory Start (%)'] + mongodb_resource_data['Memory End (%)']) / 2
clickhouse_resource_data['Memory Avg (%)'] = (clickhouse_resource_data['Memory Start (%)'] + clickhouse_resource_data['Memory End (%)']) / 2

# Создание фигуры и оси для графика среднего использования CPU
fig, ax = plt.subplots(figsize=(14, 7))

# Построение графиков для каждой базы данных с разными цветами и очень толстыми линиями
ax.plot(mysql_resource_data['Query Name'], mysql_resource_data['CPU Avg (%)'], label='MySQL', marker='o', color='blue', linewidth=4, markersize=8)
ax.plot(postgres_resource_data['Query Name'], postgres_resource_data['CPU Avg (%)'], label='Postgres', marker='o', color='green', linewidth=4, markersize=8)
ax.plot(mongodb_resource_data['Query Name'], mongodb_resource_data['CPU Avg (%)'], label='MongoDB', marker='o', color='red', linewidth=4, markersize=8)
ax.plot(clickhouse_resource_data['Query Name'], clickhouse_resource_data['CPU Avg (%)'], label='ClickHouse', marker='o', color='purple', linewidth=4, markersize=8)

# Настройка внешнего вида графика
beautify_plot(ax, 'Среднее использование CPU', 'Название запроса', 'CPU (%)', 'График показывает среднее использование CPU для выполнения запросов на MySQL, Postgres, MongoDB и ClickHouse. Высокие значения указывают на более высокую нагрузку на процессор.')

# Сохранение графика использования CPU в файл
plt.tight_layout()
plt.savefig('result/cpu_usage_comparison.png')

# Создание фигуры и оси для графика среднего использования памяти
fig, ax = plt.subplots(figsize=(14, 7))

# Построение графиков для каждой базы данных с разными цветами и очень толстыми линиями
ax.plot(mysql_resource_data['Query Name'], mysql_resource_data['Memory Avg (%)'], label='MySQL', marker='o', color='blue', linewidth=4, markersize=8)
ax.plot(postgres_resource_data['Query Name'], postgres_resource_data['Memory Avg (%)'], label='Postgres', marker='o', color='green', linewidth=4, markersize=8)
ax.plot(mongodb_resource_data['Query Name'], mongodb_resource_data['Memory Avg (%)'], label='MongoDB', marker='o', color='red', linewidth=4, markersize=8)
ax.plot(clickhouse_resource_data['Query Name'], clickhouse_resource_data['Memory Avg (%)'], label='ClickHouse', marker='o', color='purple', linewidth=4, markersize=8)

# Настройка внешнего вида графика
beautify_plot(ax, 'Среднее использование памяти', 'Название запроса', 'Память (%)', 'График показывает среднее использование памяти для выполнения запросов на MySQL, Postgres, MongoDB и ClickHouse. Высокие значения указывают на более высокое потребление оперативной памяти.')

# Сохранение графика использования памяти в файл
plt.tight_layout()
plt.savefig('result/memory_usage_comparison.png')

# Отображение графиков
plt.show()
