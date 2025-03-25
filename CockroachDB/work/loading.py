import os
import csv
import psycopg2
import logging
import time

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s', handlers=[
    logging.FileHandler("load_debug.log"),
    logging.StreamHandler()
])

ROOT = os.path.dirname(os.path.abspath(__file__))
SSB_DATA_DIR = os.path.join(ROOT, "ssb-data/")

def load_data():
    connection = psycopg2.connect(
        host="localhost",
        user="root",
        password="",
        database="ssb",
        port='26257'
    )

    def load_csv_to_table(filename, table_name, columns):
        logging.info(f"Загрузка данных в таблицу: {table_name}")
        file_path = os.path.join(SSB_DATA_DIR, filename)
        try:
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Пропустить заголовок
                batch_size = 1000
                batch = []
                for idx, row in enumerate(reader):
                    if len(row) != len(columns):
                        logging.warning(f"Skipping row {idx} with incorrect number of columns: {row}")
                        continue

                    batch.append(row)

                    if len(batch) == batch_size:
                        attempt = 0
                        while attempt < 5:
                            try:
                                cursor = connection.cursor()
                                placeholders = ','.join(['%s'] * len(columns))
                                insert_query = f'INSERT INTO {table_name} ({",".join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
                                cursor.executemany(insert_query, batch)
                                connection.commit()
                                cursor.close()
                                logging.debug(f"Batch {idx//batch_size} successfully inserted into {table_name}")
                                batch.clear()
                                break
                            except psycopg2.OperationalError as e:
                                logging.error(f"OperationalError on batch {idx//batch_size}: {e}, retrying in 5 seconds...")
                                time.sleep(5)
                                attempt += 1
                            except psycopg2.errors.InFailedSqlTransaction:
                                logging.error(f"Transaction aborted on batch {idx//batch_size}, retrying in 5 seconds...")
                                time.sleep(5)
                                attempt += 1
                            except Exception as e:
                                logging.error(f"Unexpected error on batch {idx//batch_size}: {e}")
                                break
                
                # Обработка оставшихся строк
                if batch:
                    attempt = 0
                    while attempt < 5:
                        try:
                            cursor = connection.cursor()
                            placeholders = ','.join(['%s'] * len(columns))
                            insert_query = f'INSERT INTO {table_name} ({",".join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
                            cursor.executemany(insert_query, batch)
                            connection.commit()
                            cursor.close()
                            logging.debug(f"Final batch successfully inserted into {table_name}")
                            break
                        except psycopg2.OperationalError as e:
                            logging.error(f"OperationalError on final batch: {e}, retrying in 5 seconds...")
                            time.sleep(5)
                            attempt += 1
                        except psycopg2.errors.InFailedSqlTransaction:
                            logging.error(f"Transaction aborted on final batch, retrying in 5 seconds...")
                            time.sleep(5)
                            attempt += 1
                        except Exception as e:
                            logging.error(f"Unexpected error on final batch: {e}")
                            break

        except PermissionError as e:
            logging.error(f"PermissionError while accessing file {file_path}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error while accessing file {file_path}: {e}")

    load_csv_to_table('lineorder.csv', 'lineorder', ['lo_orderkey', 'lo_linenumber', 'lo_custkey', 'lo_partkey', 'lo_suppkey', 'lo_orderdate', 'lo_orderpriority', 'lo_shippriority', 'lo_quantity', 'lo_extendedprice', 'lo_ordtotalprice', 'lo_discount', 'lo_revenue', 'lo_supplycost', 'lo_tax', 'lo_commitdate', 'lo_shipmode'])

    connection.close()

if __name__ == "__main__":
    load_data()
