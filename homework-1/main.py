"""Скрипт для заполнения данными таблиц в БД Postgres."""

import psycopg2
import csv

try:
    conn = psycopg2.connect(host='localhost',
                                  database='north',
                                  user='postgres',
                                  password='postgres')
    print('Соединение установлено')
    with conn:
        with conn.cursor() as cursor:
            with open('north_data/customers_data.csv', 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cursor.execute('INSERT INTO customers VALUES(%s, %s, %s)', (row[0], row[1], row[2]))
            with open('north_data/employees_data.csv', 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cursor.execute('INSERT INTO employees VALUES(%s, %s, %s, %s, %s, %s)',
                                   (row[0], row[1], row[2], row[3], row[4], row[5]))
            with open('north_data/orders_data.csv', 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cursor.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',
                                   (row[0], row[1], row[2], row[3], row[4]))

finally:
    if conn:
        conn.close()
        print('Соединение закрыто.')