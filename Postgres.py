from time import perf_counter
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

try:
    connection = psycopg2.connect(user="postgres",
                                  password="123456",
                                  host="localhost",
                                  port="5432",
                                dbname="postgres")
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()


    print("Tiny data:")
    test = 0
    aver1 = 0
    aver2 = 0
    aver3 = 0
    aver4 = 0
    first = 'SELECT "VendorID", count(*) FROM tiny group by 1;'
    second = 'SELECT "passenger_count", avg(total_amount) FROM tiny GROUP BY 1;'
    third = 'SELECT "passenger_count", extract(year from CAST("tpep_pickup_datetime" AS date)) FROM tiny GROUP BY 1, 2'
    fourth = 'SELECT "passenger_count", extract(year from CAST("tpep_pickup_datetime" AS date)), round("trip_distance"), count(*) FROM tiny GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;'
    while test < 10:
        start1 = perf_counter()
        cursor.execute(first)
        fresultt = cursor.fetchall()
        end1 = perf_counter()
        aver1 = aver1 + (end1 - start1)
        start2 = perf_counter()
        cursor.execute(second)
        sresultt = cursor.fetchall()
        end2 = perf_counter()
        aver2 = aver2 + (end2 - start2)
        start3 = perf_counter()
        cursor.execute(third)
        tresultt = cursor.fetchall()
        end3 = perf_counter()
        aver3 = aver3 + (end3 - start3)
        start4 = perf_counter()
        cursor.execute(fourth)
        foresultt = cursor.fetchall()
        end4 = perf_counter()
        aver4 = aver4 + (end4 - start4)
        test += 1
    print("First:")
    print(aver1 / 10)
    print("Second:")
    print(aver2 / 10)
    print("Third:")
    print(aver3 / 10)
    print("Fourth:")
    print(aver4 / 10)

    print("Big data:")
    test = 0
    aver1 = 0
    aver2 = 0
    aver3 = 0
    aver4 = 0
    first = 'SELECT "VendorID", count(*) FROM big group by 1;'
    second = 'SELECT "passenger_count", avg(total_amount) FROM big GROUP BY 1;'
    third = 'SELECT "passenger_count", extract(year from CAST("tpep_pickup_datetime" AS date)) FROM big GROUP BY 1, 2'
    fourth = 'SELECT "passenger_count", extract(year from CAST("tpep_pickup_datetime" AS date)), round("trip_distance"), count(*) FROM big GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;'
    while test < 10:
        start1 = perf_counter()
        cursor.execute(first)
        fresultb = cursor.fetchall()
        end1 = perf_counter()
        aver1 = aver1 + (end1 - start1)
        start2 = perf_counter()
        cursor.execute(second)
        sresultb = cursor.fetchall()
        end2 = perf_counter()
        aver2 = aver2 + (end2 - start2)
        start3 = perf_counter()
        cursor.execute(third)
        tresultb = cursor.fetchall()
        end3 = perf_counter()
        aver3 = aver3 + (end3 - start3)
        start4 = perf_counter()
        cursor.execute(fourth)
        foresultb = cursor.fetchall()
        end4 = perf_counter()
        aver4 = aver4 + (end4 - start4)
        test += 1
    print("First:")
    print(aver1 / 10)
    print("Second:")
    print(aver2 / 10)
    print("Third:")
    print(aver3 / 10)
    print("Fourth:")
    print(aver4 / 10)
finally:
    if connection:
        cursor.close()
        connection.close()
