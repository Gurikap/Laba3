from time import perf_counter
import duckdb

tiny = duckdb.connect('tiny.duckdb')
big = duckdb.connect('big.duckdb')

first = 'SELECT VendorID, count(*) FROM trip GROUP BY 1;'
second = 'SELECT passenger_count, avg(total_amount) FROM trip GROUP BY 1;'
third = '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM trip GROUP BY 1, 2;'''
fourth = '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), round(trip_distance), count(*) FROM trip GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;'''
cursor = tiny.cursor()
cursor.execute("CREATE TABLE trip AS SELECT * FROM read_csv_auto('nyc_yellow_tiny.csv');")
test = 0
aver1 = 0
aver2 = 0
aver3 = 0
aver4 = 0
print("Tiny data")
while test < 10:
    start1 = perf_counter()
    fresultt = cursor.execute(first).fetchall()
    end1 = perf_counter()
    aver1 = aver1 + (end1 - start1)
    start2 = perf_counter()
    sresultt = cursor.execute(second).fetchall()
    end2 = perf_counter()
    aver2 = aver2 + (end2 - start2)
    start3 = perf_counter()
    tresultt = cursor.execute(third).fetchall()
    end3 = perf_counter()
    aver3 = aver3 + (end3 - start3)
    start4 = perf_counter()
    foresultt = cursor.execute(fourth).fetchall()
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
cursor.close()
tiny.close()

test = 0
aver1 = 0
aver2 = 0
aver3 = 0
aver4 = 0
print("Big data")
cursor = big.cursor()
cursor.execute("CREATE TABLE trip AS SELECT * FROM read_csv_auto('nyc_yellow_big.csv');")
while test < 10:
    start1 = perf_counter()
    fresultb = cursor.execute(first).fetchall()
    end1 = perf_counter()
    aver1 = aver1 + (end1 - start1)
    start2 = perf_counter()
    sresultb = cursor.execute(second).fetchall()
    end2 = perf_counter()
    aver2 = aver2 + (end2 - start2)
    start3 = perf_counter()
    tresultb = cursor.execute(third).fetchall()
    end3 = perf_counter()
    aver3 = aver3 + (end3 - start3)
    start4 = perf_counter()
    foresultb = cursor.execute(fourth).fetchall()
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
cursor.close()
big.close()