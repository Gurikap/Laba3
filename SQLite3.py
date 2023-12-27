from time import perf_counter
import pandas as pd
import sqlite3

tiny = sqlite3.connect('tiny.db')
big = sqlite3.connect('big.db')
datat = pd.read_csv(r"nyc_yellow_tiny.csv")
datat.to_sql('trips', tiny, if_exists='replace', index=False)
datab = pd.read_csv(r"nyc_yellow_big.csv")
datat.to_sql('trips', big, if_exists='replace', index=False)

first = 'SELECT VendorID, count(*) FROM trips GROUP BY 1;'
second = 'SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;'
third = '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;'''
fourth = '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;'''

cursor = tiny.cursor()
test = 0
aver1 = 0
aver2 = 0
aver3 = 0
aver4 = 0
print("Tiny data:")
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
print(aver1/10)
print("Second:")
print(aver2/10)
print("Third:")
print(aver3/10)
print("Fourth:")
print(aver4/10)
cursor.close()
tiny.close()

cursor = big.cursor()
test = 0
aver1 = 0
aver2 = 0
aver3 = 0
aver4 = 0
print("Big data:")
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
print(aver1/10)
print("Second:")
print(aver2/10)
print("Third:")
print(aver3/10)
print("Fourth:")
print(aver4/10)
cursor.close()
big.close()