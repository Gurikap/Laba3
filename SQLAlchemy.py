from time import perf_counter
import pandas as pd
import sqlalchemy

tengine = sqlalchemy.create_engine("sqlite:///tiny.db")
tiny = tengine.connect()
bengine = sqlalchemy.create_engine("sqlite:///big.db")
big = bengine.connect()
datat = pd.read_csv(r"nyc_yellow_tiny.csv")
datat.to_sql('trips', tiny, if_exists='replace', index=False)
datab = pd.read_csv(r"nyc_yellow_big.csv")
datat.to_sql('trips', big, if_exists='replace', index=False)

first = sqlalchemy.text('''SELECT "VendorID", count(*) FROM trips GROUP BY 1;''')
second = sqlalchemy.text('''SELECT "passenger_count", avg(total_amount) FROM trips GROUP BY 1;''')
third = sqlalchemy.text('''SELECT "passenger_count", strftime('%Y', "tpep_pickup_datetime"), count(*) FROM trips GROUP BY 1, 2;''')
fourth = sqlalchemy.text('''SELECT "passenger_count", strftime('%Y', "tpep_pickup_datetime"), round("trip_distance"), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;''')

test = 0
aver1 = 0
aver2 = 0
aver3 = 0
aver4 = 0
print("Tiny data:")
while test < 10:
    start1 = perf_counter()
    fresultt = tiny.execute(first)
    end1 = perf_counter()
    aver1 = aver1 + (end1 - start1)
    start2 = perf_counter()
    sresultt = tiny.execute(second)
    end2 = perf_counter()
    aver2 = aver2 + (end2 - start2)
    start3 = perf_counter()
    tresultt = tiny.execute(third)
    end3 = perf_counter()
    aver3 = aver3 + (end3 - start3)
    start4 = perf_counter()
    foresultt = tiny.execute(fourth)
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

test = 0
aver1 = 0
aver2 = 0
aver3 = 0
aver4 = 0
print("Big data:")
while test < 10:
    start1 = perf_counter()
    fresultb = big.execute(first)
    end1 = perf_counter()
    aver1 = aver1 + (end1 - start1)
    start2 = perf_counter()
    sresultb = big.execute(second)
    end2 = perf_counter()
    aver2 = aver2 + (end2 - start2)
    start3 = perf_counter()
    tresultb = big.execute(third)
    end3 = perf_counter()
    aver3 = aver3 + (end3 - start3)
    start4 = perf_counter()
    foresultb = big.execute(fourth)
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