from time import perf_counter
import pandas as pd
tiny = pd.read_csv(r"D:\HSE\DB\nyc_yellow_tiny.csv")
big = pd.read_csv(r"D:\HSE\DB\nyc_yellow_big.csv")
test = 0
aver1 = 0
aver2 = 0
aver3 = 0
aver4 = 0
print("Tiny data results:")
while test < 10:
    start1 = perf_counter()
    fresultt = tiny.groupby(["VendorID"]).size()
    end1 = perf_counter()
    aver1 = aver1 + (end1 - start1)
    start2 = perf_counter()
    sresultt = tiny.groupby(["passenger_count"])["total_amount"].mean()
    end2 = perf_counter()
    aver2 = aver2 + (end2 - start2)
    start3 = perf_counter()
    tiny['Year'] = pd.to_datetime(tiny['tpep_pickup_datetime']).dt.year
    tresultt = tiny.groupby(['passenger_count', 'Year']).size()
    end3 = perf_counter()
    aver3 = aver3 + (end3 - start3)
    start4 = perf_counter()
    tiny['Year'] = pd.to_datetime(tiny['tpep_pickup_datetime']).dt.year
    tiny['trip_distance'] = tiny['trip_distance'].round()
    foresultt = tiny.groupby(['passenger_count', 'Year', 'trip_distance']).size().reset_index(name='count').sort_values(['Year', 'count'], ascending=[True, False])
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

test = 0
aver1 = 0
aver2 = 0
aver3 = 0
aver4 = 0
print("Big data results:")
while test < 10:
    start1 = perf_counter()
    fresultb = big.groupby(["VendorID"]).size()
    end1 = perf_counter()
    aver1 = aver1 + (end1 - start1)
    start2 = perf_counter()
    sresultb = big.groupby(["passenger_count"])["total_amount"].mean()
    end2 = perf_counter()
    aver2 = aver2 + (end2 - start2)
    start3 = perf_counter()
    big['Year'] = pd.to_datetime(big['tpep_pickup_datetime']).dt.year
    tresultb = big.groupby(['passenger_count', 'Year']).size()
    end3 = perf_counter()
    aver3 = aver3 + (end3 - start3)
    start4 = perf_counter()
    big['Year'] = pd.to_datetime(big['tpep_pickup_datetime']).dt.year
    big['trip_distance'] = big['trip_distance'].round()
    foresultb = big.groupby(['passenger_count', 'Year', 'trip_distance']).size().reset_index(name='count').sort_values(['Year', 'count'], ascending=[True, False])
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