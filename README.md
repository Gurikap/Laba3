Даны 4 библиотеки: DuckDB, Pandas, Psycopg 2, SQLite Задачи: написать бенчмарк для измерения скорости выполнения четырёх запросов:

SELECT VendorId, count(*) FROM db GROUP BY 1;
SELECT passenger_count, avg(total_amount) FROM db GROUP BY 1;
SELECT passenger_count, extract(year from pickup_datetime), count(*) FROM db GROUP BY 1, 2;
SELECT passenger_count, extract(year from pickup_datetime), round(trip_distance), count(*) FROM db GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;

О конфиг файле. Идею подсмотрел у Марии Щкулёвой, как и идею всей лабы в принципе.
test_count - количество запусков
query_print - печать запросов, false по умолчанию
csv_file - ссылка на оригинальный csv (только маленький)
Для постгреса можно ввести свои данные
![таблица](https://github.com/Gurikap/Laba3/assets/154047638/b0c3b26a-48a5-4e3a-b04a-1cb37fafa505)

Выводы:
Самая быстрая библиотека: Pandas(но ест очень много оперативки)
Самая медленная: SQLite3
Самая стабильная: DuckDB
Лично для меня самой удобной библиотекой является Pandas. Самой неудобной - Psycopg2.
