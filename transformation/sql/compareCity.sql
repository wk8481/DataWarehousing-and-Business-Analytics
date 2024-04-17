SELECT COUNT(*) AS identical_rows_count
FROM city t1
INNER JOIN city2 t2 ON t1.city_id = t2.city_id AND t1.city_name = t2.city_name;

SELECT COUNT(DISTINCT CONVERT(VARCHAR(36), city_id) + city_name) AS distinct_city_rows_city1 FROM city;
SELECT COUNT(DISTINCT CONVERT(VARCHAR(36), city_id) + city_name) AS distinct_city_rows_city2 FROM city2;