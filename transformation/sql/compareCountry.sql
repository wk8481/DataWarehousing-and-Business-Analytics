-- Count identical rows between country and country2
SELECT COUNT(*) AS identical_rows_count
FROM country t1
INNER JOIN country2 t2 ON t1.code = t2.code AND t1.code3 = t2.code3 AND t1.name = t2.name;

-- Count distinct rows in country based on code, code3, and name
SELECT COUNT(DISTINCT code + code3 + name) AS distinct_country_rows_country1 FROM country;

-- Count distinct rows in country2 based on code, code3, and name
SELECT COUNT(DISTINCT code + code3 + name) AS distinct_country_rows_country2 FROM country2;

