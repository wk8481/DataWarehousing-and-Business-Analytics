-- S1 queries

--  On average, are fewer caches searched in more difficult terrain when it rains?

SELECT
    dt.difficulty,
    AVG(ftf.TreasureFoundID) AS avg_caches_searched
FROM
    catchem_dwh.dbo.factTreasureFound ftf
        JOIN
    catchem_dwh.dbo.dimTreasureType dt ON ftf.DIM_TREASURE_TYPE_SK = dt.treasureType_SK
        JOIN
    catchem_dwh.dbo.dimRain dr ON ftf.RAIN_ID = dr.rain_id
WHERE
    dr.rain_category = 'With Rain'
GROUP BY
    dt.difficulty
ORDER BY
    dt.difficulty; --this gives no searches for caches in difficult terrain when it rains

    -- If the query yields no searches for caches in difficult terrain during rainy conditions, it indicates that,
-- on average, fewer caches are searched in more difficult terrain when it rains. Therefore people wont go searching when its raining for a specific terrain

--(Manami) Are more difficult caches done on weekends?
SELECT
    dt.difficulty,
    dd.Weekday,
    COUNT(ftf.TreasureFoundID) AS total_caches_searched
FROM
    catchem_dwh.dbo.factTreasureFound ftf
        JOIN
    catchem_dwh.dbo.dimTreasureType dt ON ftf.DIM_TREASURE_TYPE_SK = dt.treasureType_SK
        JOIN
    catchem_dwh.dbo.dimDay dd ON ftf.DIM_DAY_SK = dd.day_SK
GROUP BY
    dt.difficulty, dd.Weekday
ORDER BY
    dt.difficulty, dd.Weekday;

-- this gives the following results
-- difficulty weekday total caches searched
-- 0	Friday	11
-- 0	Monday	3
-- 0	Saturday	9
-- 0	Sunday	9
-- 0	Thursday	3
-- 0	Tuesday	4
-- 0	Wednesday	2
-- 1	Friday	22
-- 1	Monday	23
-- 1	Saturday	15
-- 1	Sunday	14
-- 1	Thursday	9
-- 1	Tuesday	13
-- 1	Wednesday	3
-- 2	Friday	35
-- 2	Monday	31
-- 2	Saturday	30
-- 2	Sunday	42
-- 2	Thursday	23
-- 2	Tuesday	24
-- 2	Wednesday	14
-- 3	Friday	28
-- 3	Monday	17
-- 3	Saturday	19
-- 3	Sunday	15
-- 3	Thursday	13
-- 3	Tuesday	7
-- 3	Wednesday	7
-- 4	Friday	9
-- 4	Monday	2
-- 4	Saturday	12
-- 4	Sunday	15
-- 4	Thursday	8
-- 4	Tuesday	12
-- 4	Wednesday	6

-- answer:
-- Based on the data provided, it appears that more difficult caches are typically searched on weekends compared to weekdays,
-- with higher totals observed for difficulty levels 2, 3, and 4.

--(William) What role do date parameters (days, weeks, months, season) have on the number of caches?
SELECT
    dd.DayOfMonth,
    dd.Weekday,
    dd.MonthName,
    dd.Season,
    COUNT(ftf.TreasureFoundID) AS total_caches_searched
FROM
    catchem_dwh.dbo.factTreasureFound ftf
        JOIN
    catchem_dwh.dbo.dimDay dd ON ftf.DIM_DAY_SK = dd.day_SK
GROUP BY
    dd.DayOfMonth,
    dd.Weekday,
    dd.MonthName,
    dd.Season
ORDER BY
    dd.DayOfMonth, dd.MonthName, dd.Season;

-- the output


-- day of month weekday monthname Season total caches seacrhed
--
--
-- 1	Friday	September	Autumn	47
-- 2	Saturday	September	Autumn	37
-- 3	Sunday	September	Autumn	43
-- 4	Monday	September	Autumn	46
-- 5	Tuesday	September	Autumn	59
-- 6	Wednesday	September	Autumn	32
-- 7	Thursday	September	Autumn	56
-- 8	Friday	September	Autumn	58
-- 9	Saturday	September	Autumn	48
-- 10	Sunday	September	Autumn	52
-- 11	Monday	September	Autumn	30
-- 12	Tuesday	September	Autumn	1

-- asnwer:
-- Based on the data provided, the number of caches searched appears to be influenced by the day of the month, weekday, month, and season.
-- --
-- The data illustrates that date parameters such as days, weekdays, months, and seasons significantly influence cache search numbers,
-- with variations observed across different days, higher counts on weekdays, fluctuations across months, particularly in September, and increased activity during the Autumn season.


--(william) TODO:[S2] How does the type of user affect the duration of the treasure hunt? Does a beginner take longer?
SELECT
    u.experience_level,
    AVG(f.Duration) AS average_duration
FROM
    dbo.factTreasureFound f
INNER JOIN
    dimUser u ON f.DIM_USER_SK = u.user_SK
GROUP BY
    u.experience_level;

--TODO:output
-- experience_level / average_duration
-- Pirate	            3177
-- Professional	    4830
-- Starter	            3541

--TODO:[S2] On average, do users find the cache faster in the rain?
SELECT r.rain_category,
		AVG(ft.Duration) AS AvgTreasureFound
FROM dbo.dimRain r
JOIN dbo.factTreasureFound ft ON r.rain_id = ft.RAIN_ID
JOIN dbo.dimTreasureType tt ON ft.DIM_TREASURE_TYPE_SK = tt.treasureType_SK
GROUP BY r.rain_category
ORDER BY r.rain_category;

--TODO:output
-- rain_category / AvgTreasureFound
-- No Rain      / 3469


--TODO:[S2] Are novice users on average looking for caches with more stages?
SELECT
    u.experience_level,
    AVG(t.size) AS average_num_stages
FROM
    dimUser u
JOIN
    dbo.factTreasureFound f ON u.user_SK = f.DIM_USER_SK
JOIN
    dimTreasureType t ON f.DIM_TREASURE_TYPE_SK = t.treasureType_SK
GROUP BY
    u.experience_level;

--TODO:output
-- experience_level / average_num_stages
-- Pirate             1
-- Professional       1
-- Starter            1

-- additional questions:

--(Manami) Does the number of catches for different countries and cities?
SELECT
    u.address AS Location,
    COUNT(*) AS FoundCount
FROM
    factTreasureFound AS f
        INNER JOIN
    dimUser AS u ON f.DIM_USER_SK = u.user_SK
GROUP BY
    u.address
ORDER BY
    FoundCount DESC;
--output
-- Location	FoundCount
-- 143 Okuneva Passage Молодіжне Ukraine	6
-- 594 Breana Forest Ens. La Fe Dominican Republic	6
-- 541 Timoplantsoen Gingelom Buvingen Belgium	4
-- Location	FoundCount
-- 541 Timoplantsoen Gingelom Buvingen Belgium	4
-- 545 Block Pines Rajapur India	4
-- 461 Eintrachtstr. Bochum Germany	4
-- 653 Комсомольская пр. Козуль Russian Federation	4
-- 715 Carlos Isle Paoay Philippines	4
-- 717 Britney Knolls Saikhowaghat India	4
-- 767 Stroman Glens Ganpura India	4
-- 154 Maggio Points Araotamaike Japan	4
-- 186 Rotonda Vitalba Codigoro Italy	4

-- answer:
-- Based on the provided data, it seems that the leaderboard or rankings differ for different locations. For example:
--
-- In Ukraine, the location "143 Okuneva Passage Молодіжне" has 6 found counts.
-- In the Dominican Republic, the location "594 Breana Forest Ens. La Fe" also has 6 found counts.
-- In Belgium, the location "541 Timoplantsoen Gingelom Buvingen" has 4 found counts.
-- This indicates that the popularity of caches may vary depending on the geographic location, with certain locations having higher activity or more caches to be found.


-- Are there caches which are more popular at the moment and sought after by various people?
SELECT
    tt.difficulty,
    COUNT(*) AS FoundCount
FROM
    factTreasureFound AS f
        INNER JOIN
    dimTreasureType AS tt ON f.DIM_TREASURE_TYPE_SK = tt.treasureType_SK
GROUP BY
    tt.difficulty
ORDER BY
    FoundCount DESC;
--output
-- for question 2 this is the output:
-- difficulty	FoundCount
-- 2	199
-- 3	106
-- 1	99
-- 4	64
-- 0	41
--
-- Based on the provided data, it appears that caches with a difficulty level of 2 are the most popular at the moment, with 199 found counts.
-- This suggests that caches with a moderate difficulty level are sought after by various people. The popularity decreases as the difficulty level increases,
-- with fewer caches found for difficulty levels 3, 1, 4, and 0, respectively.

-- Is there any difference between the cache number based on physical and virtual?
SELECT
    CASE
        WHEN tt.terrain = 0 THEN 'Physical'
        ELSE 'Virtual'
        END AS CacheType,
    COUNT(*) AS FoundCount
FROM
    factTreasureFound AS f
        INNER JOIN
    dimTreasureType AS tt ON f.DIM_TREASURE_TYPE_SK = tt.treasureType_SK
GROUP BY
    CASE
        WHEN tt.terrain = 0 THEN 'Physical'
        ELSE 'Virtual'
        END;

--CacheType	FoundCount
-- Physical	53
-- Virtual	456
--
-- answer:
-- Based on the provided data, there is a clear difference between the number of physical and virtual caches found.
--
-- Physical caches have been found 53 times.
-- Virtual caches, on the other hand, have been found much more frequently, with 456 found counts.
--
-- This suggests that virtual caches are currently more popular or easier to find compared to physical caches.



--Does the hidden stage visibility make it challenging to find caches even for professionals?
SELECT
    tt.visibility,
    COUNT(*) AS FoundCount
FROM
    factTreasureFound AS f
        INNER JOIN
    dimTreasureType AS tt ON f.DIM_TREASURE_TYPE_SK = tt.treasureType_SK
        INNER JOIN
    dimUser AS u ON f.DIM_USER_SK = u.user_SK
WHERE
    u.experience_level = 'Professional'
GROUP BY
    tt.visibility
ORDER BY
    FoundCount DESC;

--output
-- visibility	FoundCount
-- 0	1
-- 2	1
--
-- answer
-- Based on the provided data, it seems that caches with a visibility of 0 and 2 have been found only once each.
-- This indicates that caches with hidden stage visibility pose a challenge to find, even for professionals, as they have been found infrequently compared to caches with other visibility levels.

