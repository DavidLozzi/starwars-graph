SELECT * FROM public."all_data" a WHERE ID = 1227
ORDER BY id LIMIT 100 OFFSET 0

DELETE FROM words
SELECT lower(word), count(*) FROM words GROUP BY lower(word) HAVING count(*) > 1
SELECT count(*) FROM words
SELECT * FROM words WHERE word_length = 5 and is_star_wars = true

EXPLAIN ANALYZE
SELECT * FROM public."all_data" ORDER BY id LIMIT 16900 OFFSET 17000

EXPLAIN ANALYZE
SELECT * FROM public.all_data WHERE id >= 16900 ORDER BY id ASC LIMIT 200;

SELECT MAX(id), MIN(id) FROM public.all_data
SELECT MAX(last_ingested) FROM public.all_data


SELECT COUNT(*) FROM pg_stat_activity
SELECT * FROM pg_stat_activity;
SELECT pg_advisory_unlock_all();
CLOSE ALL;
UNLISTEN *;
RESET ALL;