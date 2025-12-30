/*
input:
  duckdb: /tmp/test.ddb
*/
-- name: query_1
SELECT * FROM user;

-- name: query_2
SELECT *
FROM user
WHERE name = 'Alice';

-- name: query_3
SELECT COUNT(*) AS user_count;
