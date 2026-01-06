/*
input:
  duckdb: ./test.ddb
*/
-- name: query_1
CREATE OR REPLACE TABLE user AS
SELECT 1 AS id, 'alice' AS name
UNION ALL
SELECT 2 AS id, 'bob' AS name
UNION ALL
SELECT 3 AS id, 'carol' AS name

;

-- name: query_2
SELECT *
FROM user
;
