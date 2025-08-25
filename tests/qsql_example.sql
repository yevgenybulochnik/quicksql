/*
output_dir: ./data
vars:
  my_var_1: Alice
  my_var_2: 25
  my_table: user
*/

-- Name: first_query
SELECT * FROM user;

-- Name: second_query
SELECT *
FROM user
WHERE name = {{ my_var_1 }};

-- Name: third_query
SELECT *
FROM user
WHERE age = {{ my_var_2 }};

-- Name: fourth_query
SELECT
  name
FROM {{ my_table }};
