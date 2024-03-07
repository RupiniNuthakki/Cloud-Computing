-- Loading input matrix and vector
matrix = LOAD '$M' USING PigStorage(',') AS (row:int, col:int, value:double);
vector = LOAD '$V' USING PigStorage(',') AS (row:int, value:double);

-- Joining matrix and vector by column in the matrix and row in the vector
A = JOIN matrix BY col, vector BY row;
dump A; -- view the contents of A in command prompt

-- For each tuple in A, it generates a new tuple with specified fields
B = FOREACH A GENERATE matrix::row AS row, (matrix::value)*(vector::value) AS value;
dump B; -- view the contents of B in command prompt

-- Grouping the tuples in B by row of the matrix using group by
C = GROUP B BY row;
dump C; -- view the contents of C in command prompt

-- For each group in C, it generates a new tuple with specified fields like row and value as sum of tuples in B which has the same row
final_result = FOREACH C GENERATE FLATTEN(group) AS row, SUM(B.value) AS value;
dump final_result; -- view the contents of final output in command prompt

-- Storing the output vector in a specified file
STORE final_result INTO '$O' USING PigStorage(',');