# Rafael-Home-Task


In order to run the script,fork it to your local machine and run it.

*Functions available:*

*  setup() --> start a connection with sqlite and create table with 2,000 rows and 3 columns
*  temp_sorted_chunks(engine,chunksize = 200) --> create temp tables in the DB,each contains 200 sorted rows
*  combine_sorted(engine,chunksize=200,output_table_size=2000,output_table_name='sorted_names_table') --> combine the 10 tables from 'temp_sorted_chunks'.The output is sorted table
*  chunk_to_sql(engine,chunk, i) --> chunk from the DB to sql


*Question 2 algorithm:*
* Export the sorted data iteratively to the DB,each time with chunksize of 200
* import top row from each table into 10 items list
* Iterate over all items and import them to the DB

*Question 3 algorithm:*
* Export the sorted data iteratively to the DB,each time with chunksize of 200 with different process (each process represents a server)
* import top row from each table into 10 items list
* Iterate over all items and import them to the DB
* Note: This question is not fully working.

