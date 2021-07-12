# Rafael-Home-Task


In order to run the scripy,fork it to your local machine and run it.

*Functions available:*

*  setup() --> start a connection with sqlite and create table with 2,000 rows and 3 columns
*  temp_sorted_chunks(engine,chunksize = 200) --> create temp tables in the DB,each contains 200 sorted rows
*  combine_sorted(engine,chunksize=200,output_table_size=2000,output_table_name='sorted_names_table') --> combine the 10 tables from 'temp_sorted_chunks' with sorting

