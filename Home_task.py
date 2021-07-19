import sqlite3
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os, sys
import time, random, string
import multiprocessing



def setup():
    """
    Create table with 2,000 rows and 3 columns.

    Returns
    -------
    engine:sqlite object

    """

    N = 2_000
    char_len = 5
    random.seed(5)
    id = [
        ("".join(random.choices(string.ascii_uppercase, k=char_len))) for _ in range(N)
    ]
    data = []
    for i in range(N):
        random_length = random.randint(5, 15)
        data.append("".join(random.choices(string.ascii_uppercase, k=random_length)))
    df = pd.DataFrame({"index": [i for i in range(1, N + 1)], "id": id, "data": data})

    engine = create_engine("sqlite://", echo=False)
    df.to_sql("ads", con=engine, index=False)
    return engine


# q2
def temp_sorted_chunks(engine, chunksize=200):
    """
    sort chunksize and export to db.

    Parameters
    ----------
    engine: sqlite object
    chunksize: int,maximum processing capability

    """

    sql = "SELECT data FROM ads"
    i = 1
    for chunk in pd.read_sql_query(sql, engine, chunksize=chunksize):
        chunk = chunk.sort_values("data")
        chunk = chunk.rename(columns={f"data": f"chunk_{i}"})
        chunk.to_sql(f"chunk_{i}", con=engine, index=False)
        i += 1


def combine_sorted(
    engine,
    chunksize=200,
    output_table_size=2000,
    output_table_name="sorted_names_table",
):
    """
    combine sorted temp tables and export to db.

    Parameters
    ----------
    engine: sqlite object
    chunksize: int,maximum processing capability
    output_table_size: int,final table size
    output_table_name: str,table name

    """
    temp = []  # to Db
    min_list = []  # contains 10 min values in each iteration

    for i in range(1, 11):
        sql_query = f"SELECT  * FROM chunk_{i} limit 1"
        result = engine.execute(sql_query).fetchall()
        result = [r for r, in result][0]
        min_list.append(result)

    for i in range(output_table_size):
        min_val = min(min_list)
        temp.append(min_val)
        if len(temp) == chunksize:
            df_to_db = pd.DataFrame({"data": temp})
            df_to_db.to_sql(
                output_table_name, con=engine, index=False, if_exists="append"
            )
        chunk_num = np.argmin(min_list) + 1
        del_query = (
            f"delete from chunk_{chunk_num} where chunk_{chunk_num} in"
            f"  (select chunk_{chunk_num} from chunk_{chunk_num} limit 1)"
        )
        engine.execute(del_query)
        sql_query = f"SELECT  * FROM chunk_{chunk_num} limit 1"
        result = engine.execute(sql_query).fetchall()
        try:
            res = [r for r, in result][0]
        except:  # reach end of table
            res = "Z" * 30
            pass
        min_list[np.argmin(min_list)] = res
        if len(temp) == chunksize:
            temp = []


def chunk_to_sql(engine, chunk, i):
    """
    sorted temp tables and export to db.

    Parameters
    ----------
    engine: sqlite object
    chunks: int,maximum processing capability
    i: int,temp table's name suffix

    """
    name = f"chunk_{i}"
    chunk = chunk.sort_values("data").rename(columns={"data": name})
    chunk.to_sql(name, con=engine, index=False)


if __name__ == "__main__":

    start = time.time()
    engine = setup()

    # query the data
    df = pd.read_sql_table("ads", engine)
    output1 = df  # ;print(output1)

    # sorted list of Data by name
    col = df["data"].tolist()
    col.sort()

    # store the list and time
    end = time.time() - start
    df = pd.DataFrame({"sorting-step1": col})
    df["Sorting-step1_process_time"] = round(end, 10)
    df.to_sql("results", con=engine, index=False)

    output2 = pd.read_sql_table("results", engine)  # ;print(output2)

    # q2

    start = time.time()
    temp_sorted_chunks(engine)
    combine_sorted(engine)
    df = pd.read_sql_table("sorted_names_table", engine)
    end = time.time() - start
    df["Sorting-step2_process_time"] = round(end, 10)
    df.to_sql("results_q2", con=engine, index=False)

    output3 = pd.read_sql_table("results_q2", engine)  # ;print(output3)

    # q3
    processes = []
    sql = "SELECT data FROM ads"
    for chunk in pd.read_sql_query(sql, engine, chunksize=200):
        p = multiprocessing.Process(target=chunk_to_sql, args=(engine, chunk, i))
        p.start()
        processes.append(p)
        i += 1
    for p in processes:
        p.join()

    start = time.time()
    temp_sorted_chunks(engine)
    combine_sorted(engine)
    df = pd.read_sql_table("sorted_names_table2", engine)
    end = time.time() - start
    df["Sorting-step3_process_time"] = round(end, 10)
    df.to_sql("results_q3", con=engine, index=False)

    output4 = pd.read_sql_table("results_q3", engine)  # ;print(output4)


# ----------------------------------------------------------------
# Outputs
# ----------------------------------------------------------------
"""
Outputs:

Output1
      index     id             data
0         1  QTUYT       PODRTXQGOZ
1         2  XAMYQ          RTEYMAZ
2         3  XCMGO     RQWYPILXRGAS
3         4  OAFHX        JEWPWLXAO
4         5  TEUDQ  DRBUQZWKGJMKNUD
...     ...    ...              ...
1995   1996  MDSUG   LFYSXUFSNNZGCR
1996   1997  NESNN      QFGWXGQAXOL
1997   1998  ILLEM  ODREACZYAYCBJFP
1998   1999  RRYFW   URNXEAAEKQSMNR
1999   2000  LWCRG         AHDTZUHX

[2000 rows x 3 columns]
Output2
       sorting-step1  Sorting-step1_process_time
0     AASIFCDSYTCQVK                    0.057853
1             AASJUY                    0.057853
2             AAZJAR                    0.057853
3        ABCHEMYVQPD                    0.057853
4            ACGGWJY                    0.057853
...              ...                         ...
1995      ZXEZGNTWUS                    0.057853
1996    ZXFIZTLYGIZE                    0.057853
1997        ZXLEIBPO                    0.057853
1998        ZXNAFRCY                    0.057853
1999       ZXQXTTPGK                    0.057853

[2000 rows x 2 columns]
Output3
                data  Sorting-step2_process_time
0     AASIFCDSYTCQVK                    0.391951
1             AASJUY                    0.391951
2             AAZJAR                    0.391951
3        ABCHEMYVQPD                    0.391951
4            ACGGWJY                    0.391951
...              ...                         ...
1995      ZXEZGNTWUS                    0.391951
1996    ZXFIZTLYGIZE                    0.391951
1997        ZXLEIBPO                    0.391951
1998        ZXNAFRCY                    0.391951
1999       ZXQXTTPGK                    0.391951


"""
