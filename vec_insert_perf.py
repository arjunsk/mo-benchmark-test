"""
create database a;
use a;
create table t3(a int, b vecf32(128));
"""
import binascii
import time

import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

table_name = "t3"
vec_len = 128
num_inserts = 1000_000
num_vector_per_insert = 1


def to_db_binary(value):
    if value is None:
        return value

    value = np.asarray(value, dtype='<f4')
    if value.ndim != 1:
        raise ValueError('expected ndim to be 1')

    return binascii.b2a_hex(value.tobytes()).decode('utf-8')


def run():
    # Using mysqlclient (MySQLdb)
    engine = create_engine("mysql+mysqldb://root:111@127.0.0.1:6001/a")
    Session = sessionmaker(bind=engine)
    session = Session()

    sql_insert = text("INSERT INTO t3 (a, b) VALUES (:id, decode(:data,'hex'));")
    for i in range(num_inserts * num_vector_per_insert):
        arr = np.random.rand(vec_len)
        arr = np.asarray(arr, dtype='<f')
        session.execute(sql_insert, {"id": i, "data": to_db_binary(arr)})
        if i % 1000 == 0:
            print(f"Inserted {i} strings")
    session.commit()


def main():
    start = time.time()
    run()
    duration = time.time() - start

    print(f"Result: vector dim={vec_len} vectors "
          f"inserted={num_inserts * num_vector_per_insert} "
          f"insert/second={num_inserts * num_vector_per_insert / duration}")


if __name__ == "__main__":
    main()