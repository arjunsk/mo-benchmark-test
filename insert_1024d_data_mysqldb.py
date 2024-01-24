# CREATE DATABASE a;
# USE a;
# CREATE TABLE speedtest (id int, one_k_vector vecf32(1024));
import binascii
import time

import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

table_name = "speedtest"
vec_len = 1024
num_inserts = 1_000_000
num_vector_per_insert = 1


def to_db_binary(value, dim=None):
    if value is None:
        return value

    value = np.asarray(value, dtype='<f')

    if value.ndim != 1:
        raise ValueError('expected ndim to be 1')

    # return value.tobytes()
    return binascii.b2a_hex(value)


def run():
    # mysql+pymysql:
    engine = create_engine("mysql+mysqldb://root:111@127.0.0.1:6001/a")
    Session = sessionmaker(bind=engine)
    session = Session()

    sql_insert = text("insert into speedtest (id, one_k_vector) values(:id, decode(:data,'hex') );")
    for i in range(num_inserts * num_vector_per_insert):
        arr = np.random.rand(vec_len)
        arr = np.asarray(arr, dtype='<f')
        session.execute(sql_insert, {"id": i, "data": to_db_binary(arr)})
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
