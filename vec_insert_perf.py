import binascii
import time
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

table_name = "speedtest"
vec_len = 128
num_inserts = 1000_000


def to_db_binary(value):
    if value is None:
        return value

    value = np.asarray(value, dtype='<f')

    if value.ndim != 1:
        raise ValueError('expected ndim to be 1')

    return binascii.b2a_hex(value)


def run():
    # Connect using mysqlclient (MySQLdb)
    engine = create_engine("mysql+mysqldb://root:111@127.0.0.1:6001/")
    Session = sessionmaker(bind=engine)
    session = Session()

    # Creating the database and table
    session.execute(text("DROP DATABASE IF EXISTS vecdb;"))
    session.commit()

    session.execute(text("CREATE DATABASE vecdb;"))
    session.execute(text("USE vecdb;"))
    session.execute(text("DROP TABLE IF EXISTS "+table_name+";"))
    session.execute(text("CREATE TABLE "+table_name+"(id INT, one_k_vector VARCHAR(1024));"))
    session.commit()

    sql_insert = text("INSERT INTO "+table_name+" (id, one_k_vector) VALUES (:id, :data);")
    for i in range(num_inserts):
        arr = np.random.rand(vec_len)
        arr = np.asarray(arr, dtype='<f')
        session.execute(sql_insert, {"id": i, "data": to_db_binary(arr)})
        if i % 1000 == 0:
            print(f"inserted {i}")
    session.commit()


def main():
    start = time.time()
    run()
    duration = time.time() - start

    print(f"Result: vector dim={vec_len} vectors "
          f"inserted={num_inserts} "
          f"insert/second={num_inserts / duration}")


if __name__ == "__main__":
    main()
