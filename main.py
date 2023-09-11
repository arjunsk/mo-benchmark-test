# CREATE DATABASE a;
# USE a;
# CREATE TABLE speedtest (id int, one_k_vector vecf32(1024));
# CREATE TABLE speedtest (id int, sequence_id int, token_id int, layer_id int, one_k_vector vecf32(1024));

import binascii
import time

import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

table_name = "speedtest"
vec_len = 1024
num_inserts = 1024 * 8
num_vector_per_insert = 5


def to_db_hex(value, dim=None):
    if value is None:
        return value

    value = np.asarray(value, dtype='<f')

    if value.ndim != 1:
        raise ValueError('expected ndim to be 1')

    # return value.tobytes()
    return binascii.b2a_hex(value)

def correctness_test():
    engine = create_engine("mysql+pymysql://root:111@127.0.0.1:6001/a")

    # insert 2 new rows
    def get_max_id():
        with engine.connect() as con:
            rs = con.execute(text('SELECT MAX(id) FROM speedtest'))
            max_id = next(rs)[0]
        return max_id

    new_id = get_max_id() + 1

    Session = sessionmaker(bind=engine)
    session = Session()
    data = [np.random.rand(vec_len), np.random.rand(vec_len)]
    sql_insert = text("insert into speedtest (id, one_k_vector) values(:id, decode(:data,'hex') );")
    for i, arr in enumerate(data):
        print("XXX", new_id + i)
        session.execute(sql_insert, {"id": new_id + i, "data": to_db_hex(arr)})
    session.commit()

    new_max_id = get_max_id()
    assert new_max_id == new_id + 1

    # select using string output
    with engine.connect() as con:
        rs = con.execute(text(f'SELECT * FROM speedtest WHERE id >= {new_id}'))
        for row in rs:
            print("XXXXXX", row)
        import ipdb; ipdb.set_trace()

    # select using hex output
    with engine.connect() as con:
        rs = con.execute(text(f'SELECT id, encode(one_k_vector, "hex") FROM speedtest WHERE id >= {new_id}'))
        for row in rs:
            print("XXXXXX", row)
        import ipdb; ipdb.set_trace()



def insert():
    engine = create_engine("mysql+pymysql://root:111@127.0.0.1:6001/a")
    Session = sessionmaker(bind=engine)
    session = Session()

    sql_insert = text("insert into speedtest (id, one_k_vector) values(:id, decode(:data,'hex') );")
    for i in range(num_inserts * num_vector_per_insert):
        arr = np.random.rand(vec_len)
        # print(arr)
        session.execute(sql_insert, {"id": i, "data": to_db_hex(arr)})
    session.commit()


correctness_test()

start = time.time()
insert()
duration = time.time() - start
print(f"Result: vector dim={vec_len} vectors "
      f"inserted={num_inserts * num_vector_per_insert} "
      f"insert/second={num_inserts * num_vector_per_insert / duration}")
