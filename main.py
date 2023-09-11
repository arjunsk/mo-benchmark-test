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
num_sql_stmt = 1024 * 8
num_vector_per_sql_stmt = 5


def to_db_hex(value, dim=None):
    if value is None:
        return value

    value = np.asarray(value, dtype='<f')

    if value.ndim != 1:
        raise ValueError('expected ndim to be 1')

    # return value.tobytes()
    return binascii.b2a_hex(value)


def from_db_hex(hex_str):
    buf = binascii.a2b_hex(hex_str)
    return np.frombuffer(buf, dtype='<f')


def get_max_id(engine):
    with engine.connect() as con:
        rs = con.execute(text('SELECT MAX(id) FROM speedtest'))
        max_id = next(rs)[0]
    return max_id


def correctness_test():
    engine = create_engine("mysql+pymysql://root:111@127.0.0.1:6001/a")

    # insert 2 new rows
    new_id = get_max_id(engine) + 1

    Session = sessionmaker(bind=engine)
    session = Session()
    data = [np.random.rand(vec_len), np.random.rand(vec_len)]
    sql_insert = text("insert into speedtest (id, one_k_vector) values(:id, decode(:data,'hex') );")
    for i, arr in enumerate(data):
        session.execute(sql_insert, {"id": new_id + i, "data": to_db_hex(arr)})
    session.commit()

    new_max_id = get_max_id(engine)
    assert new_max_id == new_id + 1

    # select using string output
    with engine.connect() as con:
        rs = con.execute(text(f'SELECT * FROM speedtest WHERE id >= {new_id} order by id'))
        data_read = []
        for row in rs:
            s = row[1].lstrip().lstrip("[").rstrip().rstrip("]")
            data_read.append(np.fromstring(s, sep=","))
        assert np.allclose(data, data_read)

    # select using hex output
    with engine.connect() as con:
        rs = con.execute(text(f'SELECT id, encode(one_k_vector, "hex") FROM speedtest WHERE id >= {new_id} order by id'))
        data_read = []
        for row in rs:
            data_read.append(from_db_hex(row[1]))
        assert np.allclose(data, data_read)


def insert():
    engine = create_engine("mysql+pymysql://root:111@127.0.0.1:6001/a")
    Session = sessionmaker(bind=engine)
    session = Session()

    start_id = get_max_id(engine) + 1

    sql_insert = text("insert into speedtest (id, one_k_vector) values(:id, decode(:data,'hex') );")
    for i in range(num_sql_stmt * num_vector_per_sql_stmt):
        arr = np.random.rand(vec_len)
        # print(arr)
        session.execute(sql_insert, {"id": start_id + i, "data": to_db_hex(arr)})
    session.commit()

def select_string():
    pass


def select_hex():
    pass

correctness_test()

for func in [insert, select_string, select_string]:
    start = time.time()
    func()
    duration = time.time() - start
    print(f"{func.__name__} Result: vector dim={vec_len} vectors "
          f"rows={num_sql_stmt * num_vector_per_sql_stmt} "
          f"rows/second={num_sql_stmt * num_vector_per_sql_stmt / duration}")
