# CREATE DATABASE a;
# USE a;
# CREATE TABLE speedtest (id int, one_k_vector vecf32(1024));
# CREATE TABLE speedtest (id int, sequence_id int, token_id int, layer_id int, one_k_vector vecf32(1024));
# CREATE TABLE tbl(id int, embedding vecf32(2));
import binascii
import json
import time

import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# if you face pymysql issue, just call `from pymysql import *` and manually sync

table_name = "speedtest"
vec_len = 1024
num_inserts = 1024 * 8
num_vector_per_insert = 5


def to_db_binary(value, dim=None):
    if value is None:
        return value

    value = np.asarray(value, dtype='<f')  # for vecf32
    # value = np.asarray(value, dtype='<f8') # for vecf64

    if value.ndim != 1:
        raise ValueError('expected ndim to be 1')

    return binascii.b2a_hex(value)


def generate_random_array():
    # arr0 = np.random.uniform(73.29566438951028, 77.999999999)
    # arr1 = np.random.uniform(18.0, 23.9999999999)

    # arr0 = np.random.uniform(122.29566438951028, 127.999999999)
    # arr1 = np.random.uniform(7.0, 12.9999999999)

    arr0 = np.random.uniform(103.29566438951028, 108.999999999)
    arr1 = np.random.uniform(18.0, 23.9999999999)

    return np.array([arr0, arr1])


def print_json(arr):
    arr_list = arr.tolist()
    data = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": arr_list
        },
        "properties": {}
    }

    json_text = json.dumps(data, separators=(',', ':'))
    print(json_text + ",")


def run():
    engine = create_engine("mysql+pymysql://root:111@127.0.0.1:6001/a")
    Session = sessionmaker(bind=engine)
    session = Session()

    sql_insert = text("insert into tbl (id, embedding) values(:id, decode(:data,'hex') );")
    for i in range(num_inserts * num_vector_per_insert):
        arr = generate_random_array()
        print_json(arr)
        session.execute(sql_insert, {"id": i, "data": to_db_binary(arr)})
    session.commit()


start = time.time()
run()
duration = time.time() - start
print(f"Result: vector dim={vec_len} vectors "
      f"inserted={num_inserts * num_vector_per_insert} "
      f"insert/second={num_inserts * num_vector_per_insert / duration}")
