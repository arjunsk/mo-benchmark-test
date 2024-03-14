"""
create database a;
use a;
create table t1( layer_id bigint, token_pos bigint, token_str varchar(30), act_vector vecf32(4000), exp_ts timestamp);
create table t2( layer_id bigint, token_pos bigint, token_str varchar(30), act_vector vecf32(4000), exp_ts timestamp);
create table activations( layer_id bigint, token_pos bigint, token_str varchar(30), act_vector vecf32(4000), exp_ts timestamp);
"""
import binascii
import time

import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

table_name = "activations"  # replace with t1, t2 and activations.
vec_len = 4000
num_inserts = 10_000


def to_db_binary(value, dim=None):
    if value is None:
        return value

    value = np.asarray(value, dtype='<f')

    if value.ndim != 1:
        raise ValueError('expected ndim to be 1')

    # return value.tobytes()
    return binascii.b2a_hex(value)


def run():
    engine = create_engine("mysql+pymysql://root:111@127.0.0.1:6001/a")
    Session = sessionmaker(bind=engine)
    session = Session()

    sql_insert = text("insert into " + table_name + "(layer_id, token_pos, token_str, act_vector, exp_ts) "
                                                    "values(1,1, 'test', decode(:data,'hex') , now() );")
    for i in range(num_inserts):
        arr = np.random.rand(vec_len)
        arr = np.asarray(arr, dtype='<f')
        session.execute(sql_insert, {"data": to_db_binary(arr)})
        if i % 1000 == 0:
            print(f"inserted {i}")
    session.commit()


start = time.time()
run()
duration = time.time() - start
print(f"Result: vector dim={vec_len} vectors "
      f"inserted={num_inserts} "
      f"duration={duration}")
