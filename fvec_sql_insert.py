# create table t3(a int, b vecf32(128));
import binascii
import time

import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def to_db_binary(value):
    if value is None:
        return value

    value = np.asarray(value, dtype='<f4')
    if value.ndim != 1:
        raise ValueError('expected ndim to be 1')

    return binascii.b2a_hex(value.tobytes()).decode('utf-8')


def fvecs_read(filename, c_contiguous=True):
    fv = np.fromfile(filename, dtype=np.float32)
    if fv.size == 0:
        return np.zeros((0, 0))
    dim = fv.view(np.int32)[0]
    assert dim > 0
    fv = fv.reshape(-1, 1 + dim)
    fv = fv[:, 1:]
    if c_contiguous:
        fv = np.ascontiguousarray(fv)
    return fv


def run():
    engine = create_engine("mysql+mysqldb://root:111@127.0.0.1:6001/a")
    Session = sessionmaker(bind=engine)
    session = Session()

    sql_insert = text("INSERT INTO t3 (a, b) VALUES (:id, decode(:data,'hex'));")

    vecList = fvecs_read("/Users/arjunsunilkumar/Downloads/benchmark/1million128/sift/sift_base.fvecs")

    for i in range(0, len(vecList)):
        session.execute(sql_insert, {"id": i, "data": to_db_binary(vecList[i])})
        session.commit()


def main():
    start = time.time()
    run()
    duration = time.time() - start
    print(f"duration={duration}")


if __name__ == "__main__":
    main()
