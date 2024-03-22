"""
CREATE EXTENSION vector;
CREATE TABLE t3 (a bigserial, b vector(128));
CREATE INDEX idx3 ON t3 USING ivfflat (b vector_l2_ops) WITH (lists = 500);
"""

import time
import numpy as np
from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.orm import sessionmaker, mapped_column, DeclarativeBase

from pgvector.sqlalchemy import Vector

table_name = "t3"
vec_len = 128


class Base(DeclarativeBase):
    pass


class Item(Base):
    __tablename__ = table_name
    a = Column(Integer(), primary_key=True)
    b = mapped_column(Vector(vec_len))


def run():
    # pgvector psycopg2 issue on PyCharm https://stackoverflow.com/a/72288416/1609570
    engine = create_engine("postgresql+psycopg2://postgres:111@127.0.0.1:5432")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    vecList = read_fvec("/Users/arjunsunilkumar/Downloads/benchmark/1million128/sift/sift_base.fvecs")
    for i in range(0, len(vecList)):
        item = Item(b=vecList[i])
        session.add(item)

    session.commit()


def read_fvec(filename, c_contiguous=True):
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


def main():
    start = time.time()
    run()
    duration = time.time() - start
    print(f"duration={duration}")


if __name__ == "__main__":
    main()
