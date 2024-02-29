"""
select * from tbl where
a between '8MmmdJmpWz' and 'xHy0cJd7Me' and
b between 'Vrj8rrlQAL' and 'tVBS0kcxGA' and
c between 'hwUtVT7mUI' and 'iIrOCXQ1Wh' and
d between 'HqRnGoR1cT' and 'UBEPUiptKD';

create index idx using master on tbl(a,b,c,d);

drop index idx on tbl;
"""
from sqlalchemy import create_engine, select, Column, String
from sqlalchemy.orm import sessionmaker, declarative_base  # Updated import here
import random
import string
import time

DATABASE_URI = "mysql+pymysql://root:111@127.0.0.1:6001/a"
engine = create_engine(DATABASE_URI, echo=False)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()


class Tbl(Base):
    __tablename__ = 'tbl'
    a = Column(String(10), primary_key=True)
    b = Column(String(10))
    c = Column(String(10))
    d = Column(String(10))
    e = Column(String(10))


def generate_random_string(length=10):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


def run_queries(n=100):
    total_time = 0
    for _ in range(n):
        a_start, a_end = sorted([generate_random_string(), generate_random_string()])
        b_start, b_end = sorted([generate_random_string(), generate_random_string()])
        c_start, c_end = sorted([generate_random_string(), generate_random_string()])
        d_start, d_end = sorted([generate_random_string(), generate_random_string()])

        query = select(Tbl).where(
            Tbl.a.between(a_start, a_end),
            Tbl.b.between(b_start, b_end),
            Tbl.c.between(c_start, c_end),
            Tbl.d.between(d_start, d_end)
        )

        start_time = time.time()
        results = session.execute(query).all()
        total_time += time.time() - start_time

        if _ == 0:
            print(f"Query 1 returned {len(results)} results.")

    qps = n / total_time
    return qps


qps = run_queries()
print(f"QPS: {qps}")
