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
    locals().update({f'a{i}': Column(String(10)) for i in range(1, 100)})
    a100 = Column(String(10), primary_key=True)


def generate_random_string(length=10):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


def run_queries(n=100):
    total_time = 0
    for _ in range(n):

        conditions = []
        for i in range(1, 10):
            attr = getattr(Tbl, f'a{i}', None)
            if attr is not None:
                start_range, end_range = sorted([generate_random_string(), generate_random_string()])
                conditions.append(attr.between(start_range, end_range))

        start_time = time.time()
        query = select(Tbl).where(*conditions)
        # print(str(query.compile(compile_kwargs={"literal_binds": True})))
        results = session.execute(query).all()
        total_time += time.time() - start_time

        print(f"Query 1 returned {len(results)} results.")

    qps = n / total_time
    return qps


qps = run_queries()
print(f"QPS: {qps}")
