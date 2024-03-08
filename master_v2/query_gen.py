import csv

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


def generate_random_string(a, b):
    random_ascii = random.randint(ord(a), ord(b))
    return chr(random_ascii)


def run_queries(n=10):
    total_time = 0
    with open('query_results.csv', mode='w', newline='') as result_file, open('query_strings.csv', mode='w', newline='') as query_file:
        result_writer = csv.writer(result_file)
        query_writer = csv.writer(query_file)
        for _ in range(n):
            conditions = []
            for i in range(1, 100):
                attr = getattr(Tbl, f'a{i}', None)
                if attr is not None:
                    start_range, end_range = sorted([generate_random_string('0', '3'), generate_random_string('y', '{')])
                    conditions.append(attr.between(start_range, end_range))

            start_time = time.time()
            query = select(Tbl.a100).where(*conditions)
            results = session.execute(query).all()
            total_time += time.time() - start_time

            if len(results) > 0:
                query_str = str(query.compile(compile_kwargs={"literal_binds": True})).replace('\n', ' ')
                query_writer.writerow([query_str])

                flattened_result = [item for sublist in results for item in sublist]
                result_writer.writerow(flattened_result)

                # print(query_str)
                print(f"Found {len(results)} rows.")

        qps = n / total_time
        return qps


qps = run_queries()
print(f"QPS: {qps}")
