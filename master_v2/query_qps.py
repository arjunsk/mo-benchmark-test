import csv

from sqlalchemy import create_engine, select, Column, String, text
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


def run_queries():
    query_strings_file = '/Users/arjunsunilkumar/PycharmProjects/mo-benchmark-test/master_v2/query_strings.csv'
    query_results_file = '/Users/arjunsunilkumar/PycharmProjects/mo-benchmark-test/master_v2/query_results.csv'

    currSession = Session()
    total_time = 0
    n = 0
    with open(query_strings_file, 'r', newline='') as q_file, open(query_results_file, 'r', newline='') as r_file:
        query_reader = csv.reader(q_file)
        result_reader = csv.reader(r_file)

        for query_line, result_line in zip(query_reader, result_reader):
            query_str = query_line[0]
            expected_result = result_line

            start_time = time.time()
            result = currSession.execute(text(query_str)).fetchall()
            total_time += time.time() - start_time
            n += 1

            flattened_result = [str(item) for sublist in result for item in sublist]
            if sorted(flattened_result) != sorted(expected_result):
                print("Result does not match the expected output. len(result):", len(result), "len(expected_result):",
                      len(expected_result))

            if n % 100 == 0:
                print(f"Processed {n} queries.")

        qps = n / total_time
        return qps


qps = run_queries()
print(f"QPS: {qps}")
