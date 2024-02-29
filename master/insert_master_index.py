"""
CREATE TABLE tbl (a varchar(10), b varchar(10), c varchar(10), d varchar(10), e varchar(10) primary key);
"""
from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import random
import string

DATABASE_URI = "mysql+pymysql://root:111@127.0.0.1:6001/a"
engine = create_engine(DATABASE_URI, echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Tbl(Base):
    __tablename__ = 'tbl'
    a = Column(String(10))
    b = Column(String(10))
    c = Column(String(10))
    d = Column(String(10))
    e = Column(String(10), primary_key=True)

# If the table doesn't exist, create it
Base.metadata.create_all(engine)

session = Session()

def generate_random_string(length=10):
    """Generate a random string of fixed length."""
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

def insert_random_rows(n=1000000, batch_size=10000):
    """Insert n random rows into the table using session commit with batches."""
    for batch_start in range(0, n, batch_size):
        for _ in range(batch_size):
            data = Tbl(
                a=generate_random_string(),
                b=generate_random_string(),
                c=generate_random_string(),
                d=generate_random_string(),
                e=generate_random_string()
            )
            session.add(data)
        session.commit()
        print(f"Inserted {batch_start + batch_size} rows into 'tbl'.")

# Insert 1 million random rows
insert_random_rows()
