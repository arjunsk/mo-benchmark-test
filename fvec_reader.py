import numpy as np


def read_fvecs(file_name):
    with open(file_name, 'rb') as f:
        while True:
            d = np.fromfile(f, dtype=np.int32, count=1)
            if not d:
                break
            d = int(d[0])
            vec = np.fromfile(f, dtype=np.float32, count=d)
            yield vec


for vector in read_fvecs("/Users/arjunsunilkumar/Downloads/benchmark/1million/gist/gist_query.fvecs"):
    print(vector)
    break
