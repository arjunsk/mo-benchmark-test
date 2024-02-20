import struct


def read_fvecs(file_path):
    vectors = []
    with open(file_path, 'rb') as f:
        while True:
            # Read the dimensionality of the vector (an integer)
            dim_bytes = f.read(4)
            if not dim_bytes:
                break
            dim = struct.unpack('i', dim_bytes)[0]

            # Read the vector
            vector = struct.unpack('f' * dim, f.read(4 * dim))
            vectors.append(vector)
    return vectors


# Usage
file_path = '/Users/arjunsunilkumar/Downloads/benchmark/1million/gist/gist_query.fvecs'
vectors = read_fvecs(file_path)
print(vectors[0])
print(len(vectors))