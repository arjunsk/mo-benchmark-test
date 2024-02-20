from sqlalchemy import create_engine, text
import numpy as np
import struct


def read_fvecs_file(filename):
    with open(filename, 'rb') as f:
        vectors = []
        while True:
            bytes = f.read(4)
            if not bytes:
                break
            dim = struct.unpack('i', bytes)[0]
            vector = np.fromfile(f, dtype=np.float32, count=dim)
            vectors.append(vector)
    return vectors


def read_ivecs_file(filename):
    with open(filename, 'rb') as f:
        vectors = []
        while True:
            bytes = f.read(4)
            if not bytes:
                break
            dim = struct.unpack('i', bytes)[0]
            vector = np.fromfile(f, dtype=np.int32, count=dim)
            vectors.append(vector)
    return vectors


engine = create_engine("mysql+mysqldb://root:111@127.0.0.1:6001/a")


def execute_knn_query(set_query, select_query, engine):
    with engine.connect() as conn:
        conn.execute(text(set_query))
        result = conn.execute(text(select_query))
        return [id for id, in result.fetchall()]


def calculate_recall(expected, actual):
    set_expected = set(expected)
    set_actual = set(actual)
    intersection = set_expected.intersection(set_actual)
    if not set_expected:
        return 0.0
    recall = len(intersection) / len(set_expected)
    return recall


def build_knn_query_template_with_ivfflat(input_vector_val, options):
    org_tbl_name = options['OrgTblName']
    org_tbl_sk_name = options['OrgTblSkName']
    org_tbl_id_name = options['OrgTblIdName']
    k = options['K']
    probe_val = options['ProbeVal']

    # Convert input_vector_val to a suitable string representation for your SQL dialect
    input_vector_str = str(list(input_vector_val)).replace('[', '{').replace(']', '}')

    # Separate SET and SELECT queries
    set_query = f"SET @probe_limit={probe_val};"
    select_query = f"SELECT {org_tbl_id_name} FROM {org_tbl_name} ORDER BY l2_distance({org_tbl_sk_name},'{input_vector_str}') ASC LIMIT {k};"
    return set_query, select_query


if __name__ == "__main__":
    query_vectors = read_fvecs_file('/Users/arjunsunilkumar/Downloads/benchmark/1million128/sift/sift_base.fvecs')
    expected_results = read_ivecs_file(
        '/Users/arjunsunilkumar/Downloads/benchmark/1million128/sift/sift_groundtruth.ivecs')

    options = {
        "DbName": "a",
        "OrgTblName": "t3",
        "OrgTblIdName": "a",
        "OrgTblSkName": "b",
        "OrgTblVecIdxName": "idx3",
        "ProbeVal": 32,
        "K": 100,
    }

    total_recall = 0

    for i, vec in enumerate(query_vectors):
        set_query, select_query = build_knn_query_template_with_ivfflat(vec, options)
        actual_results = execute_knn_query(set_query, select_query, engine)

        recall = calculate_recall(expected_results[i], actual_results)
        total_recall += recall
        print(f"Query {i + 1}: Recall = {recall:.4f}")

    average_recall = total_recall / len(query_vectors) if query_vectors else 0
    print(f"Average Recall: {average_recall:.4f}")
