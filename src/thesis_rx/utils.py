from pyspark.sql import functions as F
from pyspark.sql import types as T


@F.udf(T.DoubleType())
def jaccard_distance(a, b):
    a = set([] if a is None else [x for x in a if x is not None])
    b = set([] if b is None else [x for x in b if x is not None])
    u = a.union(b)
    if len(u) == 0:
        return 0.0
    return float(1.0 - (len(a.intersection(b)) / len(u)))
