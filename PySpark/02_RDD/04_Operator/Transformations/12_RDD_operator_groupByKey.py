# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 初始化环境，构建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 1), ('b', 1), ('a', 1)])

    result = rdd.groupByKey()
    print(result.collect())
    print(result.map(lambda t: (t[0], list(t[1]))).collect())