# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 初始化环境，构建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 1), ('b', 1), ('a', 1)])
    print(rdd.map(lambda x: (x[0], x[1] * 10)).collect())
    print(rdd.mapValues(lambda values: values * 10).collect())
