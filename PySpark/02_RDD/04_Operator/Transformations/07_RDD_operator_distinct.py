# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 初始化环境，构建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 1, 3, 3, 5, 6, 8])
    print(rdd.distinct().collect())

    rdd2 = sc.parallelize([('a', 1), ('a', 1), ('a', 3)])
    print(rdd2.distinct().collect())
