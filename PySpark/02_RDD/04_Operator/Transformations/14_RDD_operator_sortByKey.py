# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 初始化环境，构建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 1), ('b', 2), ('a', 1), ('a', 3)])
    print(rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key: str(key).lower()).collect())
