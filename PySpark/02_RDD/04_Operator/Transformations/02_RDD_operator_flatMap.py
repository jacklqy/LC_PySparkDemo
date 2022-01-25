# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 初始化环境，构建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(["hadoop flink pyspark", "spark pyspark hadoop flink", "hadoop flink spark"])
    # rdd2 = rdd.map(lambda line: line.split(" "))
    # 解除嵌套
    rdd2 = rdd.flatMap(lambda line: line.split(" "))
    print(rdd2.collect())
