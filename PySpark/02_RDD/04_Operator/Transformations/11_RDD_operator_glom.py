# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 初始化环境，构建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 3)
    print(rdd.glom().collect())

    # 解嵌套
    print(rdd.glom().flatMap(lambda x: x).collect())
