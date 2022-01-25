# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 初始化环境，构建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    # 通过textFil API读取数据
    file_rdd1 = sc.textFile("../data/input/words.txt")
    print("默认读取分区数：", file_rdd1.getNumPartitions())
    print("file_rdd1 内容：", file_rdd1.collect())

    # 加最新分区数测算
    file_rdd2 = sc.textFile("../data/input/words.txt", 3)
    # 最小分区数是参考值，Spark有自己的算法，你给的太大Spark不会理会
    file_rdd3 = sc.textFile("../data/input/words.txt", 100)
    print("file_rdd2分区数：", file_rdd2.getNumPartitions())
    print("file_rdd3分区数：", file_rdd3.getNumPartitions())

    # 读取HDFS文件
    # hdfs_rdd = sc.textFile("hdfs:")
