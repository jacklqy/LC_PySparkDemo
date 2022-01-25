# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 初始化环境，构建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    # 读取小文件文件夹
    rdd = sc.wholeTextFiles("../data/tiny_files")

    # print(rdd.map(lambda x: x[1]).collect())
    print(rdd.collect())
