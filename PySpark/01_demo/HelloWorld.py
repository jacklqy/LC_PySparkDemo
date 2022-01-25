# coding:utf8
from pyspark import SparkConf, SparkContext
# import os
# 环境变量配置了，这里就可以不用指定了。 os.environ['PYSPARK_PYTHON'] = 'D:\\anaconda3\\envs\\pyspark\\python.exe'

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("WordCountHelloWorld")
    sc = SparkContext(conf=conf)

    # 需求：wordcount单词计数，读取HDFS上的words.txe文件，统计单词的数量
    # 读取文件 此处读取的是本地文件
    file_rdd = sc.textFile("../data/input/words.txt")

    # 将单词进行切割，得到一个存储全部单词的集合对象
    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))

    # 将单词转换为元祖对象，key是单词，value是数字1
    words_with_one_rdd = words_rdd.map(lambda x: (x, 1))

    # 将元祖的value按照key分组，对所有的value执行聚合操作(相加)
    result_rdd = words_with_one_rdd.reduceByKey(lambda a, b: a + b)

    # 通过collect方法收集RDD的数据打印输出结果
    print(result_rdd.collect())
