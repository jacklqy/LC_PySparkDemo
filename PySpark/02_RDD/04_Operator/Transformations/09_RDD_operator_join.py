# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 初始化环境，构建SparkContext对象
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([(1001, "张三"), (1002, "李四"), (1003, "王五")])
    rdd2 = sc.parallelize([(1001, "销售部"), (1002, "总经办")])

    print(rdd.join(rdd2).collect())

    # 左外连接
    print(rdd.leftOuterJoin(rdd2).collect())
    # 右外连接
    print(rdd.rightOuterJoin(rdd2).collect())
