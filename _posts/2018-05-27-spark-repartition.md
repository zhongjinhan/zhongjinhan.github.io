---
layout:     post
title:      深入Spark Repartition API
date:       2018-05-27
summary:    介绍Spark repartition API的使用与实现
categories: spark
published:  true
---





在Spark的Scala版本的[Dataset API文档](http://spark.apache.org/docs/2.3.0/api/scala/index.html#org.apache.spark.sql.Dataset)中，
我们可以看到下列跟repartition有关的API


```scala
def repartition(numPartitions: Int): Dataset[T]
def repartition(partitionExprs: Column*): Dataset[T]
def repartition(numPartitions: Int, partitionExprs: Column*): Dataset[T]
def repartitionByRange(partitionExprs: Column*): Dataset[T]
def repartitionByRange(numPartitions: Int, partitionExprs: Column*): Dataset[T]
```

这些Dataset的repartition操作是不会修改数据本身的，而是触发shuffle，让数据在按给定的条件下重新分布到各个partition。
而数据分布方式的不同则是因为采用了不同的Partitioning方式，



### Repartition By RoundRobin
首先来看RoundRobinParititiong, 由下面这个API触发

```scala
def repartition(numPartitions: Int): Dataset[T]
```
它把所有的数据以轮循的方式放到新的partition里面，所以最后数据是平均分配到各个partition里面，每个partition的大小都差不多。有些时候在一个数据处理链中，上游处理完之后的数据分布是极度不均匀的，这使得后续数据处理会变得比较麻烦，很可能几个长时间运行的任务会拖累整个作业运行，这时候加上这样一个repartition操作之后，你会发现作业运行时效会有一个质的飞跃。


主要实现在[ShuffleExchangeExec](https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L203)里面接。大致流程如下：
1. 针对每个现有partition，调用 getPartitionKeyExtractor() 方法生产一个值函数
2. 上面产生的这个函数其输入参数为InternalRow，返回值为Any，里面的变量position的值在getPartitionKeyExtractor被调用的时候，由现有partitionId作为种子，新的partition个数作为范围随机生成，作为一个初始值。（其实也不算随机了，假设现有partitionId=0， 新的partition个数设置为100，那么初始position = new Random(0).nextInt(100)， 这个结果是恒定的，等于60）
3. 对于现有一个partition里面，把上面的初始值position加上1作为第一行数据的key，后续每行的key基于第一行的逐行递增
4.  把3步里面产生的key模上新partition个数，即是这一行的新partition key.
5.  从1-4里面可以看出，对于现有partition里面的数据，每行数据以轮询的方式逐条放到新的partition里面，所以数据的分布是非常均匀的
6.  另外每可以看出行数据新的partition key主要由两个因素决定：初始position值和这一行数据在整个现有partition里面的位置，即行号；前者刚才看到了，是固定的，如果想让后者也固定，最简单的办法就是对partition内部进行排序，spark目前也是这么做的，在这之前确实做了排序，只有这样才能保证在失败任务在重跑后所产生的数据是恒定的


getPartitionKeyExtractor 代码段

```scala
    def getPartitionKeyExtractor(): InternalRow => Any = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) =>
        // Distributes elements evenly across output partitions, starting from a random partition.
        var position = new Random(TaskContext.get().partitionId()).nextInt(numPartitions)
        (row: InternalRow) => {
          // The HashPartitioner will handle the `mod` by the number of partitions
          position += 1
          position
        }
      ...
    }
```




### Repartition with Hash

HashPartitioning由以下两个API触发，这其实是同一个API，只不过没有numPartitions的那个使用配置项spark.sql.shuffle.partitions的值。


HashPartitipning的主要使用场景是你想要把同一组数据放到同一个partition里面，以便后续的sortWithinPartition和mapPartition等算子的运算。

HashPartitioning实现方面相对来说要简单很多
1. 先对partition columns进行哈希求值，这里采用的是Murmur3算法
2. 然后用哈希值对新的分区个数求余，如果是余数是负数，用余数加上分区个数然后再求余，结果就是该条数据新分区的id


求哈希和求余在Spark SQL里面都有相应的函数所以假设表t里面的c1和c列作为repartition列，目标partition个数设定为new_part_num
Irtition id

```sql
select t.*, pmod(hash(c1,c2),new_part_num) as new_part_num from t
```


### Repartition with Range
用RangePartitioning的那当然就是下面两个API了，跟HashPartitioning一样，这两个其实就是一个, 没有numPartitions的那个使用配置项spark.sql.shuffle.partitions的值

```scala
def repartitionByRange(partitionExprs: Column*): Dataset[T]
def repartitionByRange(numPartitions: Int, partitionExprs: Column*): Dataset[T]
```

在实际应用方面RangePartitioning的选择优先级一般到比上面两个在用hash之如果发现数据倾斜的比较严重，那么可以尝试下range，还是跟数据本身的特性相关。

RangePartitioning在概念上跟数据库里面的range partition类似，只是数据库里面我们都会预设好每个分区具体的范围或者事先确定好根据数据动态生成范围值逻辑。而Spark是使用样本数据去估算数据的范围值, 具体实现在[RangeParition](https://github.com/apache/spark/blob/v2.3.0/core/src/main/scala/org/apache/spark/Partitioner.scala#L137) 里面下面介绍下大致流程：


1. 对现有的RD的每个partition采样，样本数量由配置项spark.sql.execution.rangeExchange.sampleSizePerPartition.rtition个数共同决定。采样方法则采蓄水池抽样算法大致流程是：从拥有n个元素的总体中抽取k个样，先从总体中拷贝前k个元素，然后遍历剩余的元素，剩余的第l个元素，按k/l的概率选择该元素作为样本。因为采样涉及对rdd的操作并需要取回样本数据到driver端，所以这里会触发一个action，而你在Spark Web UI就会额外看到一个job的运行记录.

2. 对初抽取的样本进行检查，如果有些partition的总体数量特别大的，那么重新对这些partition进行抽样以获取更多的样本代表这个时候就是按比较去抽样，具体调用的是RDD#sample这个方法然后对所有样本数据算权重作Range分区的范围值的候选行。

3. 对候选行进行排序按权重算出n-1个partition的范围值







### Summary
总的来说repartition主要用来使数据重新分配以达你想要的状态。






