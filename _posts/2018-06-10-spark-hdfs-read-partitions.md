---
layout:     post
title:      Spark读取HDFS时RDD的分区数量问题
date:       2018-06-10
summary:    详细介绍了Spark从HDFS读取数据生成RDD之后，RDD的Parition个数是如何决定的
categories: spark
published:  true
---



Spark从HDFS读取文件生成的RDD含有几个partition呢？这是个经常在面试中会被问的问题，当然有时候你也会在应该开发过程当中提出类似的疑惑。那么到底会生成几个partition呢，这里要按使用的API分两种情况说明，分别是SparkContext的读取API和SparkSession的DataFramReader读取API，而这两种API分别代表2.0之前和2.0之后的常用API。


### SparkContext里面的HDFS读取API
SparkContext里面跟HDFS数据读取相关的API在生成partition方面相对是比较简单，这里的简单指spark本身没做任何处理，而完全依赖于Hadoop Mapreduce的API，具体是使用InputFormat里面的getSplits方法获取InputSplits生成Partitions，有几个InputSplits最后就有几个partitions。关于InputFormat#getSplits的实现,这个跟具体的文件格式有关，比如文本格式，具体实现在FileInputFormat#getSplits里面。

SparkContext里面的API可以进一步细分为新的和旧的API，相关的底层RDD分别是HadoopRDD和NewHadoopRDD，这里新的旧的分别对应于MapReduce里面的新旧API,关于旧的和新的API的区别可以参考 [Difference  between Hadoop OLD API and NEW API](http://hadoopbeforestarting.blogspot.com/2012/12/difference-between-hadoop-old-api-and.html)。下面分别就HadoopRDD和NewHadoopRDD进行简单说明。


#### HadoopRDD

parkContext里有相关的API，比较典型的有:
```scala
def textFile(path: String, minPartitions: Int = defaultMinPartitions): RDD[String]
```
一般来说在这些API都提供一个参数minPartitions，如果调用端不提供，那么就取min(spark.default.parallelism,2)作为默认值，然后spark在HadoopRDD里面直接调用Hadoop里面org.apache.hadoop.mapred.InputFormat的getSplits方法来获取InputSplit。

这里举个文本格式的例子，文本格式的getSplits具体实现在org.apache.hadoop.mapred.FileInputFormat里面，这里面比较关键的一个变量是inputSplit,具体由以下方法获得：

```
splitSize = max(minsize, min(totalFileSize / minPartitions, blockSize))
```

这里的minsize由参数mapreduce.input.fileinputformat.split.minsize(mapred-site.xml),默认为1,totalFileSize是这次读取文件大小的总和，minPartitions则是由调用者传入，blockSize是文件块大小。

剩下的就比较简单了，遍历所有的文件按这个splitSize分成一个一个InputSplits。


所以在用HadoopRDD相关的API的时候，生成的分区个数大致可以归结为：
- 如果所有参数按默认来, 那么一个HDFS文件至少是一个partition, 一个文件如果包含多个block,则相应的会生成多个partition.
-  保持其他参数不变，可以通过提高mapreduce.input.fileinputformat.split.minsize来增大splitSize,从而达到减少partition个数的目的(这里前提是至少有文件包含多个blocks）
- 保持其他参数不变，增大minParitions来增加partition的个数


#### NewHadoopRDD

NewHadoopRDD相关的API跟HadoopRDD的类似，也是在SparkContext里面，比如
```scala
def newAPIHadoopFile[K, V, F <: InputFormat[K, V]](path: String)(implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)]
```

NewHadoopRDD也是直接调用InputFormat#getSplits来生成partition的，只是在MapReduce新的API里面没有了minPartitions参数，并且这个InputFormat位于org.apache.hadoop.mapreduce下面。当然getSplits的具体实现也是要看具体的文件格式，比如相应的文本文件的格式实现是在org.apache.hadoop.mapreduce.lib.input.FileInputFormat，这里的getSplitsHadoopRDD里面的类似，只是在生成splitSize上面有所区别，splitSiz由以下方式生成：


```
inputSplit = max(minsize, min(maxsize, blocksize))
```
这里的maxsize是mapreduce的配置项 mapreduce.input.fileinputformat.split.maxsize, 默认最大9223372036854775807L(长整型的最大值)；minsize是指mapreduce.input.fileinputformat.split.minsize, 默认1;blocksize是文件块大小。
总的来说NewHadoopRDD在生成partition个数方除了用mapreduce.input.fileinputformat.split.maxsize配置来替换minPartition来提升partition的数量外，其他基本一样。




### 2.0之后SparkSession里面的HDFS读取API

Spa2.0使用SparkSession#rea下面的API去读HDFS生成的partition个数所采用的方式则发生了比较大的变化，spark抛弃了原来的getSplits方法来生成partitions，自己采用多个参数来控制生成partitions。主要的逻辑在FileSourceScanExec里面大致可以分为下面几步：


1. 首计算maxSplitBytes，计算方法如下：
    ```
    maxSplitBytes = min(defaultMaxSplitBytes, max(openCostInBytes, totalBytes/defaultParallelism))
    ```

    这defaultMaxSplitByte指spark.sql.files.maxPartitionBytes，默认128M；openCostInBytes取自配置spark.sql.files.openCostInBytes， 默认4MtotalBytes是把本次所读取求和再加上对每个文件额外增加一次openCostInBytes;defaultParallelism则是spark.default.parallelism

2. 遍历所有文件，按maxSplitByte切成一个个PartitionedFile这个跟FileInputFormat的getSplits有点类似，一个文件至少一个PartitionedFile，一个文件的大小如果maxSplitBytes的好几倍，那么也会分成几个Partitioned
3. 把生成的所有ParitionedFil大小倒序排然逐个装到FilePartition里面（FilePartition所设置的大小上限是maxSplitBytes, 这其实是个装桶问题，采用了Next Fit Decreasing，一个比较简单的算法，详情可参考[这里](http://personal.morris.umn.edu/~mcquarrb/teachingarchive/M1001/Resources/BinPacking.html)，我想这里之所以使用Next Fit Decreasing主要第一简单，装过的桶不需要保持开着的状态，第二相对来说分布的会比较均匀）。这一步在原来的MapReduce API里面是没有的，主要的作用是可以合单个小文件，使整体Partition个数比较可控。


依据以上流程，我们可有下面这些结论：

- 一般情况下，如参数都按默认的来，我们可以看spark.read所产生RDD的partition个数跟spark.default.parallelis的值非常接近，会比这个值大一些，但都非常接近。
-如果有大量小文件spark.read会有很多合并的操作，这要归功于流程里最后装桶那一步所以整体产生的partition数量会比MapReduce API里面的少很多。
-如果想减少partition的数量则可以提高maxParitionBytes这个参数，这个参数可在运行时动态设置的
    ```scala
    spark.sql("set spark.sql.files.maxPartitionBytes = 536870912")
    ```





### 数据的分布与RDD Partition个数的关系
数据从HDF读出变成RDD之后，它的Partition个数和数据的均匀分布这个并不是一定的关系还是决定于文件格式比如一个500M的文件夹里面的数据按10Partition读取，那么每个Partition数据一定是50M左右吗？如果是文本文件，那么可能差不多是这样，但如果是parquet格式的文件，则另当别论了，比如这文件夹里面只有5parquet文件，那么这10个partition里面只有5是有数据的，另外5个是空的。


