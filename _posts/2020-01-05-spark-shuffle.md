---
layout:     post
title:      Spark Shuffle 实现详解
date:       2020-01-05
summary:    详细介绍了spark shuffle 实现细节
categories: spark
published:  true
---



Shuffle是Map Reduce算法里面一个核心过程，它把输入的处于分布式存储上的键值对数据中同一个键的数据分配到一个计算节点上面，以便后续的计算。Spark作为MapReduce框架的高级版当然也少不了这一个过程的处理,这里以wordcount这个最有名的demo来深入了解下spark整个shuffle流程。

<br/>
## WordCount的shuffle流程

Word Count演示代码如下，可以看到这里用10个元素的List生成一个KV RDD, 初始会有3个partition， 然后调用reduceByKey生成拥有2个partition的result RDD，最终collect然后println出来。

```scala

val sc = new SparkContext("local[1]","test")
val sampleData = List(
  ("a", 1),
  ("b", 2),
  ("c", 3),
  ("a", 2),
  ("b", 1),
  ("d", 4),
  ("b", 3),
  ("d", 2),
  ("d", 1),
  ("d", 1)
)
val input = sc.parallelize(sampleData, 3)
val result = input.reduceByKey((x,y) => x + y, 2)
result.collect().foreach(println)
sc.stop()
```

<br/>
在这个过程当中，shuffle部分的流程如图所示:
![]({{ "/images/spark-shuffle-demo.png" | absolute_url }})

可以看到通过DAGScheduler生成了3个ShufflMapTask和2个ResultTask，每个ShuffleMapTask会把输入数据按partition id排序后写入到本地文件，在这个例子中因为满足了map端combine,所以本地临时文件的数据已经做过初步聚合了。当所有ShuffleMapTask完成之后，两个ResultTask会启动，通过网络从临时文件里面获取对应的partition的数据，最后聚合产生结果



<br/>

## Spark Shuffle 主要相关类

<br/>
**ShuffleManager**

跟Spark其他概念非常类似，Shuffle代码的入口类是ShuffleManager, 目前的实现是SortShuffleManager，跟其他类似Manager一样，都是在SparkEnv#create的时候被初始化，这意味着不管是driver还是exedutor都包含一个shuffle manager

 ```scala
val shortShuffleMgrNames = Map(
  "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
  "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
val shuffleMgrClass =
  shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase(Locale.ROOT), shuffleMgrName)
val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)
```

ShuffleManager主要提供了下面3个方法
- registerShuffle: 返回ShuffleHandle
- getWriter: 返回ShuffleWrite
- getReader: 返回ShuffleReader


<br/>
**ShuffleHandle**

ShuffleManager#registerShuffle在ShuffleDependency里面被调用返回ShuffleHandle
```scala
val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
  shuffleId, _rdd.partitions.length, this)
```

根据不同条件会有三种ShuffleHandle
BypassMergeSortShuffleHandle
SerializedShuffleHandle
BaseShuffleHandle

ShuffleHandle本身并没什么卵用，主要是存放附带信息然后决定后面的ShuffleWriter



<br/>
**ShuffleWriter**

ShuffleWriter是调用ShuffleManager#getWriter生成的，这个方法是在ShuffleMapTask#runTask中被调用
```scala
try {
  val manager = SparkEnv.get.shuffleManager
  writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
  writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
  writer.stop(success = true).get
} catch {
```

ShuffleWriter的主要是目的是在map端把数据按partition写入到本地文件，供后续reduce任务读取，目前有三种实现，分别对应三种handle
- BypassMergeSortShuffleWriter
- UnsafeShuffleWriter
- SortShuffleWriter

<br/>
**ShuffleReader**

相应地ShuffleReader是调用ShuffleManager#getReader生成，这个方法主要是在ShuffledRDD#compute里面被调用
```
override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
  val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
  SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
    .read()
    .asInstanceOf[Iterator[(K, C)]]
}
```

目前Reader只有一种实现 BlockStoreShuffleReader


<br/>
## Shuffle Write流程
考虑到WordCount这个例子涉及到的是SortShuffleWriter， 所以这里会以SortShuffleWriter为例重点讲解shuffle write过程。

Shuffle write的入口在SortShuffleWriter#write这个方法里面，
```scala
sorter = if (dep.mapSideCombine) {
  new ExternalSorter[K, V, C](
    context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
} else {
  new ExternalSorter[K, V, V](
    context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
}
sorter.insertAll(records)
val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
val tmp = Utils.tempFileWith(output)
try {
  val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
  val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
  shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
  mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
} finally {
  if (tmp.exists() && !tmp.delete()) {
    logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
  }
}
```

从方法里面可以看到shfuflewrite流程大致分为两步骤：
1. 把数据写入Buffer，内存放不下的时候spill到文件
2. 把buffer跟spill里面的数据一同写入到最终Parition文件


![]({{ "/images/spark-shuffle-write.png" | absolute_url }})


<br/>
### Buffer数据结构

可以在ExternalSort#insertAll里面看到buffer的数据结构有两种，分别是PartitionedPairBuffer和PartitionedAppendOnlyMap。具体使用哪种结构要根据产生这个shuffle的算子是否支持map side combine，如果支持则使用PartitionedAppendOnlyMap，否则使用PartitionedPairBuffer。这里的word count例子里面的算子reduceByKey就是支持map side combine的。

```scala
def insertAll(records: Iterator[Product2[K, V]]): Unit = {
  // TODO: stop combining if we find that the reduction factor isn't high
  val shouldCombine = aggregator.isDefined

  if (shouldCombine) {
    // Combine values in-memory first using our AppendOnlyMap
    val mergeValue = aggregator.get.mergeValue
    val createCombiner = aggregator.get.createCombiner
    var kv: Product2[K, V] = null
    val update = (hadValue: Boolean, oldValue: C) => {
      if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
    }
    while (records.hasNext) {
      addElementsRead()
      kv = records.next()
      map.changeValue((getPartition(kv._1), kv._1), update)
      maybeSpillCollection(usingMap = true)
    }
  } else {
    // Stick values into our buffer
    while (records.hasNext) {
      addElementsRead()
      val kv = records.next()
      buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
      maybeSpillCollection(usingMap = false)
    }
  }
}
```



<br/>
**PartitionedAppendOnlyMap**

因为map side combine需要在map端对相同的key进行预聚合，所以需要一个Map来对key进行快速定位更新，Spark自己实现了一个AppendOnlyMap，这个数据结构跟java HashMap比较类似，但是有如下几点区别：

1. 底层用的数组来存储数据，但因为输入的KV数据，所以存储的时候也是把KV分开存储，K放前V放后，所以数据实际长度是这个Map存储能力乘以2.
2. 为了让拥有相同低位值的hashcode能够更好地得到分布，对hashCode再加了一层hash，用murmuerhash实现。
3. 没有用到HashMap里面的bucket，如果有hash值冲突的情况，直接往后面推，直到找到没有用过的数组元素，这里之所以能这么实现，主要是因为没有删除操作。这样设计的好处是当需要对这个结构里面的数据进行排序的时候，可以不需要消耗额外的空间。




![]({{ "/images/spark-shuffle-buffer-map.png" | absolute_url }})

在word count这个例子里面因为reduceByKey这个算子有预聚合的条件，所以使用的是PartitionedAppendOnlyMap 来作为map端的buffer。


<br/>
**PartitionedPairBuffer**

对于不需要map side combie的操作，数据结构相对来说就简单一些，直接append数据到一个数组上面，这样做显然性能会更好。



<br/>

### Spill流程

当buffer装载到一定数据量之后，就会触发spill流程，即把buffer里面的数据排序之后存到本地临时文件里面。


**Spill触发**

spill触发条件是在Spillable#maybeSpill确定的，有两种情况都会触发spill，分别是

1. 处理的元素个数超过配置项spark.shuffle.spill.numElementsForceSpillThreshold所设置的值，默认是Integer.MAX_VALUE
2. 当前内存使用超过一定的内存阈值。这里要计算两个东西，分别是内存使用量和内存阈值。
- 内存使用量： 通过SizeTracker这个trait完成，两种buffer数据结构PartitionedAppendOnlyMap和PartitionedPairBuffer都扩展了SizeTracker。takeSample的频次由变量SAMPLE_GROWTH_RATE决定，目前是1.1，意味着每隔十分之一更新数量就做一次takeSample，比如说当前的更新数量是1000，那么下次takeSample要再更新量达到1100的时候; 每次takeSample的时候调用SizeEstimator.estimate方法预估下当前buffer的大小，具体预估方法https://www.javaworld.com/article/2077408/sizeof-for-java.html，大致是通过遍历这个对象图获取所有相关实例的大小然后求和。
- 内存阈值: 每增加32个元素就会检查一下内存阈值，阈值初始值5*1024*1024，后续会不断从Memory Manager获取内存，可以看出spark会尽量使用内存。到最后申请不到新的内存，现有内存大于这个阈值就会触发spill。


<br/>
**Buffer按parition id排序**

spill流程的入口是在ExternalSorter#spill里面，在落盘之前，会先对buffer里面的数据进行排序, 这是在WritablePartitionedPairCollection（这两种buffer类型都继承了这个借口）的方法destructiveSortedWritablePartitionedIterator里完成的，主要步骤如下

1. 调用partitionedDestructiveSortedIterator， 两种buffer各自实现了这个方法，这里就以PartitionedAppendOnlyMap来详细讲下这个方法，PartitionedPairBuffer的类似
PartitionedAppendOnlyMap 相对应的排序是在AppendOnlyMap#destructiveSortedIterator方法完成的，这个排序动作会摧毁这个map底层的结构，流程如下：
- 首先移动底层数组的元素，使得非空值处在数组的前端，空值在数组的后部分。
- 对有数组有数据的部分进行排序，先获取key排序comparator，用来指定键值的排序方法，默认是WritablePartitionedPairCollection#partitionComparator，即只对partition进行排序（这里使用(partition id, key)这对tuple进行排序)；然后构建Sorter然后调用它的sort方法对当前内存内的数据进行排序，排序算法用的是Timsort，也是spark在内存里面主要使用的排序方法。因为是直接对当前collection进行排序，所以最后返回当前iterator。

2. 构建并返回WritablePartitionedIterator，这个iterator封装了原来的iterator，增加了writeNext和nextPartition方法，writeNext会写入Key和Value的值, nextPartition返回当前的partitiion id



```scala
def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
  destroyed = true
  // Pack KV pairs into the front of the underlying array
  var keyIndex, newIndex = 0
  while (keyIndex < capacity) {
    if (data(2 * keyIndex) != null) {
      data(2 * newIndex) = data(2 * keyIndex)
      data(2 * newIndex + 1) = data(2 * keyIndex + 1)
      newIndex += 1
    }
    keyIndex += 1
  }
  assert(curSize == newIndex + (if (haveNullValue) 1 else 0))

  new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)

  new Iterator[(K, V)] {
    var i = 0
    var nullValueReady = haveNullValue
    def hasNext: Boolean = (i < newIndex || nullValueReady)
    def next(): (K, V) = {
      if (nullValueReady) {
        nullValueReady = false
        (null.asInstanceOf[K], nullValue)
      } else {
        val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
        i += 1
        item
      }
    }
  }
}
```




<br/>
**spill到磁盘**

在ExternalSorter#spillMemoryIteratorToDisk里面完成，会对传入的数据（排好序都buffer) 进行遍历按批次flush写入到文件，期间统计每个partition的写入元素以及每个flush批次的文件长度,具体步骤如下：
1. 调用DiskBlockManager#createTempShuffleBlock生成shuffle临时文件和blockid
2. 调用BlockManager#getDiskWriter获取一个writer
3. 对传入的按分区排序的collection进行遍历，调用writeNext方法写入writer，期间统计每个partition id的写入数量，并且当写入量达到了spark.shuffle.spill.batchSize的时候（默认10000）时，进行flush操作并记录下写入的文件长度。
4. 生成SpilledFile并返回，SpilledFile里面包含了文件信息、blockId、每个partition的元素个数以及每个flush批次的长度。



<br/>

### 合并成Partition文件

在所有数据写入buffer（包括spill）之后，最终会把buffer里面的数据和spill文件的数据合并成一个分区文件供reduce task获取数据，具体实现是在SortShuffleWriter#write方法里面，基本步骤如下：

1.  通过org.apache.spark.shuffle.IndexShuffleBlockResolver#getDataFile新建一个文件存放最终数据
2. 通过shuffleId和mapId构建org.apache.spark.storage.ShuffleBlockId
3. 调用org.apache.spark.util.collection.ExternalSorter#writePartitionedFile合并文件
4. 调用org.apache.spark.shuffle.IndexShuffleBlockResolver#writeIndexFileAndCommit生成index文件
5. 调用org.apache.spark.scheduler.MapStatus#apply生成最后的MapStatus



可以看出比较重要的是3-5步，如下图所示：

![]({{ "/images/spark-shuffle-partition-merge.png" | absolute_url }})


可以看到大致的流程是缓存经过排序后跟spill文件里面按partition进行合并生成数据文件，根据合并中获取到的每个分区在文件里面的长度生成index文件，下面详细展开讲解。


<br/>
**合并并生成分区数据文件**


合并生成分区文件的入口在org.apache.spark.util.collection.ExternalSorter#writePartitionedFile方法，首先通过org.apache.spark.storage.BlockManager#getDiskWriter方法获取org.apache.spark.storage.DiskBlockObjectWriter，然后再检查是否有spill文件，如果没有则跟spill流程里面的排序一样屌用destructiveSortedWritablePartitionedIterator方法来排序，然后在写入文件；如果有spill文件，则要先合并内存里面的buffer和spill文件，其实这个时候也不算合并，只是对spill文件和buffer进行梳理，遍历所有partition ID，对于每个id获取每个spill文件以及buffer里对应于这个id的iterator，最后返回一个大的iterator，这个iterator里面的元素是partition id以及这个partition ID对应的所有数据的iterator，里面的这个iterator的数据有可能来自buffer，也有可能来自spill文件。合并过程具体在ExternalSorter#partitionedIterator方法里面完成，主要调用了ExternalSorter#merge方法，这个方法的主要步骤：

1. 用spill创建SpillReader
2. 遍历所有partition id，对于每个partition id
a. 用partition id和内存里面的数据创建IteratorForPartition，相当于拿到了当前partition id在内存数据里面的iterator（主要是因为内存数据已经经过partition id排序，所以IteratorForPartition#hasNext方法才可以正确工作。
b. 对所有SpillReader，调用它的readNextPartition，返回所有spill文件里面关于当前partition id的数据的iterator, spill文件里面定位partition主要是通过SpilledFile#elementsPerPartition来达成。
c. 最后把每个partition ID对应到内存里面和每个spill文件里面的数据以iterator的形式返回，这样就达到了按parition id进行 merge sort的效果。



在获取到每个partition对应的iteartor之后，针对每个partition id，调用org.apache.spark.storage.DiskBlockObjectWriter#write方法写入该id对应的所有KV组合，写完之后会调用DiskBlockObjectWriter#commitAndGet方法做一些flush操作以及生成一个FileSegment，FileSegment记录这个parition在最终分区文件里面的偏移量以及长度。

最后writePartitionedFile方法返回一个数组，改数据记录了每个partition在文件里面所占用的长度。

<br/>
**生成index文件并提交**

在合并完成partition数据文件之后会获取到每个partition的长度的信息，然后早方法IndexShuffleBlockResolver#writeIndexFileAndCommit里面生成index文件，index文件里面存的就是用每个partition的长度信息稍微经过转换变成每个partition在分区数据文件里面的offset信息，这时候数据和索引都是临时文件，所以需要把临时文件重命名成正式文件。

<br/>
**生成MapStatus**

可以看到最后返回的MapStatus里面只包含了shuffleServerId和各个partition的长度，shuffleServerId也就是BlockManagerId，这个ID并不能指定到文件。如果要获取data文件或者index，则需要调用IndexShuffleBlockResolver的getDataFile和getIndexFile。


<br/>
## MapOutputTracker

MapOutputTracker用来记录map status的地方，根据是在driver还是executor分别是MapOutputTrackerMaster和MapOutputTrackerWorker，他们是在SparkEnv里面被创建，

```scala
    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
    } else {
      new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerEndpoint after initialization as MapOutputTrackerEndpoint
    // requires the MapOutputTracker itself
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))
```

可以看到如果是driver会创建MapOutputTrackerMaster，否则创建MapOutputTrackerWorker，并且后续registerOrLookupEndpoint方法里面也会根据是否在driver相应地注册endpoint还是只是获取一个endpointref。

当ShuffleMapTask成功完成任务时，DAGScheduler会在handleTaskCompletion方法里面调用MapOutputTrackerMaster的registerMapOutput方法来注册这个task的MapStatus，也就是说MapStatus的存放的中心是driver端的MapOutputTrackerMaster。



<br/>

## Shuffle Read流程


等所有map任务完成之后，reduce任务就可以开始shuffe读取的操作了，入口时在ShuffleReader的唯一实现BlockStoreShuffleReader的read方法，一般来说这是在ShuffledRDD被调用的

```scala
override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
  val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
  SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
    .read()
    .asInstanceOf[Iterator[(K, C)]]
}
```

Shfulle读取阶段主要分为3步：
1. 通过MapOutputTracker获取MapStatus以便确认mapoutput的位置。
2. 读取partition id相对应的所有map output的block到本地
3. 从本地block文件逐条读取处理数据

下面对这几个步骤展开来讲。

<br/>
**获取MapStatus**

在BlockStoreShuffleReader#read方法里面可以看到会先初始化一个ShuffleBlockFetcherIterator，在这时候会先执行MapOutputTracker#getMapSizesByExecutorID方法去获取map output的位置信息。


MapOutputTracker#getMapSizesByExecutorID在MapOutputTrackerWorker和MapOutputTrackerMaster里面都有实现 ，在分布式的环境下MapOutputTrackerWorker#getMapSizesByExecutorId 被调用，而MapOutputTrackerMaster#getMapSizesByExecutorId只是local 模式被调用，两者只是在获取map status的方式上面有所不同，master上面直接本地获取，而worker则要通过调用askTracker方法并传入GetMapOutputStatuses通过RPC方式获取map status；

在获取mapstatus之后都会调用MapOutputTracker.convertMapStatuses来对map status作处理，把partition范围内（startPartition和endPartition之内，不包括endPartition）的所有block按blockManager分好，一个blockManager对应一个executorid，这样可以方便后续block的获取。详细步骤如下：				

1. 构建一个HashMap splitsByAddress来存储每个blockManagerID所对应的所有blockid以及每个blockid的大小。
2.对这个shuffle所对应的MapStatus数组进行遍历，如果有status是null则直接扔出Missing an output location for shuffle的异常，如果非null，则从status里面取出相应的partition范围内每个partition的大小等信息，然后去更新splitsByAddress这个HashMap
3. 返回splitsByAddress


在返回的spiltByAddress涉及到了BlockManagerId, 这代表了一个BlockManager，每个SparkEnv都有一个BlockManager。BlockManagerMasterEndpoint用来track每个executor上面的blockmanager，每个blockmanager通过BlockManagerMaster（封装了一个EndpointRef）来跟BlockManagerMasterEndpoint沟通。

<br/>
**获取Map 输出数据**

在构建ShuffleBlockFetcherIterator对象的时候，会调用ShuffleBlockFetcherIterator#initialize方法去初始化map输出数据的获取流程，并开始获取map输出数据到本地。


首先会调用ShuffleBlockFetcherIterator#splitLocalRemoteBlocks，把传入的shuffle block 按本地和远程分开，如果一个blockManager内的blocks获取累计大小超过一定大小的时候，重新起一个FetchRequest,  最后返回一个FetchRequest的ArrayBuffer

然后在调用org.apache.spark.storage.ShuffleBlockFetcherIterator#fetchUpToMaxBytes去最大限度地获取map output数据，具体步骤如下：
1.  从队列fetchRequests得到一个FetchRequest ,   检查下目标host正在传输的block数量加上这个FetchRequest里面block的数量是否超过了限制，这个限制由spark.reducer.maxBlocksInFlightPerAddress控制，默认是Int.MaxValue，一般来说不会有超限发生，在一些特殊场景，比如使用external shuffle的情况下才需要对这个参数进行设置。
2. 如果不超限，则调用org.apache.spark.storage.ShuffleBlockFetcherIterator#sendRequest方法从远程主机获取map输出相关的block数据，假如获取的总量超过了spark.maxRemoteBlockSizeFetchToMem（默认2G左右），则会把获取到的block数据放到磁盘上，否则放到内存里面。获取block具体调用的是org.apache.spark.network.netty.NettyBlockTransferService#fetchBlocks方法，NettyBlockTransferService是用Netty实现的在spark executor之间传输block的服务，它是在SparkEnv创建的时候初始化然后传入BlockManager的。

3. 获取成功的block会通过BlockFetchingListener把SuccessFetchResult添加到results（LinkedBlockingQueue[FetchResult]）



接着调用org.apache.spark.storage.ShuffleBlockFetcherIterator#fetchLocalBlocks获取本地block，直接调用BlockManager#getBlockData获取本地block的信息，然后生成SuccessFetchResult并添加到results

<br/>
**记录插入ExternalSorter**

在构建ShuffleBlockFetcherIterator之后，会调用org.apache.spark.serializer.SerializerInstance#deserializeStream对每个stream进行反序列化，并把stream转换成kv iterator， 在对iterator进行各种包装处理之后会新建一个ExternalSorter，然后调用ExternalSorter#insertAll方法插入所有记录，最后再返回org.apache.spark.util.collection.ExternalSorter#iterator 供后续计算调用。



至此就shuffle这个过程就完成了。





<br/>




注：本篇文章的spark源码是基于2.4.4版本，更确切地说是基于tag v2.4.4-rc5




