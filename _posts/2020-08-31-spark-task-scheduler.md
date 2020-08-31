---
layout:     post
title:      Spark任务集调度
date:       2020-08-31
summary:    详细介绍了任务集的调度流程
categories: spark
published:  true
---


<br/>



Spark作业提交之后，DAGScheduler会用作业的RDD和RDD之间的依赖来创建一系列相互依赖的Stage，然后提交root Stage，即没有依赖的Stage，可能会有多个。提交Stage的时候会利用Stage里面RDD partition信息来创建task，接下来就任务调度流程，如下图所示


![]({{ "/images/spark-task-scheduler-process.png" | absolute_url }})

整个流程可以分为以下几个部分：

1. 提交TaskSetManager: EventLoop独占线程把RDD解析成Stage之后，再从Stage创建TaskSetManger，最后提交到任务池
2. 发起ResourceOffer: DriverEndpoint接收到ReviveOffers之后发起ResourceOffer，即把可用的执行资源按一定规则跟任务池里面的任务相匹配
3. 启动任务: 如果第2步里面有匹配成功的任务，则启动任务


流程里面的核心环节是Resourceoffer，也就是资源匹配环节，这里面主要考虑的是任务的本地性，后面会详叙，先来看下提交任务和任务池。




<br/>

## 任务提交与任务池

<br/>

### 提交之前的准备

创建好MapStageTask和ResultTask之后会把这些task放到一个TaskSet对象中，然后调用TaskSchedulerImpl#submitTasks来提交这一批task，但是在正式提交之前会用
创建一个TaskSetManager，这个manager的主要作用是协调一个TaskSet中的任务调度,包括对TaskSet里面的每个任务的状态追踪、失败重试、任务本地性处理等。最后会提交TaskSetManager到队列中。


<br/>

### 任务池

在运行spark作业的时候我们可以通过spark.scheduler.mode来配置调度模式，可以有两种选择，分别是FIFO和FAIR，这个两种配置在内部的反应就是任务池结构的不同。
在具体实现方面，用接口SchedulableBuilder来构建任务池，而两种实现分别是FIFOSchedulableBuilder和FairSchedulableBuilder。


<br/>

**FIFO Schedule**


FIFO模式下任务池的结构如下，可以看到有个只有一个池子 - root pool，它里面包含了一个FIFO队列(ConcurrentLinkedQueue)。


![]({{ "/images/spark-task-scheduler-fifo.png" | absolute_url }})

默认情况下就是这种模式，任务集是一个个提交到这个FIFO队列里面。具体是addTaskSetManager方法调用rootPool#addSchedulable方法TaskSetManager添加到rootPool里面的schedulableQueue，并且把该TaskSetManager的parent设置成rootPool。rootPool在TaskSchedulerImpl构建的时候生成，不管是那种模式都有这样一个root pool。





<br/>

**FAIR schedule**


FAIR模式下的任务池是个树状结构，在构建任务池(FairSchedulableBuilder#buildPools)的时候从文件fairscheduler.xml里面读取调度配置信息构建一个任务池，如果没有这个文件则构建一个默认的任务池。下图是一个两层结构的任务池:


![]({{ "/images/spark-task-scheduler-fair.png" | absolute_url }})


FAIR模式调度最常用的场景是需要并行运行多个Spark作业的时候，在提交TaskSetManager的时候会调用FairSchedulableBuilder#addTaskSetManager，这时候会检查spark.scheduler.pool属性的值，如果这个值在rootPool里面已经存在，则从里面取出来然后调用addSchedule方法添加TaskSetManager；否则创建一个默认的pool，并添加到rootPool里面，然后再调用pool的addSchedule方法添加TaskSetManager。




<br/>

### 从任务池获取任务集

这是后续资源匹配的时候流程，但是因为跟任务池关联比较紧，所以直接就放在这里描述了。


从任务池获取任务集调度用的是Pool#getSortedTaskSetQueue方法，这时候考虑的主要是排序问题，一个是pool的排序，另外一个是pool里面的每个TaskSetManager排序，排序算法根据调两种：

1. FIFOSchedulingAlgorithm: 对于两个schedulable，如果优先级有差异，则直接按优先级排序；如果优先级一样，则按stageId排序。
2. FairSchedulingAlgorithm: 考虑的因素有minshare、runningtask、weight等，主要是子pool之间的比较。


从任务集队列里面消费队列并不会把TaskSetManager从队列里面移除，只有当一个TaskSet里面的所有task都完成的时候才会调用TaskSchedulerImpl#taskSetFinished把这个TaskSetManager从pool里面移除。



<br/>



## ResourceOffer入口



任务资源供给的入口有两个：
1. 对单个executor检查资源并把空闲的资源供给带运行的任务，方法是CoarseGrainedSchedulerBackend.DriverEndpoint#makeOffers(executorId)。这个方法是当一个运行的任务完成的时候会调用触发。
2. 对所有executor检查资源并把空闲的资源供给带运行的任务，方法是CoarseGrainedSchedulerBackend.DriverEndpoint#makeOffers()。DriverEndpoint收到新注册executor的时候会调用这个方法，另外很多地方会发送ReviveOffers Rpc消息给DriverEndpoint来触发这个方法，例如：
  - DriverEndpoint主线程里面，默认每隔一秒就会调用
  - TaskSchedulerImpl#submitTasks提交TaskSetManager之后
  - TaskSchedulerImpl#statusUpdate的时候发现有executor丢失之后
  - TaskSchedulerImpl#handleFailedTask任务失败之后



不管是makeOffers(executorId)还是makeOffers都会从executorDataMap里面过滤出可用的executor信息，然后调用TaskSchedulerImpl#resourceOffers尝试给任务分配资源，如果有任务分配到资源的话，最后调用launchTasks启动任务。




<br/>

## Executor执行资源

CoarseGrainedSchedulerBackend#executorDataMap保存着spark所有执行器的状态，包括服务器地址端口、所有核数、空闲核数等，也就是Spark的运行资源情况。

executorDataMap里面executor的增删是通过DriverEndpoint接收executor发送的RPC消息来完成的，比如
- 一个新启动的executor会发送RegisterExecutor消息给DriverEndpoint，DriverEndpoint接收到消息之后会在executorDataMap里面新增executor信息。
- 接收到RemoveExecutor消息的时候会删除executorDataMap里面对应的信息，RemoveExecutor消息可能由executor发起，也可能由调度器发起。



一个executor的空闲与否是通过空闲的cpu核数来决定，空闲的cpu核数通过如下方式更新：
- 从executor接收到StatusUpdate消息里面包含TaskFinished状态的时候，也就是当一个任务完成时，会释放占用的cpu增加对应executor的空闲cpu核数
- 当一个任务已获取资源准备启动的时候，会从对应的executorData里面的减少空闲cpu数量


DriverEndpoint是一个ThreadSafeRpcEndpoint，也就是说DriverEndpoint在处理消息的时候是逐条处理，不会并行处理消息，所以在处理消息的时候更新executorDataMap里面的数据是线程安全的。扩展了ThreadSafeRpcEndpoint的Endpoint串行处理消息的逻辑主要在Inbox这个类里面，可以看到在Inbox#process方法里面在收到onStart消息时候会判断endpoint是否是ThreadSafeRpcEndpoint,如果不是才把enableConcurrent设置为true。


```scala
case OnStart =>
  endpoint.onStart()
  if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
    inbox.synchronized {
      if (!stopped) {
        enableConcurrent = true
      }
    }
  }
```




ResourceOffer会从executorDataMap里面过滤出存活的executor信息然后构建WorkerOffer，最后用WorkerOffer去跟任务匹配资源, WorkerOffer包含的信息
- executorId
- host
- cores
- address




<br/>

## 单个TaskSetManager调度



因为到此为止，可用的资源已经知道了，待运行的任务也有了，那么接下来的问题就是遍历任务集列表后如何把一个个任务安插在资源槽上面。
如果不考虑任务的本地性，只是随机的把任务分配到executor上面运行，那么事情就变得非常简单，用少量代码就可以实现。
但是继承于map reduce的spark肯定会考虑任务的本地性，也就是通常说的任务跟着数据走，这样可以节省大量的网络io消耗，而大部分数据处理场景下IO永远都是瓶颈，所以任务分配的大部分逻辑都是跟这个本地性相关。


<br/>

### Spark任务的优选位置

为了节省网络IO开销，Spark会尽量让任务接近它的输入数据，这里的前提条件是必须要知道任务输入数据的位置，也就是任务的优选位置，这个可以通过Task#preferredLocations方法获取，但是不管是ShuffleMapTask还是ResultTask，位置信息都是在构建Task的时候传入的，也就是在提交任务阶段(方法DAGScheduler#submitMissingTasks)构建任务的时候通过从对应的RDD里面获取的位置信息。

获取位置信息的主要逻辑在DAGScheduler#getPreferredLocsInternal，基本上就是有限从cache去取，如果RDD没cache,则调用RDD#preferredLocations获取，如果还是没有的话，则检查RDD的依赖关系类型，如果是窄依赖，即非shuffle依赖的时候，则递归处理其父RDD。也就是说除了Cache，大部分是直接从RDD的preferredLocations获取位置信息的。下面举两个比较常见的RDD。

<br/>

**FileScanRDD位置信息**

数据源相关的RDD的位置信息理解起来是最直观了，尤其是HDFS数据源，因为一个文件block通常来说有3个副本，那么Spark获取位置信息时就会考虑这3个副本的datanode。但是在Spark2.0之后对于HDFS的文件默认使用了FileScanRDD，这个RDD的一个partition有可能对应多个文件，也有可能对应一个文件里面的某几块，FileScanRDD最终返回的位置信息是按块从大到小排后取前三，逻辑实现在FileScanRDD#getPreferredLocations


<br/>

**ShuffledRDD**

另外一个常见location相关的RDD就是ShuffledRDD, 紧跟着Shuffle的RDD通常就是这个RDD，或者是它的一些特殊的版本，比如ShuffledRowRDD,但是他们关于位置获取相关的实现是一样的，就是通过MapOutTracker里面存储着的map output状态信息中获取，具体实现是在MapOutputTrackerMaster#getPreferredLocationsForShuffle。


<br/>

### 任务的本地性层级



任务本地性层级总共有如下5个级别：

```
PROCESS_LOCAL < NODE_LOCAL < NO_PREF < RACK_LOCAL < ANY
```

本地性层级从左到右对资源的要求逐级降低，其实就是优先级逐级降低，PROCESS_LOCAL对executor有要求，NODE_LOCAL对host有要求，NO_PREF表示对位置没有要求，RACK_LOCAL对配置了rack的应用在rack上面有要求，ANY则是没有要求。 

各个层级之间有一定的包含关系，如下图所示，

![]({{ "/images/spark-task-scheduler-locality.png" | absolute_url }})

- 如果一个task属于PROCESS_LOCAL，那么它一定也包含在NODE_LOCAL、RACK_LOCAL和ANY里面
- 如果一个task属于NO_PREF, 那么它也会被包含在ANY里面，但是它一定不会出现在PROCESS_LOCAL、NODE_LOCAL和RACK_LOCAL，也就说上图的至下而上的路径只能择其一




在实现方面，这个5个层级对应了TaskSetManager里面的5个变量，这5个变量是在TaskSetManager构建的时候调用addPendingTasks方法使用任务优选位置信息来初始化的，具体流程如下：


- PROCESS_LOCAL对应变量pendingTasksForExecutor：location类型是ExecutorCacheTaskLocation或者HDFSCacheTaskLocation的task会放到这个map，map的键为executorId，值为task index列表
- NODE_LOCAL对应变量pendingTasksForHost: 任务相关的所有location信息都放到这个map里面，键是host，值为task index列表，一个task index可能会出现在多个host对应的列表里面，这个非常正常，比如HDFS的一个block就有3个副本，对应3个host。
- NO_PREF对应变量pendingTasksWithNoPrefs: 没有location信息的task都会放置到这个列表里面，也就是说一个task index要么出现在pendingTasksForHost, 要么出现在pendingTasksWithNoPrefs。
- ALL对应allPendingTasks：所有待运行的任务都会默认加入到这个task列表里面。
- RACK_LOCAL对应pendingTasksForRack: 从pendingTasksForHost构建一个rack对应task index列表的信息，这个是在上面这些变量构建完成之后处理，因为这样可以一次性批处理所有hosts的rack查找，查找rack的实现是在SparkRackResolver#resolve里面。









<br/>



### 资源本地性匹配






一个TaskSetManager当前所有层级信息保存在myLocalityLevels变量里面，这是在TaskSetManager构建的时候调用computeValidLocalityLevels方法生成，具体流程是检查每个本地性层级所对应的变量，如果该层级不为空则把这个层级添加到结果里面。

ResourceOffer在匹配任务资源的时候会对myLocalityLevels里面的所有本地性层级进行逐个遍历，这里记为MaxLocality。


另外在resourceOffer方法里面会调用getAllowedLocalityLevel方法获取当前允许的本地性层级，允许的本地性层级可以有条件的降低，比如从NODE_LOCAL降低到RACK_LOCAL，优先级降低的条件有两种
1. 当前本地性层级内没有待运行的任务，那么只能增加currentLocalityIndex降低优先级。
2. 经过一段时间的等待，当前最高层级内的任务一直没有等到所需要的资源，比如NODE_COCAL层级内的任务一直等不到所需要的host，那么只能降低到RACK_ROCK。这里等待时间由spark.locality.wait控制，默认3秒，另外还可以按本地性层级进行精确控制，分别是spark.locality.wait.process，spark.locality.wait.node和spark.locality.wait.rack。这个其实就是所谓的delay schedule, 延迟调度。


获取到allowedLocality之后会跟maxLocailty去对比，如果allowedLocailty优先级小于等于maxLocailty，则把allowedLocaity设置成maxLocailty。


最后使用这个最终的AllowedLocaitly去调用TaskSetManager#dequeueTask从层级对应的待运行列表里面获取任务运行。

整个流程如下图所示:

![]({{ "/images/spark-task-scheduler-offer.png" | absolute_url }})





<br/>


## 小结

整个任务集调度的两个核心点为
1. 任务池：分为FIFO和FAIR，如果想让spark作业有效地并行运行，通常会把调度模式设置为FAIR
2. 任务本地性：为了达到任务跟着数据走的目的，会尽量满足任务的本地性等级，如果在等待一段时间后还是没法满足，则降低优先级。

