---
layout:     post
title:      Spark作业调度
date:       2020-04-05
summary:    详细介绍了Spark作业是如何运行调度和执行的
categories: spark
published:  true
---



以DAGScheduler为中心的作业任务调度模块从一开始便是Spark的核心基础，从代码历史记录也可以看出来这些类从16年之后的改动就比较少了，一方面说明新特性的添加不是在这里，另一方面也说明这块代码基本已趋于稳定，但他们还是Spark运行的基础，每个作业都少不了这些代码的运行。

这篇文章以下面这张图作为切入点来了解下Spark作业调度方面的整体流程。



![]({{ "/images/spark-dag-scheduler-process.png" | absolute_url }})


上图描述的是从一个Spark作业提交到第一个stage所对应的任务执行完成的整个流程（假设这里用spark的cluster提交模式)，具体步骤包括:

1. 提交作业：用户在主线程或者自己创建的线程里面提交作业到DAGSchedulerEventProcessLoop事件队列里面。
2. 创建Stage并提交对应的Task集合：用提交的RDD生成一个基于Stage的DAG，并提交DAG里面的开头Stage的TaskSet到TaskSchedulerImpl的Pool里面
3. 调度并启动Task: DriverEndpoint获取Pool队列里面的TaskSetManager并经过诸多的调度设置以后发送LaunchTask消息到Executor 里面的 CoarseGrainedExecutorBackend
4. 运行Task: Executor上面的 CoarseGrainedExecutorBackend接收到LaunchTask消息之后在Executor的线程池里面运行任务，运行完成之后会向DripverEndpoint发送StatusUpdate消息。
5. 更新Task状态: DriverEndpoint接收到StatusUpdate消息之后往TaskResultGetter的任务结果处理线程队列里面加入task结果处理, 处理之后往DAGSchedulerEventProcessLoop发送CompletionEvent事件。 EventProcessLoop 调用DAGScheduler#handleTaskCompletion来处理task完成之后后续该触发的动作



<br/>


## 作业提交

DAGScheduler#runJob是Spark作业运行的入口，不管是2.0之前的RDD API作业还是2.0之后的DataSet/SparkSql最终都会通过这个方法对RDD进行相应的计算。执行这个方法是在用户的线程里面，不管是主线程还是专门用来执行这个作业的线程(多个作业可以在线程里面并行运行)。这个方法会调用DAGScheduler#submitJob把JobSubmitted扔到DAGSchedulerEventProcessLoop的事件队列里面，然后返回一个JobWaiter之后，一直等待作业运行完成。


<br/>

## 创建Stage并提交对应的Task集合

DAGSchedulerEventProcessLoop是DAGScheduler里面的事件处理队列，处理的事件包括作业提交、作业取消、stage提交、stage取消等等DAG里面相关的事件，它里面有一个独占线程(thread dag-scheduler-event-loop)来处理所有的事件。

处理作业提交（JobSubmitted 事件) 调用了DAGScheduler#handleJobSubmitted方法, 处理这个事件大概分为这几步
- 首先会调用createResultStage来创建stage
- 创建完成之后调用submitStage来提交stage
  - 在提交stage的时候实际上调用的是submitMissingTasks
    - submitMissingTasks最后会调用TaskSchedulerImpl#submitTasks来提交taskSet, 


（这时候会往TaskSchedulerImpl里面的Pool队列里面加入TaskSet生成的TaskSetManager, 最后调用TaskScheduleImpl里面的backend的reviveOffers往DriverEndpoint发送ReviveOffers消息。至此，DAGSchedulerEventProcessLoop里面处理事件的线程就完成了。）


<br/>
**创建Stage**


入口方法是DAGScheduler#createResultStage, 在这里面最后一个ResultStage其实是最后创建的，而最关键的是调用getOrCreateParentStages方法生成父stage。getOrCreateParentStages方法的目的是比较明确的，即通过RDD的依赖关系以ShuffleDependency为边界从后往前遍历所有RDD创建出所有ShuffleMapStage，然后返回直接的父ShuffleMapStage，几个主要方法的调用栈如下：

```
DAGScheduler#getOrCreateParentStages 
|-- DAGScheduler#getShuffleDependencies
|-- DAGScheduler#getOrCreatehuffleMapStage
    |-- DAGScheduler#getMissingAncestorShuffleDependencies 
        |-- DAGScheduler#getShuffleDependencies
    |-- DAGScheduler#createShuffleMapStage 
```


下面对这些方法进行一些简要的说明：
- getShuffleDependencies：主要用来获取直接依赖于当前最后一个RDD的所有直接ShuffleDependency，可以看到在方法里面先获取当前RDD的依赖列表并遍历依赖列表，如果是ShuffleDependency，则添加到最后的返回结果parents里面，并且不再对ShuffleDependency对应的RDD作处理；如果不是shuffle依赖，继续递归处理这个依赖对应的RDD，直到找到Shuffle Dependency或者RDD的依赖为空的Seq（一般来说就是source rdd）为止，所以最后获取到的是直接的最近的ShuffuleDependeny
- getMissingAncestorShuffleDependencies: 递归获取当前RDD的所有直接和间接的shuffledependency， 这里同样也调用了getShuffleDependencies方法来获取直接的shuffle dependency。 可以看到这方法里面用ArrayStack来存储shuffle dependency，这样的目的是为了后续处理的时候会优先考虑越晚被遍历到的stage，有点类似于深度优先遍历，这样一来就可以看到开始的stage所生成的stage id要比后面的小。
- getOrCreateShuffleMapStage：调用getMissingAncestorShuffleDependencies方法获取传入的ShuffleDependencys的ancestor shuffle dependency， 并逐一为这些dependecy创建ShuffleMapStage, 最后为传入的Shuffle Dependency创建ShuffleMapStage，然后返回。
- createShuffleMapStage: 为ShuffleDependency创建ShuffleMapStage 


<br/>
**提交Stage**


提交Stage的入口方法是submitStage，这里首先会调用getMissingParentStages方法获取不可用的父Stage，这里可不可用的判断条件是所有的partition是不是都已经产生Map output了。 当获得不可用的父Stage之后，会遍历这个列表，然后递归调用submitStage对这些stage进行提交，然后把当前stage加入waitingStage列表里面。

假设我们有如下依赖的三个stage，stage C是result stage，那么首先被提交的是Stage A， B和C会处在waitingStage列表里面，等A运行完成之后，会再调用submitWaitingChildStages去提交B和C。

```
Stage A -> Stage B -> Stage C
```


<br/>
提交一个Stage其实是提交一个Stage里面的所有task，这是在方法submitMissingTasks里面完成的，在这里面主要做了以下几件事情：
1. 把任务执行码分发到各个executor: 根据stage类型序列化stage对应的RDD和shuffle dep或者执行的func， 然后调用braodcast方法把二进制码发送到各个executor。
2. 创建Task列表: 根据stage类型创建对应的ShuffleMapTask或者ResultTask
3. 提交Task集合: 用task列表生成TaskSet，然后调用TaskSchedulerImpl#submitTasks提交TaskSet




<br/>



TaskScheduleImpl里面包含一个SchedulableBuilder， 根据调度模式的配置会有FIFOSchedulableBuilder和FairSchedulableBuilder两种实现。而在构建SchedulableBuilder的时候会传入一个rootPool，这个Pool会有一个队列用来存放任务集合。而TaskSchedulerImpl#submitTasks里面主要就是用taskSet构建一个TaskSetManager，然后往这个队列里面塞入。最后调用SchedulerBackend#reviveOffers，往DriverEndpoint这个Rpc端发送ReviveOffers消息，每个TaskSchedulerImpl都会有一个ScheduleBackend。



<br/>

## 调度并启动Tasks

CoarseGrainedSchedulerBackend.DriverEndpoint 是Spark RPC里面的一个RpcEndpint，用来接收处理从别的进程以及别的节点上面发送过来的消息。

当DriverEndpoint接收到ReviveOffers的消息之后会调用makeOffers方法, 在这里主要做了两件事情：
1. 为Task准备集群运行资源
2. 启动Task


<br/>
**准备集群运行资源**

首先从CoarseGrainedSchedulerBackend#executorDataMap获取到集群上面可用的executor信息并转换成WorkerOffer列表，然后调用TaskSchedulerImpl#resourceOffers为task在各个executor上面提供运行所需要的资源， 包括
- 填充executor信息到几个跟踪的HashMap变量里面，包括hostToExecutors、executorIdToRunningTaskIds、executorIdToHost等,并确定是否有新的executor加入。
- 过滤掉在黑名单里面executor，这个只有在黑名单机制启动之后。
- 对WorkOffer进行重新随机排列以避免总是把任务放到同一个节点上面
- 从RootPool队列拿出任务集合列表，然后调用resourceOfferSingleTaskSet方法配对task和executor，最后返回成功获得资源的taskdescription列表


<br/>
**启动Task**

DriverEndpoint会调用launchTasks来启动任务，针对每个任务，首先会把任务描述(TaskDescription)序列化，TaskDescriptiond的序列和反序列化没有使用通用框架而是定义了encode和decode方法，这个的主要考量是效率，包括速度和序列化之后的大小。然后从CoarseGrainedSchedulerBackend#executorDataMap获取到Executor对应RpcEndpoint的reference，即CoarseGrainedExecutorBackend的reference，最后往这个reference发送RPC消息LaunchTask。




<br/>
## 执行Task

所有在CoarseGrainedExecutorBackend接收到LaunchTask消息的executor首先都会对应地解码TaskDescription数据，然后调用Executor#launchTask来生成TaskRunner，最后在executor线程池里面运行TaskRunner,即调用TaskRunner的run方法，运行完成之后会调用CoarseGrainedExecutorBackend#statusUpdate方法向DripverEndpoint发送StatusUpdate消息。

TaskRunner的核心实现都在run这个方法里面，可以看到代码主要在做几件事情
1. 任务执行前的环境准备
2. 任务执行异常处理，可以看到嵌套了两层try catch来处理各种异常。
3. 任务执行后的清理以和状态更新



<br/>
## Task执行状态更新



DriverEndpoint接收到StatusUpdate消息之后会调用TaskSchedulerImpl#statusUpdate方法, 执行TaskResultGetter#enqueueSuccessfulTask往TaskResultGetter的任务结果处理线程队列里面加入task结果处理。之后DriverEndpoint会检查这个task是否是完成状态，如果是的话则会释放这个task对应的Executor上面的cpu core，然后调用makeOffers，这次makeOffers跟上面不同，只针对当前这个executor，目的是让空闲的executor能够马上运行task。


TaskResultGetter会用一个默认4线程的线程池来处理任务结果，里面的结果处理线程会把task的结果进行反序列化处理，然后调用TaskSchedulerImpl#handleSuccessfulTask，在这里面继续调用TaskSetManager#handleSuccessfulTask, 并在这方法里面做一些跟任务结束相关的状态变更，调用DAGScheduler#taskEnded方法往DAGSchedulerEventProcessLoop发送CompletionEvent事件。




当DAGSchedulerEventProcessLoop独占线程接收到CompletionEvent事件后会调用DAGScheduler#handleTaskCompletion, 在这方法里面主要对一些Stage相关联的状态进行了更新，比如说DAGScheduler#stageIdToStage里面的stage状态。如果这个task对应的是ShuffleMapTask并且整个stage所对应的task都已经完成，那么会继续调用DAGScheduler#submitWaitingChildStages来提交先前放在waitingStage列表里面的stage，如此反复循环执行直至一个作业运行完成。






<br/>
## 主要类类图


![]({{ "/images/spark-dag-scheduler-class.png" | absolute_url }})



