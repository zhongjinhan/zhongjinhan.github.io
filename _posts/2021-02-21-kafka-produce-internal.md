---
layout:     post
title:      Kafka消息生产详解
date:       2021-02-21
summary:    介绍了Kafka消息生产详细流程
categories: kafka
published:  true
---



<!-- TOC -->

- [消息Produce客户端部分](#消息produce客户端部分)
    - [KafkaProducer#send方法](#kafkaproducersend方法)
    - [RecordAccumulator队列](#recordaccumulator队列)
    - [Sender线程](#sender线程)
        - [发送Producer数据](#发送producer数据)
        - [NetworkClient#poll](#networkclientpoll)
- [消息Produce服务端部分](#消息produce服务端部分)

<!-- /TOC -->


Kafka的消息生产类似于数据库的写入路径，详细理解这方面的内容是进一步理解Kafka的前提。Kafka的消息生产涉及到客户端和服务端的流程，服务端只有Java实现，但是客户端涉及到了多种语言，这里也只考虑Java实现，下面就详细对这两部分进行介绍。


<br/>

# 消息Produce客户端部分

当我们想用Kafka Java API来实现一个消息发送程序的时候，先会构建一个KafkaProducer，然后调用它的send方法来发送消息，这个send方法并不是直接把这个消息通过网络组件发送给Broker，而是如下图示所示先放到一个队列里面:


![]({{ "/images/kafka-produce-internal-produce-overview.png" | absolute_url }})

从上图可以看到消息生产或者推送主要涉及到如下两个个步骤
- 客户端线程调用KafkaProducer#send方法来追加消息到队列RecordAccumulator
- Sender线程从RecordAccumulator里面获取记录批次然后通过网络组件发送到Broker



<br/>

## KafkaProducer#send方法

这是Kafka客户端API里面最常见的方法之一，用它来发送构造好的ProducerRecord，而实际上这个方法只是把ProducerRecord处理之后发送到队列，集体步骤如下


![]({{ "/images/kafka-produce-internal-append.png" | absolute_url }})



每个步骤的详细情况如下：

* 等待Metadata更新完成：调用KafkaProducer#waitOnMetadata等待Metadata更新完成，最长等待时间由max.block.ms控制，默认是60秒。Metadata的更新以异步的方式由Sender主线程调用NetworkClient#poll方法去更新，内部会调用DefaultMetadataUpdater#maybeUpdate方法判断是否要更新，主要判断Metadata#needUpdate变量，如果需要更新，则调用NetworkClient#sendInternalMetadataRequest发起Metadata请求，Broker处理完成之后发回来由NetworkClient#handleCompletedReceives调用DefaultMetadataUpdater#handleCompletedMetadataResponse来处理response，接着调用Metadata#update(int, MetadataResponse, long)，这里面会让updateVersion加一，然后调用Metadata#handleMetadataResponse来更新metadata cache，最后调用notifyAll方法来唤醒当前的等待。


* 序列化Key和Value: 根据用户提供的Key和Value对应的Serializer，调用对应的serialize方法来序列化Key和Value

* 计算消息的partition：默认partition类由配置partitioner.class控制，默认是producer.internals.DefaultPartitioner。DefaultPartitioner的策略是如果ProducerRecord里面已经指定了，则用partition，则直接使用这个partition；否则检查key是否为null，如果不为null，则用key的hash mod partition个数，如果为null，则round robin选择。这里的partition个数就是从上面更新的metadata里面获取，所以metadata必须要在这之前更新好。

* 调用RecordAccumulator#append把消息append到队列



<br/>

## RecordAccumulator队列

队列信息如下，每个TopicPartition对应一个ProducerBatch的一个Deque



![]({{ "/images/kafka-produce-internal-produce-queue.png" | absolute_url }})

Append消息到RecordAccumulator流程

1. 调用RecordAccumulator#getOrCreateDeque获取TopicPartition对应的队列
2. 如果队列为空或者处于队列末端的ProducerBatch已经满的情况下，创建一个新的ProducerBatch，然后把消息append到这个ProducerBatch。
3. 最后把这个新建的ProducerBatch加入到Deque的末端


每个ProducerBatch包含一个MemoryRecordsBuilder，往ProducerBatch追加消息记录，其实就是调用MemoryRecordsBuilder#append往MemoryRecordsBuilder里面的bytebuffer写入消息记录，bytebuffer外面包了一层ByteBufferOutputStream， 然后再加上DataOutputStream，里面包含了一个可配置的压缩流程，最后key、value等信息是通过DefaultRecord#writeTo方法往DataOutputStream写入。




<br/>

## Sender线程




Sender线程的主要工作是从RecordAccumulator队列里面获取数据然后发送Broker，KafkaProducer构建的时候就会构建Sender并启动其主线程，主线程调用的方法Sender#runOnce主要做了两件事情
1. 调用Sender#sendProducerData发送数据
2. 调用NetworkClient#poll处理网络IO


<br/>

### 发送Producer数据

发送Producer数据步骤

![]({{ "/images/kafka-produce-internal-send.png" | absolute_url }})



1. 获取可发送数据的节点列表：调用RecordAccumulator#ready获取可发送数据的broker节点列表，这里面会遍历所有TopicPartition所对应的队列，查看队列里面第一个ProducerBatch是否同时满足以下条件：
  - 这个ProducerBatch不处于backingoff状态，所谓的backingoff状态就是发送失败并处于等待期(retryBackoffMs)内，这是为了避免不断地对一个失败节点进行重复尝试。
  - ProducerBatch满了或者Accumulator没内存了并且有线程在等待内存或者有flush操作等等
2. 检查节点是否可用：遍历步骤1里面的节点，调用NetworkClient#ready检查目标节点，主要检查到节点的网络连接是否创建，如果没创建则调用NetworkClient#initiateConnect去初始化连接。另外还会调用InFlightRequests#canSendMore检查是否可发送更多请求到对应的节点，同一时间只有一个Send处于发送状态，可以有多个Send处于等待应答状态。

3. 从队列获取ProducerBatch列表：调用RecordAccumulator#drain从RecordAccumulator里面获取ProducerBatch列表，会对可用的节点进行遍历，然后调用RecordAccumulator#drainBatchesForOneNode获取每个节点包含的leader partition的producer batch，这里面drainIndex的作用是防止一只获取某个partition的batch，而均匀地获取一个节点上每个partition的数据。

4. 构建并发送ProduceRequest：调用Sender#sendProduceRequests发送数据，这里面会遍历每个ProducerBatch，调用ProducerBatch#records构建MemoryRecords，最后加上Partition信息一起构建一个ProducerRequest，接着进一步构建构建ClientRequest，然后调用NetworkClient#send发送请求，这个发送也是异步的，只是发起了发送流程。


<br/>

### NetworkClient#poll

不管是Consumer还是Producer都会用到NetworkClient，都会调用NetworkClient#poll方法来完成异步的网络IO操作，包括连接创建、发送数据和接收数据，详细流程如下：


1. 调用MetadataUpdater#maybeUpdate来更新metadata，内部会调用NetworkClient#sendInternalMetadataRequest来发送MetadataRequest到broker
2. 调用Selector#poll，实际的数据IO都在这里面触发处理
  - 调用Selector#select，这方法调用了nio selector的select方法
  - 获取一个readykeys的集合 Set<SelectionKey> readyKeys = this.nioSelector.selectedKeys();
  - 针对不同的条件调用Selector#pollSelectionKeys来处理这些readykey，这里面会循环所有的selectKeys，然后根据key的类型以及对应的条件作处理
    - 对于read：调用xxx处理
    - 对于write：调用KafkaChannel#write把Send写入io channel，然后往this.completedSends列表添加Send
  - 对于数据接收方面，会调用Selector#addToCompletedReceives()来把完成接收的数据放在Selector#completedReceives里面
3. 调用NetworkClient#handleCompletedSends来处理已经完成的发送，这里主要针对那些不需要应答的发送请求, 即NetworkClient.InFlightRequest#expectResponse = false
  - 调用InFlightRequests#lastSent获取相当节点对应的发送队列的第一个元素
  - 如果是不需要应答的请求，调用InFlightRequests#completeLastSent移除第一个元素
4. 调用NetworkClient#handleCompletedReceives来处理已经完成的接收
  - 遍历Selector#completedReceives里面的NetworkReceive
  - 针对每个NetworkReceive，先调用NetworkClient#parseStructMaybeUpdateThrottleTimeMetrics来解析header和response body
  - 接着调用AbstractResponse#parseResponse用上一步骤的responsebody(struct)根据apiKey来构建对应的Response，比如metadata，那么就是MetadataResponse  
  - 最后把response通过NetworkClient.InFlightRequest#completed方法处理之后生成ClientResponse然后加到传入的List<ClientResponse>里面，但特殊的response除外，比如:
    - 如果是MetadataResponse，调用MetadataUpdater#handleCompletedMetadataResponse处理
    - 如果是ApiVersionsResponse，调用NetworkClient#handleApiVersionsResponse处理
5. 调用NetworkClient#handleDisconnections来处理断开连接
6. 调用NetworkClient#handleConnections来处理已经建立的连接
  - 遍历Selector#connected里面的内容
  - 检查变量discoverBrokerVersions，如果false的话调用ClusterConnectionStates#ready把连接状态设置成READY，如果是true的话则构建ApiVersionsRequest.Builder，KafkaConsumer构建NetworkClient传入的值是true,所以结果是构建ApiVersionsRequest.Builder并添加到NetworkClient#nodesNeedingApiVersionsFetch里面，在NetworkClient#poll里面会调用NetworkClient#handleInitiateApiVersionRequests来处理发送这个请求。最后在NetworkClient#handleCompletedReceives会调用NetworkClient#handleApiVersionsResponse来处理对应的response。

7. 调用NetworkClient#completeResponses完成一般请求的reponse
  - 调用ClientResponse#onComplete
    - 调用RequestCompletionHandler#onComplete，这个方法的实现有如下两个，分别对应于consume和produce的应答处理
      1. ConsumerNetworkClient.RequestFutureCompletionHandler#onComplete
      2. RequestCompletionHandler#onComplete



<br/>


# 消息Produce服务端部分



Broker端处理Produce请求的大致流程如下：

![]({{ "/images/kafka-produce-internal-produce-server.png" | absolute_url }})



1. 解析请求并扔到RequestChannel队列 - 对于任何类型的客户端请求，解析请求的流程类似，在Processor里面接收到完成的请求数据之后，会通过每个请求类似预定义好的schema来解析请求，详细情况如下：
    - 解析处理的入口是Processor#processCompletedReceives，这里会遍历Selector#completedReceives已经完成接收的请求
    - 先调用RequestHeader#parse解析请求的头部信息，因为需要对SASL认证请求作特殊处理
    - 接着构建RequestContext#RequestContext，并用它跟其他数据接着构建RequestChannel.Request
    - 在构建RequestChannel.Request的时候，会调用RequestContext#parseRequest来解析请求
    - 解析过程中首先会获取ApiKeys，比如Produce请求预定义的ApiKeys是ApiKeys#PRODUCE，这里面包含了ProduceRequest.schemaVersions()和ProduceResponse.schemaVersions()；接着会调用ApiKeys#parseRequest根据Request的schema信息来解析传入的bytebuffer，最后返回一个struct
    - 最后调用AbstractRequest#parseRequest依据不同的api
   key用struct生成不同的请求，比如PRODUCE生成ProduceRequest
    - 请求完成之后会调用RequestChannel#sendRequest把请求扔到队列


2. 从RequestChannel获取ProduceRequest分别写入不同leader partition的本地日志 - 不同ApiKey所对应不同的处理方法，这个路由是在KafkaApis#handle里面。一个ProduceRequest可以包含多个partition的数据，所以需要遍历每个TopicPartition，获取对应当然log，然后往里面写入数据。因为ProduceRequest发送的broker是leader replica所在的节点，所以写入的是本地的leader replica所对应的log，其他follower replica会从这个leader replica读取数据然后应用到他们本地的日志。详细信息：
    - ReplicaManager#appendToLocalLog
kafka.cluster.Partition#appendRecordsToLeader，先获取Replica，从Replica获取对应的日志kafka.log.Log，调用kafka.log.Log#appendAsLeader方法把记录追加到日志文件,这里面经过各种验证之后，最后调用kafka.log.LogSegment#append来append这些REcords，最后的最后在FileRecords里面把records写入FileChannel。

3. 不延迟发送response的情况直接构建response然后扔到Processor的response队列里面：当ack不等于-1的时候，会直接调用KafkaApis#handleProduceRequest里面的sendResponseCallback方法返回response，这里面会进一步调用RequestChannel#sendResponse，最终通过Processor#enqueueResponse往Processor的response队列里面加入一个response

4. 延迟发送response的提交到DelayedOperationPurgatory - 如果满足了ReplicaManager#delayedProduceRequestRequired方法里面的条件，则会进入等待状态。会先构建一个DelayedProduce，调用DelayedOperationPurgatory#tryCompleteElseWatch尝试完成这个操作，如果没成功则会调用DelayedOperationPurgatory#watchForOperation提交到watch列表。然后接着调用DelayedOperation#maybeTryComplete再次尝试完成，如果还是没完成则调用SystemTimer#add加入到timer里面。

5. 负责follower读取的handler会尝试完成delayedoperation然后通过processor发送response -  当follower replica从leader replica读取数据的时候，leader replica所在的broker响应了这个请求并读取数据之后发送应答之前会更新当前broker维护的replica列表(follower)的信息，这个更新包含两个步骤，第一步会去更新Replica的_logEndOffsetMetadata，第二步会尝试去更新highwater mark。接着检查watch列表里面对应partition的delayedproduce操作，如果所有的partition对应的replica的hw都满足要求，则完成本次DelayedProduce，调用sendResponseCallback发送response给Processor的队列。





---------------------------------
注：kafka源码基于apache kafka 2.2.1 










