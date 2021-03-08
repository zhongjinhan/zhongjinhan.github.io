---
layout:     post
title:      Kafka消息消费详解
date:       2021-03-08
summary:    介绍了Kafka消息消费详细流程
categories: kafka
published:  true
---



<!-- TOC -->

- [消息Consume客户端实现](#消息consume客户端实现)
    - [a. 更新Topic订阅状态](#a-更新topic订阅状态)
    - [b. 准备Fetch请求并放入unsent队列](#b-准备fetch请求并放入unsent队列)
    - [c. 发送请求到KafkaChannel并放入inFlightRequests队列](#c-发送请求到kafkachannel并放入inflightrequests队列)
    - [d. 请求数据写入SocketChannel](#d-请求数据写入socketchannel)
    - [e. 从SocketChannel接收到响应数据](#e-从socketchannel接收到响应数据)
    - [f. 解析响应数据并放入pendingCompletion队列](#f-解析响应数据并放入pendingcompletion队列)
    - [g. 处理FetchResponse并放到completedFetches队列](#g-处理fetchresponse并放到completedfetches队列)
    - [h. 生成ConsumerRecord](#h-生成consumerrecord)
- [消息Consume服务端实现](#消息consume服务端实现)
    - [1. 解析请求并扔到RequestChannel](#1-解析请求并扔到requestchannel)
    - [2. 处理FetchRequest](#2-处理fetchrequest)
    - [3. 直接返回Response](#3-直接返回response)
    - [4. 延迟返回response](#4-延迟返回response)
    - [5. 完成延迟response](#5-完成延迟response)

<!-- /TOC -->



Kafka的消息消费流程类似于数据库的读取路径，详细理解这方面的内容是进一步理解Kafka的前提。Kafka的消息消费涉及到客户端和服务端的流程，服务端只有Java实现，但是客户端涉及到了多种语言，这里也只考虑Java实现。



<br/>

# 消息Consume客户端实现


如果要用最简单的方式使用Kafka Client API去消费Topic里面的数据，通常我们会有如下几个步骤
1. 用各类配置构建一个KafkaConsumer
2. 采用subscribe自动或者assign手动订阅TopicPartition
3. 调用poll方法来获取ConsumerRecords


这里就讲下poll方法里面到底做了哪些事情。因为Kafka Consume客户端大量使用了异步调用，同一个方法经常会被多次调用，所以如果只按代码的执行顺序去讲解整个poll流程会显得比较乱。因为consume整个流程无非就是向broker发送请求然后处理应答获取ConsumerRecord的过程，所以这里就讲下这个线程过程，然后把每个步骤都映射到代码里面，整个详细流程如下图所示：


![]({{ "/images/kafka-consume-internal-overview.png" | absolute_url }})



<br/>

虽然上图有很多队列存在，但每一步骤都是由客户端线程发起的，另外这里默认没有coordinator，也就是group.id为null的情况的流程，下面针对每个步骤进行详解。



<br/>

## a. 更新Topic订阅状态



更新Topic订阅状态里面的核心类是SubscriptionState，这是Consumer用来跟踪的topic、partition以及offset的一个类，当调用SubscriptionState#assignFromUser(由KafkaConsumer#assign方法被用户调用触发)或者xx的时候会初始化一系列assignment，。
SubscriptionState最重要的状态变量就是assignment，类型是PartitionStates，内部包含了一个LinkedHashMap用来存放TopicPartition和TopicPartitionState的对应关系。PartitionStates#update这里面的一堆操作看起来只是为了维持TopicPartition一定的顺序。


SubscriptionState是在KafkaConsumer构造的时候被构建，并被传到ConsumerCoordinator和Fetcher这两个类里面。



更新Topic订阅状态的入口是KafkaConsumer#updateAssignmentMetadataIfNeeded，主要目的是更新SubscriptionState各个TopicPartition的初始fetch position。方法里面在检查coordinator之后接着调用KafkaConsumer#updateFetchPositions，流程如下:

1. 调用SubscriptionState#hasAllFetchPositions检查状态里面的各个positio是否都已经有值了
2. 如果state里面的position没有设置，调用SubscriptionState#resetMissingPositions重置每个TopicPartitionState内部的OffsetResetStrategy，默认strategy是latest，这时候TopicPartitionState里面的poisition还是没有更新的
3. Fetcher#resetOffsetsIfNeeded: 
    - 调用Fetcher#offsetResetStrategyTimestamp用每个partition的OffsetResetStrategy去获取对应的ListOffsetRequest，
        - EARLIEST返回ListOffsetRequest.EARLIEST_TIMESTAMP(-2L)，
        - LATEST返回ListOffsetRequest.LATEST_TIMESTAMP,
    - 接着调用Fetcher#resetOffsetsAsync，内部会先调用Fetcher#groupListOffsetRequests来把topicpartition相关信息按对应的leader node分组，leader node信息从metadata获取。然后遍历分组之后的数据，调用Fetcher#sendListOffsetRequest往每个node发送ListOffsetRequest，成功获取offset结果之后会调用Fetcher#resetOffsetIfNeeded来更新SubscriptionState里面的position信息。


<br/>

## b. 准备Fetch请求并放入unsent队列


这一步骤的入口是方法Fetcher#sendFetches，这里面首先调用Fetcher#prepareFetchRequests生成Fetch请求，然后遍历每个请求通过ConsumerNetworkClient#send方法添加到unsent队列，unsent队列的实现是UnsentRequests类，里面包含了一个按节点分组的队列，即每个节点都有一个未发送队列。发送到队列之后还会添加一个RequestFutureListener的回调函数，这个函数里面的方法会在收到应答之后才会被执行。



Fetcher#prepareFetchRequests的详细流程：
  - Fetcher#fetchablePartitions指的是不在nextInLineRecords和completedFetches里面的TopicPartition
  - 调用Metadata#partitionInfoIfCurrent获取TopticPartition对应的Node，如果node是null，则调用Metadata#requestUpdate请求metadata更新；如果node存在，则调用构建一个FetchSessionHandler，并调用FetchSessionHandler#newBuilder构建一个Builder，然后调用FetchSessionHandler.Builder#add增加一个fetch
  - 最后遍历fetchable，调用所有FetchSessionHandler.Builder#build方法构建FetchRequestData




RequestFutureListener实现了onSuccess和onFailure方法，分别为发送成功和失败的处理方法。当成功返回的时候调用Fetcher#sessionHandler来获取存放在sessionHandlers变量里面的FetchSessionHandler，并且调用FetchSessionHandler#handleResponse来处理response。接着从FetchResponse#responseData里面获取partition和records数据，并用这些来构建Fetcher.CompletedFetch，然后添加到completedFetches里面。



Fetch请求如果能放入Unsent队列，至少说明消费的TopicPartition相关的metadata是没有问题的。


<br/>

## c. 发送请求到KafkaChannel并放入inFlightRequests队列


从unsent队列获取请求发送到KafkaChannel并放入inFlightRequests队列，这一步骤的入口在ConsumerNetworkClient#trySend，是在ConsumerNetworkClient#poll里面被调用的。trySend的流程是以节点为单位遍历UnsentRequests里面的请求，检查节点是否就绪，如果可用则把请求放到对应的KafkaChannel，KafkaChannel代表到一个节点的连接，并同时把这个请求放到NetworkClient#inFlightRequests队列里面，后续接收到的应答的时候会到这个队列里面查找对应的请求信息。如果一个请求能完成这个步骤，说明请求包含的的TopicPartition对应的leader replica所在的节点是连接已经创建并且可以发送请求给对方了，但是还没有真正地往目标节点发送数据包。




**检查节点是否就绪**

检查节点是否就绪调用的是NetworkClient#ready方法，


- 开始连接某个节点，先调用NetworkClient#isReady来确认到这个节点的连接是否已经ready，这里的检查条件包括：1) ClusterConnectionStates里面的信息是否准备就绪，调用了ClusterConnectionStates#isReady； 2）Selector里面的KafkaChannel是否已经建立，调用了Selector#isChannelReady；3)调用InFlightRequests#canSendMore检查是否可以发送更多请求，因为InFlightRequests里面的队列是有大小限制，并且同一时间只允许一个请求处于发送状态。
- 如果不是ready的，那么调用NetworkClient#initiateConnect来初始化到某个节点的连接，
    - 调用ClusterConnectionStates#connecting来更新ClusterConnectionState节点连接信息
    - 调用Selector#connect 连接到broker: 这个方法只是发起连接并不保证连接到客户端，真正连接成功要由poll来确认。




**发送到对应的KafkaChannel**


这里调用的是NetworkClient#send方法，最终会调用Selector#send把构建的Send对象放到把KafkaChannel里面。在这过程中会构建InFlightRequest并放入inFlightRequests，这是在NetworkClient#doSend完成。


<br/>

## d. 请求数据写入SocketChannel

最终在这一步把数据往目标节点对应的SocketChannel里面写入，写完之后会更新Send的compelted标记，这样InFlightRequest队列里面的信息会被更新掉，下一个请求可以继续发送了。这一步骤的主要作用是考虑到网络输出的繁忙程度，有可能数据传输会阻塞，所以采用非阻塞的网络IO来实现。



数据写入SocketChannel的入口是Selector#pollSelectionKeys方法，里面会调用KafkaChannel#write，最终写入会调用对应的TransportLayer#
write方法把数据写入SocketChannel，比如没有任何认证的网络传输采用的是PlaintextTransportLayer。数据全部往SocketChannel写完之后，ByteBufferSend#completed方法就返回true了。




[comment]: <> (//TODO: 关于OP_WRITE key 是如何跟selector需要拎清楚)


<br/>

## e. 从SocketChannel接收到响应数据

这也是网络层Selector负责，入口同样也是在Selector#pollSelectionKeys方法，这里面会调用Selector#attemptRead方法，内部会进一步调用KafkaChannel#read方法从socketChannel读取数据，详细流程跟数据写入SocketChannel一样。读取完成的网络请求会通过Selector#addToCompletedReceives方法添加到Selector#completedReceives列表供后续处理

<br/>

## f. 解析响应数据并放入pendingCompletion队列

这一步骤的入口是在NetworkClient#handleCompletedReceives，里面会从上一步Selector#completedReceives里面获取到已经完成接收的响应数据，并从inFlightRequests队列里面匹配到响应所对应的请求信息，然后用请求里面包含的相关信息去解析对应的响应，这里就是FetchResponse，最后会调用原先请求里面设置的回调函数把回调函数自身放入ConsumerNetworkClient#pendingCompletion队列。



<br/>

**在inFlightRequests队列匹配响应的请求**

从inFlightRequest匹配响应对应的请求非常简单，只是调用InFlightRequests#completeNext从对应节点的队列里面拿出最后一个请求即可。这也说明了一个客户端到一个broker的请求和响应是严格按顺序来的。

<br/>

**解析响应**

跟服务端的请求解析类似，响应解析调用的是NetworkClient#parseStructMaybeUpdateThrottleTimeMetrics方法生成Struct，然后调用AbstractResponse#parseResponse用Struct生成FetchResponse。


<br/>

**回调函数**

在NetworkClient#poll内部最后会调用NetworkClient#completeResponses, 这里面会遍历每个response，然后调用callback函数，即RequestFutureCompletionHandler#onComplete把自身放入ConsumerNetworkClient#pendingCompletion队列。RequestFutureCompletionHandler这个回调是在发送请求的时候放入unsent队列之前构建的，是在ConsumerNetworkClient#send方法里面。


<br/>

## g. 处理FetchResponse并放到completedFetches队列

入口方法是ConsumerNetworkClient#firePendingCompletedRequests，这里会遍历pendingCompletion队列里面的RequestFutureCompletionHandler，然后调用RequestFutureCompletionHandler#fireCompletion，这里面在正常情况下会调用RequestFuture#complete方法去执行注册到RequestFuture里面的的所有listener#onSuccess方法。

这些listener正是在Fetcher#sendFetches方法里面调用ConsumerNetworkClient#send之后添加的，在onSucess方法里面对FetchResponse进行处理，最后构建CompletedFetch添加到Fetcher#completedFetches队列里面。


<br/>

## h. 生成ConsumerRecord

最后一个步骤当然是从Fetcher#completedFetches队列里面获取CompletedFetch，然后处理成ConsumerRecords，期间会用到TopicPartitionState里面的信息。

这一步骤的入口是Fetcher#fetchedRecords方法，详细流程如下。
1. Fetcher#completedFetches#peek, 这个是一个ConcurrentLinkedQueue，如果队列是空peek返回null，对于首次调用这个方法，肯定返回一个空的记录集合;如果非空返回一个CompletedFetch,里面包含
  - TopPartition
  - Records
  - FetchResponseMetricAggregator
  - responseVersion
2. 调用Fetcher#parseCompletedFetch来解析CompletedFetch，步骤如下
  - 调用SubscriptionState#position(TopicPartition)获取当前TopicPartitionState里面对应partition的position信息，拿获取的position信息和CompletedFetch里面的offset对比，如果不一致直接放弃这次结果。
  - 构建Fetcher.PartitionRecords并返回，PartitionRecords的主要内容是MemoryRecords#batches返回一个RecordBatch的itrator
3. 调用completedFetches.poll()删除队列头上的元素
4. while循环第二轮，这时候nextInLineRecords不为null，所以调用Fetcher#fetchRecords从nextInLineRecords来获取ConsumerRecord
  - 调用Fetcher.PartitionRecords#fetchRecords获取ConsumerRecord List, 这里先调用Fetcher.PartitionRecords#nextFetchedRecord来获取下一个record，接着调用Fetcher#parseRecord来把record解析成ConsumerRecord
    - nextFetchedRecord先从batches获取一个RecordBatch，然后调用DefaultRecordBatch#streamingIterator获取Record的迭代器，最后在下一个循环里面调用next方法从迭代器里面获取一条Record并返回。

5. 第二轮里面在获取到ConsumerRecord列表之后，把这些记录按Topicpartition放到HashMap里面。
6. 整个循环的退出条件是获取的记录数量到达max.poll.records - 默认大小500，或者completedFetches目前为空




<br/>


# 消息Consume服务端实现


broker端处理Fetch请求的流程跟Produce请求非常类似，只是在handler里面调用KafkaApis#handleFetchRequest来处理Fetch请求，然后从指定的Partition log读取数据。通常在有数据的情况下会立即返回数据，在读取到最新的offset没有更新数据的情况下会把答复挂起，等对应的log写入数据之后再返回数据。


![]({{ "/images/kafka-consume-internal-broker.png" | absolute_url }})

<br/>

## 1. 解析请求并扔到RequestChannel

任何发送到broke的请求都经历这个过程，一个连接到Broker的客户端在Processor里面维持一个对KafkaChannel对象，当客户端发送请求之后，会根据请求的头信息被解析成对应类型的请求然后放到RequestChannel队列里面。

<br/>

## 2. 处理FetchRequest

处理FetchRequest的入口是KafkaApis#handleFetchRequest，这里面会根据请求参数获取对应的日志对象，然后通过日志对象获取对应的日志文件引用。需要注意的是，从broker角度来看，发送FetchRequest有两类客户端，分别是
1. 普通客户端：普通用户客户端，Kafka集群之外发起的消费请求
2. replica follower：同一个partition的follower replica发起的fetch请求，用作partitoin的数据复制

对于follower客户端，会调用ReplicaManager#updateFollowerLogReadResults方法对每个TopPartition做一些额外的事情，包括
- 调用Replica#updateLogReadResult把读取结果更新到本地leader replica，包括_logEndOffsetMetadata
- 调用Partition#maybeExpandIsr检查ISR是否需要扩展，并提升highwater mark
- 调用Partition#tryCompleteDelayedRequests完成延迟的produce

<br/>

## 3. 直接返回Response

如果有数据不延迟发送，则会生成Reponse然后放到对应Processor的responseQueue队列里面，Processor的主线程会处理逐个处理队列里面的response，把日志数据往客户端回写，下面解释下这中间涉及到的一些性能优化的实现方式。

当broker端handler处理Fetch请求的时候，最后会调用LogSegment#read来读取每个Parition对应最新logSegment的数据，这里面返回的消息记录是用FileRecords#slice生成的，结果还是一个FileRecords对象，只是特殊标示了开始和结束位置，所以到这里位置并没有把数据从文件读取到内存里面。

最后在通过KafkaChannel#send方法往客户端SocketChannel写数据的时候，会调用RecordsSend#writeTo方法，最终会调用FileRecords#writeTo。
FileRecords#writeTo方法里面可以看到数据从FileChannel直接写到SocketChannel,调用的是sun.nio.ch.FileChannelImpl#transferTo方法，这是native实现的方法，通常来说这个方法比channel之间的循环读取写入效率要高些，因为很多操作系统可以把page cache里面的数据直接往目标channel里面传输而无需拷贝。这就是所谓的zero copy。


<br/>

## 4. 延迟返回response

满足如下条件的任何一个都会马上返回Fetch响应
1. 传入的timeout小于等于0，这个值是由Consumer客户端的参数fetch.max.wait.ms控制，默认是500
2. 取得足够多的数据了，最小fetch量由fetch.min.bytes控制，这个配置也是在Consumer客户端配置，默认是1
3. 数据读取发生错误

所以在现实场景下面，在获取到最新数据之后，大部分情况下会满足条件2而进入延迟返回状态，延迟的处理跟produce的延迟返回一样，用的也是DelayedOperationPurgatory，只是操作这里构建的是DelayedFetch。

<br/>


## 5. 完成延迟response

在两种情况下会完成延迟的FetchResponse
1. 新记录追加到日志文件：在方法Partition#appendRecordsToLeader里面，在日志写入对应的Log文件之后会根据情况分别调用Partition#tryCompleteDelayedRequests或者ReplicaManager#tryCompleteDelayedFetch尝试去完成延迟Fetch。
2. follower读取数据之后更新状态的时候：在方法Partition#updateReplicaLogReadResult里面也会调用Partition#tryCompleteDelayedRequests去尝试完成延迟请求。




---------------------------------
注：kafka源码基于apache kafka 2.2.1 



