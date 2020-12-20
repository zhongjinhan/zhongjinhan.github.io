---
layout:     post
title:      Kafka内部构造
date:       2020-12-20
summary:    介绍了Kafka Broker内部主要组件
categories: kafka
published:  true
---



## Kafka 集群结构


![]({{ "/images/kafka-internal-cluster.png" | absolute_url }})


Kafka的集群结构很简单，由多个Broker和一个zookeeper集群构成，从表面上看是没有master角色的，但是实际上每个Broker里面包含一个KafkaController，并且同一时间只有一个KafkaController是激活状态，这个激活的KafkaController就相当于是master。激活的KafkaController保持这对其他Broker的长连接，当Topic
、Partition或者其他信息发生变更时，会通知KafkaController，接着KafkaController会把变更的metadata和partition/replica发送到broker。

Broker之间的沟通主要是Partition的leader和replica之间的数据复制。

<br/>

## Zookeeper的作用

Zookeeper在Kafka集群里面的主要作用如下

1. 监控Broker和Controller，Broker和Controller启动之后会在zookeeper相应的路径(broker - /brokers/ids, controller - /controller)下面创建ephemeral节点
2. 存放Topic/Partition/Replica的信息，默认路径是/brokers/topics下面, 配置信息放在/config下面
3. 监控存放在zk上面的内容，当内容发生变更时通知到controller，controller在启动之后挂载了各种内容的监听(watch)



<br/>

## KafkaController 


Kafka的数据模型比较简单，一个消息流对应一个Topic，Topic可以分成多个Partition，每个Partition由若干个Replica，所以总体的元数据信息内容不是很大，一个Master角色维护这些信息相对于数据库系统来说就显得非常轻量化，所以可以把这个Master角色挂载到任意一个worker（broker）角色上面，这就是Broker里面的KafkaController。



KafkaController的主要工作是监听Zookeeper上面的元数据内容变更，然后把变更的内容处理以后主要以两种类型的请求发送给Broker
1. ApiKeys.UPDATE_METADATA：把元数据发送给各个broker，每个broker维护了一份元数据cache
2. ApiKeys.LEADER_AND_ISR：partition的leader和replica发送变化的时候，会通过状态机处理之后发送给Broker

下图是KafkaController的主要结构，可以看到KafkaController内部主要包含了一个ControllerEventManager、两个状态机以及ControllerChannelManager

![]({{ "/images/kafka-internal-controller.png" | absolute_url }})

<br/>

### Elect流程


Broker启动之后会先往zk上面注册一个ControllerChangeHandler来监听controller的变动，接着就调用KafkaController#elect方法进行选举，选举是通过看哪个controller先在zk路径/controller创建一个ephemeral节点，创建节点的那个controller就是active controller，其他controller进入standby模式。

成为Active的controller会调用KafkaController#onControllerFailover来完成初始化工作，包括：
- 调用KafkaZkClient#registerZNodeChildChangeHandler注册child handler，包括brokerChangeHandler,topicChangeHandler,topicDeletionHandler,logDirEventNotificationHandler,isrChangeNotificationHandler
- 调用KafkaZkClient#registerZNodeChangeHandlerAndCheckExistence注册nodechangehandler，包括preferredReplicaElectionHandler
和partitionReassignmentHandler
- 初始化Controller Context，里面主要包含了ControllerChannelManager的初始化，controller用这个来管理到broker的连接
- 启动Partition和Replica的状态机

<br/>

### ControllerEventManager

KafkaController内部的工作是由事件驱动的，事件的处理由的ControllerEventManager完成，CEM是在controller启动的时候被创建。CEM包含一个队列和一个线程，所有的事件会扔到这个队列，由单一线程消费处理。比如Controller启动的时候会往队列里面扔一个Startup的事件，当Topic变动的时候挂载到zk的监听会往队列里面扔TopicChange事件。每个事件都实现了ControllerEvent trait，事件处理的时候会调用事件对应的process方法的实现。

<br/>

### ControllerChannelManager

ControllerChannelManager负责管理Controller到Broker的连接，因为Controller维护的元数据以及partition的leader和replica信息随时都有可能发生变更，需要及时同步到各个broker，所以需要保持对他们的长连接。

CCM初始化Context的时候(KafkaController#initializeControllerContext)调用KafkaController#startChannelManager的时候创建，在创建的时候就会调用ControllerChannelManager#addNewBroker方法创建并维护已知broker的连接，每个Broker会有独立的一个发送队列和线程(RequestSendThread)，有关于broker的信息都存放于变量brokerStateInfo。

需要向Broker发送请求时，会调用ControllerChannelManager#sendRequest。



<br/>

### Partition和Replica状态机

KafkaController用Partition和Replica状态机来处理Partition和Replica的状态变化，两个状态机都定义了若干个状态和状态之间的合法转换，合法状态转换是通过各个状态的validPreviousStates来验证。

状态转换主要是通过PartitionStateMachine#handleStateChanges和ReplicaStateMachine#handleStateChanges来完成，比如处理新建TopicPartition的方法KafkaController#onNewPartitionCreation，就是调用了这两个方法。

在状态转换的过程当中会把关键的信息记录在ControllerBrokerRequestBatch的变量里面，比如leaderAndIsrRequestMap、updateMetadataRequestPartitionInfoMap等，最后调用ControllerBrokerRequestBatch#sendRequestsToBrokers来组织请求发送给Broker





<br/>

## Broker内部结构

除去Controller，Broker内部剩余的部分就相当于一般集群架构里面的worker角色，主要的内部组件如下图所示：


![]({{ "/images/kafka-internal-broker-overview.png" | absolute_url }})

请求在网络层面由SocketServer来处理，解析完成的请求会提交到一个队列，后续由handler调用各类Manager来完成处理。


<br/>

## SocketServer


SocketServer加上后续的队列和handler相当于是一种常见的RPC框架，比如Hadoop和HBase都采用了这种基于NIO的RPC处理框架。这个框架通常会包含以下几个角色

- 一个Acceptor，或者叫Listener，用来监听服务端口并接收分发客户端的请求
- 多个Processor（有些叫Reader）用来跟客户端建立连接(SocketChannel)，解析客户端的请求并送到队列
- 一个队列用来缓冲请求，因为处理请求的资源是有限的
- 一组handler（线程）来处理请求

SocketServer以及关联组件的交互图如下：

![]({{ "/images/kafka-internal-socket-server.png" | absolute_url }})


<br/>


### Acceptor

Acceptor主要的工作是监听并接收来自客户端的请求，并把请求分发给Processor，内部包含了一个ServerSocketChannel以及注册了SelectionKey.OP_ACCEPT的Selector。


ServerSocket启动的时候(kafka.network.SocketServer#startup)会调用方法SocketServer#createDataPlaneAcceptorsAndProcessors来启动1以acceptor和多个processor，processor的个数由config.numNetworkThreads决定，默认3。

Acceptor启动过程中调用kafka.network.Acceptor#openServerSocket创建SocketServerChannel并绑定端口。

Acceptor线程的运行方法是Acceptor#run，主要工作是循环调用Selector的select方法，接收来自客户端的连接请求，并把返回的SocketChannel轮询分配到Processor。



<br/>

### Processor

Acceptor把SocketChannel分配到Processor里面的队列（newConnections）之后，在Processor的线程方法Processor#run里面会调用Processor#configureNewConnections方法去队列里面获取SocketChannle，并配置新的连接客户端连接，期间会调用ChannelBuilder#buildChannel生成一个KafkaChannel，ChannelBuilder有三种实现，分别是SaslChannelBuilder、SslChannelBuilder和PlaintextChannelBuilder。


Processor主线程还会调用org.apache.kafka.common.network.Selector#poll方法去从客户端读取数据，后续会调用kafka.network.Processor#processCompletedReceives去处理已经完成的数据读取并解析成RequestChannel.Request，然后把Request提交到RequstChannel。


当RequestHandler处理完成之后会生成Response并提交到对应Processor里面的答复队列里面。Processor主线程调用Processor#dequeueResponse从response队列获取response，然后针对不同的reponse类型调用不同的方法处理，比如SendResponse类型，会调用Processor#sendResponse发送response，最后调用KafkaChannel的send方法来发送给客户端。




<br/>

## 请求处理

KafkaRequestHandlerPool
Request会同一缓冲在RequestChannel里面，最后由一组KafkaRequestHandler消费后处理，KafkaRquestHandler会调用KafakApis#handle方法来处理请求。

![]({{ "/images/kafka-internal-request-handling.png" | absolute_url }})


<br/>

### RequestChannel

RequestChannel包含了一个阻塞队列，用的ArrayBlockingQueue，队列长度由参数queued.max.requests指定，默认是500。

在Processor#processCompletedReceives会调用RequestChannel#sendRequest把请求提交到阻塞队列，在KafkaRequestHandler里面调用RequestChannel#receiveRequest来消费请求并处理。

<br/>

### KafkaRequestHandlerPool

请求的具体处理在KafkaRequestHandler线程里面完成，这些handler线程由KafkaRequestHandlerPool创建，handler数量由num.io.threads控制，默认8。

<br/>

### KafkaApis

KafkaRequestHandler调用RequestChannel#receiveRequest从队列消费Request之后调用KafkaApis#handle处理，这个handle方法相当于是一个路由器，会根据ApiKeys来决定调用什么方法处理，比如最常见的：
```
case ApiKeys.PRODUCE => handleProduceRequest(request)
case ApiKeys.FETCH => handleFetchRequest(request)
case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
case ApiKeys.METADATA => handleTopicMetadataRequest(request)
case ApiKeys.LEADER_AND_ISR => handleLeaderAndIsrRequest(request)
```

<br/>

## ReplicaManager


ReplicaManager负责处理很多请求，包括kafka的核心请求处理：
- 处理Produce和Consume请求
- 处理Partition的主备同步

<br/>

### Produce和Consume请求处理



KafkaApis里面会调用ReplicaManager#appendRecords来处理ApiKeys.PRODUCE请求，调用ReplicaManager#readFromLocalLog来处理ApiKeys.FETCH的请求，fetch的请求不仅来自于客户端，还来自于broker的partition replication请求。

这两类请求最后都会调用Partition所对应的Log来处理，Log由LogManager负责管理，这就涉及到kafka日志的存储与offset是如何管理的，这里就不展开讲了。

<br/>

### Partition的主备同步

Kafka Partition的主备数据同步是依靠follower向leader发起fetch请求来完成的。

当KafkaHandler接受到ApiKeys.LEADER_AND_ISR类型的请求时，调用kReplicaManager#becomeLeaderOrFollower来处理，后续进一步分别调用makeLeaders和makeFollowers来处理leader partition和follower partition。

对于Leader Partition，会LogManager#getOrCreateLog来创建partition的log；对于FollowerPartition，会调用ReplicaFetcherThread#buildFetch为每个follower partition生成一个fetcher线程，这些线程会发送fetch请求到对应leader partition所在的broker并且把这些日志应用到本地。


<br/>


## AdminManager

AdminManager主要负责Topic管理相关的事务，比如topic创建与删除、partition创建等，很多时候需要连接到zookeeper去操作具体的事项。


<br/>

## FetchManager

FetchManager在接受到Fetch请求的时候生成FetchContext共后续使用。


<br/>


## 小结

这篇下来基本Kafka内部的整体框架已了然于胸，为进一步深入了解kafka内部机制作好了十足的准备。


-----------
注：kafka源码基于apache kafka 2.2.1 
