---
layout:     post
title:      HBase Coprocessor简介
date:       2020-05-17
summary:    简单介绍了 HBase Coprocessor
categories: hbase
published:  true
---






HBase Coprocessor引入的初衷是HBase数据操作的方式过于单一，比如读取只提供了单点GET和区间扫描，但是如果要做简单的服务端聚合就没有办法了，当然可以用Map Reduce，但是太重了，如果只用HBase Client代码并且想做一些轻量化的聚合操作，不但能节省客户端到服务端的带宽使用，而且可以使客户端的代码更简洁，这时候就引入了Coprocessor， 大概是在HBase 0.92版本引入的。
简单来讲，Coprocessor扩展了HBase，可以让用户在服务端执行自定义代码。


Coprocessor按照用法的不同，大体上可以分为两种
1. Observer: Observer提供了一种可以把自定义代码嵌入HBase核心代码里面的方法，比如你可以定义在Get之前执行某些代码，Get之后执行另外一些代码, 类似于传统数据库里面的trigger。
2. Endpoint：Endpoint就比较灵活了，自定义程度很高，需要在客户端主动调用预先定义好的接口。


<br/>

## Observer

Observer 官方定义好的接口和类如下图所示：



![]({{ "/images/hbase-coprocessor-observer.png" | absolute_url }})


可以看到按作用的范围， Observer有4种类型，分别是RegionObserver、 MasterObserver、WALObserver和RegionServerObserver, 以上这4个是接口，4个接口扩展了Coprocessor接口，并且每个接口都有4个对应的Base类，Base实现里面只是简单地把所有方法覆盖了一个空的实现，当用户要扩展的时候只需要扩展Base类里面感兴趣的方法就可以了。

从接口的方法名字其实就可以推断出他们的作用了：
1. MasterObserver: 大部分都是跟MasterService接口方法相关的操作，很多都是DDL类似的，比如可以在建表前和建表后执行一些代码。
2. RegionServerObserver: 针对RegionServer级别的操作嵌入一些自定义的代码，最典型的就是Region Merge操作(Region Merge是region server级别的操作), 可以让region server在merge之前和merge之后执行一些代码。
3. WALObserver: 顾名思义，主要是针对WAL写入和Roll操作的前后进行代码嵌入。
4. RegionObserver: 嵌入region级别操作的代码，这个接口里面的方法很多，毕竟大部分应用级别的扩展所涉及到的操作都是在region级别。





<br/>

**Observer的装载与运行**

Observer的装载与运行主要是借助于CoprocessorHost和CoprocessorEnvironment, 如下图所示，每种Observer都有对应的CoprocessorHost和CoprocessorEnvironment，图里面只列出了Master和Region对应的类



![]({{ "/images/hbase-coprocessor-coprocessor-host.png" | absolute_url }})
CoprocessorHost有点类似于对应Observer的代理，而CoprocessorEnvironment相当于对Observer(或者说Coprocessor)包裹了一层运行时的环境状态。


CoprocessorHost的初始化在各个实现所对应的组件里面，MasterCoprocessorHost是在HMaster的初始化中被构建, RegionServerCoprocessorHost是在HRegionServer的初始化中被构建，RegionCoprocessorHost是在HRegion的构造函数中被构建，RegionWALCoprocessorHost是在FSHLog的构建函数里面被构建。


当CoprocessorHost初始化的时候，会调用对用的load方法把各自相应的Observer和Coprocessor实现类load到内存里面.4个host都会调用CoprocessorHost#loadSystemCoprocessors来装载对应配置的类，配置项分别是
- hbase.coprocessor.wal.classes
- hbase.coprocessor.master.classes
- hbase.coprocessor.region.classes
- hbase.coprocessor.regionserver.classes


RegionCoprocessorHost除了这之外，还会load配置hbase.coprocessor.user.region.classes设置的类。另外它会从Region对应的table的属性里面获取coprocessor的配置，用的方法是RegionCoprocessorHost#getTableCoprocessorAttrsFromSchema，然后装载到内存里面。




Observer的运行是通过CoprocessorHost来完成的，以RegionObserver的prePut为例，当RegionServer接受到Put操作，即Mutate操作的时候，会调用HRegion#doPreMutationHook来处理preHoo操作，这里面会根据操作类型来决定调用哪种方法，Put操作对应的RegionCoprocessorHost里面的prePut操作，RegionCoprocessorHost#prePut会调用RegionCoprocessorHost#execOperation把这个prePut操作推送到所有装载的observer和coprocessor。



<br/>



**Observer当前的实现**


HBase当前版本Observer的主要一个实现是AccessController, 用于HBase各种操作的权限控制，它实现了RegionObserver、MasterObserver、RegionServerObserver等，毕竟是权限控制，涉及到的面比较广。


<br/>

## Endpoint

相对于Observer，Endpoint的自定义程度就相当高了，你可以定制任何想在服务器端执行的代码，因此，除了实现coprocessor接口，还需要定义一个自己的RPC接口，来实现数据在客户端和服务器端传输的方式。


Endpoint的装载与运行在服务器端跟Observer没什么两样，比如说有个enpoindt实现叫SecureBulkLoadEndpoint，这个是作用在region级别的endpoint，那么可以直接设置在配置项hbase.coprocessor.region.classes里面,

```
<property>
  <name>hbase.coprocessor.region.classes</name>
  <value>org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint</value>
</property>
```

当Region被初始化的时候自动会通过RegionCoprocessorHost把这个类装载到内存里面。




与Endpoint差别比较大的是客户端代码的执行，比如执行SecureBulkLoadEndpoint#prepareBulkLoad需要在客户端运行以下代码:
方法

```java
CoprocessorRpcChannel channel = table.coprocessorService(HConstants.EMPTY_START_ROW);
SecureBulkLoadProtos.SecureBulkLoadService instance =
    ProtobufUtil.newServiceStub(SecureBulkLoadProtos.SecureBulkLoadService.class, channel);
ServerRpcController controller = new ServerRpcController();
BlockingRpcCallback<SecureBulkLoadProtos.PrepareBulkLoadResponse> rpcCallback =
    new BlockingRpcCallback<SecureBulkLoadProtos.PrepareBulkLoadResponse>();
SecureBulkLoadProtos.PrepareBulkLoadRequest request =
    SecureBulkLoadProtos.PrepareBulkLoadRequest.newBuilder()
    .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();

instance.prepareBulkLoad(controller,
    request,
    rpcCallback);

SecureBulkLoadProtos.PrepareBulkLoadResponse response = rpcCallback.get();
```


这里首先调用table的coprocessService方法构建一个channel，然后用channel去初始化自定义的RPC服务，然后构建对应的RPC request，然后调用这个服务的prepareBulkLoad方法执行远程调用。


<br/>

## 小结

Coprocessor主要是为了提供一种可以扩展HBase的方法，可以利用已定义好的Observer扩展HBase核心操作的代码，也可以重新另起炉灶采用Ednpoint的方式来完成自定义化程序较高的操作，我们所熟知的Apache Phoenix其实就重度使用了Coprocessor。




