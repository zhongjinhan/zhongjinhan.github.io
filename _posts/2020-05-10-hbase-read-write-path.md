---
layout:     post
title:      HBase数据写入与读取路径
date:       2020-05-10
summary:    就是随便介绍介绍
categories: hbase
published:  true
---



如果你想深入理解一个数据存储系统，第一步要做的事情就是了解它的数据写入与读取路径，毕竟数据的写入与读取是存储系统最基本也是最核心的功能，其他模块都是服务于这个核心功能。这里就来看看HBase的数据写入与读取路径。



<br/>


## HBase数据写入路径

先来看一段简单的客户端写入demo代码

```java
public static void writeData() throws IOException{

    String tableName = "test_ns:test_table";
    String defaultColumnFamily = "test_cf";
    String key = "test_k";

    Configuration conf = HBaseConfiguration.create();
    Connection conn = ConnectionFactory.createConnection(conf);

    Table table = conn.getTable(TableName.valueOf(tableName));

    Put p = new Put(Bytes.toBytes(key));
    p.addColumn(Bytes.toBytes(defaultColumnFamily), Bytes.toBytes("test_col"), Bytes.toBytes("test!"));
    
    table.put(p);
    conn.close();
}
```
这段代码先从获取了一个到集群的Connection，然后在Connection里面构建一张HTable，接着构建一个Put操作，然后调用HTable#put方法更新数据，这个HTable#put操作就是数据写入路径的入口。

写入操作是通过RPC调用完成的，所以这里分成客户端和服务端两部分来描述，先看下客户端部分。

<br/>
### 数据写入路径客户端部分



从HTable#put开始的主要方法调用栈如下：


```
|-----------------------------------------------------------------------------------------------------------------|
|1 -  HTable#put(Put)                                                                                            -|
|-----------------------------------------------------------------------------------------------------------------|
|1.1 -      -> HTable#getBufferedMutator                                                                         -|
|-----------------------------------------------------------------------------------------------------------------|
|1.1.1 -          -> HConnectionImplementation#getBufferedMutator                                                -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2 -      -> BufferedMutatorImpl#mutate                                                                        -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1 -          -> BufferedMutatorImpl#backgroundFlushCommits                                                  -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1 -              -> AsyncProcess#submit                                                                   -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.1 -                  -> ConnectionManager.HConnectionImplementation#locateRegion                        -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.1.1 -                      -> ConnectionManager.HConnectionImplementation#locateRegionInMeta            -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.1.1.1 -                          -> ConnectionManager.HConnectionImplementation#getCachedLocation       -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.1.1.2 -                          -> ConnectionManager.HConnectionImplementation#locateRegion            -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.1.1.2.1 -                              -> ZooKeeperRegistry#getMetaRegionLocation                       -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.1.1.2.1.1 -                                  -> MetaTableLocator#blockUntilAvailable                    -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.1.1.3 -                          -> ConnectionManager.HConnectionImplementation#getClient               -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.1.1.4 -                          -> ProtobufUtil#getRowOrBefore                                         -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.1.1.5 -                          -> MetaTableAccessor#getRegionLocations                                -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.1.1.6 -                          -> ConnectionManager.HConnectionImplementation#cacheLocation           -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.2 -                  -> AsyncProcess#submitMultiActions                                                 -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.2.1 -                      -> AsyncProcess#createAsyncRequestFuture                                     -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.2.2 -                      -> AsyncProcess.AsyncRequestFutureImpl#sendMultiAction                       -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.2.2.1 -                          -> AsyncProcess.AsyncRequestFutureImpl#getNewMultiActionRunnable       -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.2.2.2 -                          -> AsyncProcess.AsyncRequestFutureImpl.SingleServerRequestRunnable#run -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.2.2.2.1 -                              -> AsyncProcess#createCallable                                   -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.2.2.2.2 -                              -> AsyncProcess#createCaller                                     -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.2.2.2.3 -                              -> RpcRetryingCaller#callWithoutRetries                          -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.2.2.2.3.1 -                                  -> MultiServerCallable#prepare                             -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.2.2.2.3.1.1 -                                      -> RegionServerCallable#setStub                      -|
|-----------------------------------------------------------------------------------------------------------------|
|1.2.1.1.2.2.2.3.2 -                                  -> MultiServerCallable#call                                -|
|-----------------------------------------------------------------------------------------------------------------|
```

这一系列的调用主要做了两项工作：
- 定位Region
- 构建RPC调用并发送到对应的RegionServer

具体步骤可以用如下图来补充描述：


![]({{ "/images/hbase-path-write-client.png" | absolute_url }})




1. 尝试从Cache里面获取region信息：客户端会把已经获取到的region信息存放在cache里面，cache的具体实现是MetaCache，等下次请求的时候会先在cache里面查找，如果有的话则直接跳到第四步去提交RPC调用。这里用HConnectionImplementation#getCachedLocation方法来获取Cache里面的location。
2. 从Zookeeper获取meta对应的region信息：集群启动的时候会在zookeeper里面写入hbase:meta表对应的region信息，zk里面的默认位置是/hbase/meta-region-server。这里通过ZooKeeperRegistry#getMetaRegionLocation方法读取zk /hbase/meta-region-server节点的信息从而获取到region信息。

3. 从meta表里面获取更新数据的region信息：hbase:meta存放了所有region的信息，这张表每一行存放一个region，row key由tableName+startKey+regionId+encodedRegionName组成，column family只有一个，叫info, column有regioninfo、server等等。这里通过调用ProtobufUtil#getRowOrBefore 方法来查询需要更新的row对应的region信息。

4. 提交RPC任务到线程池: 在调用HConnectionImplementation#getBufferedMutator创建BufferedMutatorImp的时候会创建一个ExecutorPool，后面在调用AsyncProcess.AsyncRequestFutureImpl#getNewMultiActionRunnable生成SingleServerRequestRunnable后，会把这个Runnable提交到pool里面运行。

5. 发送RPC调用到目标RegionServer：SingleServerRequestRunnable#run里面最终会调用MultiServerCallable#call来构建ClientProtos.MultiRequest,然后通过ClientService.BlockingInterface#multi方法发送RPC调用到目标RegionServer。



<br/>

### 数据写入RPC接口


从0.94开始，HBase的RPC接口和消息的定义都是用ProtocalBuffer来完成，数据写入的接口也一样，它的定义在Client.proto里面，MutliRequest、MultiResponse以及multi方法的定义如下:

```
message MultiRequest {
  repeated RegionAction regionAction = 1;
  optional uint64 nonceGroup = 2;
  optional Condition condition = 3;
}

message MultiResponse {
  repeated RegionActionResult regionActionResult = 1;
  optional bool processed = 2;
}

service ClientService {
...
  rpc Multi(MultiRequest)
    returns(MultiResponse);
}

```




<br/>

### 数据写入路径服务端部分

multi方法在Region Server端的实现是RSRpcServices#multi。当RegionServer接收到客户端发来的multi远程调用的时候，会在Handler里面调用RSRpcServices#multi方法来处理这个请求，下面是multi方法的调用链路:





```
|-------------------------------------------------------------------------------------------|
|1            -  RSRpcServices#multi                                                       -|
|-------------------------------------------------------------------------------------------|
|1.1          -      -> RSRpcServices#getRegion                                            -|
|-------------------------------------------------------------------------------------------|
|1.2          -      -> RSRpcServices#getQuotaManager                                      -|
|-------------------------------------------------------------------------------------------|
|1.3          -      -> RSRpcServices#doNonAtomicRegionMutation                            -|
|-------------------------------------------------------------------------------------------|
|1.3.1        -          -> RSRpcServices#doBatchOp                                        -|
|-------------------------------------------------------------------------------------------|
|1.3.1.1      -              -> OperationQuota#addMutation                                 -|
|-------------------------------------------------------------------------------------------|
|1.3.1.2      -              -> MemStoreFlusher#reclaimMemStoreMemory                      -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3      -              -> Region#batchMutate                                         -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.1    -                  -> HRegion#startRegionOperation                           -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.2    -                  -> HRegion#checkReadOnly                                  -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.3    -                  -> HRegion#checkResources                                 -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4    -                  -> HRegion#doMiniBatchMutation                            -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.1  -                      -> HRegion#checkFamilies                              -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.2  -                      -> HRegion#checkTimestamps                            -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.3  -                      -> HRegion#checkRow                                   -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.4  -                      -> HRegion#getRowLock(byte[], boolean)                -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.5  -                      -> HRegion#updateCellTimestamps                       -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.6  -                      -> HRegion#rewriteCellTags                            -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.7  -                      -> HRegion#lock(java.util.concurrent.locks.Lock, int) -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.8  -                      -> HRegion#getEffectiveDurability                     -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.9  -                      -> HLogKey#HLogKey                                    -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.10 -                      -> WAL#append                                         -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.11 -                      -> HRegion#applyFamilyMapToMemstore                   -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.12 -                      -> ReentrantReadWriteLock.ReadLock#unlock             -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.13 -                      -> HRegion#syncOrDefer                                -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.14 -                      -> RegionCoprocessorHost#postBatchMutate              -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.15 -                      -> MultiVersionConcurrencyControl#completeAndWait     -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.4.16 -                      -> RegionCoprocessorHost#postPut                      -|
|-------------------------------------------------------------------------------------------|
|1.3.1.3.5    -                  -> HRegion#requestFlush                                   -|
|-------------------------------------------------------------------------------------------|
```

可以看到首先会通过RSRpcServices#getRegion方法从这个region server的online region里面获取到对应的Region，然后调用HRegion的一系列方法来处理这个请求,而最主要的写入处理都在HRegion#doMiniBatchMutation方法里面，处理步骤如下图所示:


![]({{ "/images/hbase-path-write-server.png" | absolute_url }})

1. 加锁。对本次mutation里面所有的row做一些检查然后加上锁，检查包括调用checkFamilies对column family的检查，调用checkTimestamps对自定义时间戳的检查，调用checkRow对rowkey进行范围有效检查。检查完成之后调用HRegion#getRowLock尝试获取读锁。
2. 构建WriteAheadLog Edit。在构建Edit之前会遍历所有mutation, 分别调用updateCellTimestamps和prepareDeleteTimestamps来更新put和delete操作的时间戳。然后遍历所有mutation，先检查Duration支持性等级，默认是SYNC_WAL, 如果设置的SKIP_WAL，则直接忽略构建WAL；否则先合并来自coprocessor的WALEdit， 然后再调用addFamilyMapToWALEdit追加本次变更内容到WALEdit。最后用region名称、表名等等构建一个WALKey（具体实现是HLogKey), 然后把WALKey、WALEdit追加到WAL。完成之后从WALKey里面获取mvcc number。
3. 写入memstore。遍历本次操作集合里面的所有mutation对应的familymap，然后调用applyFamilyMapToMemstore方法把内容写入到各自family对应的memstore
4. 解锁。把第一步里面获取的读锁释放掉。
5. sync WAL。调用HRegion#syncOrDefer把第三、四步构建的WAL Edit刷到磁盘上面，并且调用MultiVersionConcurrencyControl#completeAndWait来推进mvcc，这个动作会使本次更新内容可查询。








<br/>
**HRegion#doMiniBatchMutation**

从调用链路里面可以看到主要的工作基本上是在HRegion#doMiniBatchMutation方法完成，从代码注视里面也可以看出来总共分为9个步骤

1. 加锁。对本次mutation里面所有的row做一些检查然后加上锁，检查包括调用checkFamilies对column family的检查，调用checkTimestamps对自定义时间戳的检查，调用checkRow对rowkey进行范围有效检查。检查完成之后调用HRegion#getRowLock尝试获取读锁。
2. 更新LATEST_TIMESTAMP。遍历本次操作集合里面的所有mutation, 分别调用updateCellTimestamps和prepareDeleteTimestamps来更新put和delete操作的时间戳
3. 构建WriteAheadLog Edit。 遍历本次操作集合里面的所有mutation，先检查Duration支持性等级，默认是SYNC_WAL, 如果设置的SKIP_WAL，则直接忽略构建WAL；否则先合并来自coprocessor的WALEdit， 然后再调用addFamilyMapToWALEdit追加本次变更内容到WALEdit。
4. 追加WALEdit到WAL。用region名称、表名等等构建一个WALKey（具体实现是HLogKey), 然后把WALKey、WALEdit追加到WAL。完成之后从WALKey里面获取mvcc number。
5. 写入memstore。遍历本次操作集合里面的所有mutation对应的familymap，然后调用applyFamilyMapToMemstore方法把内容写入到各自family对应的memstore
6. 解锁。把第一步里面获取的读锁释放掉。
7. sync WAL。调用HRegion#syncOrDefer把第三、四步构建的WAL Edit刷到磁盘上面。
8. 推进MVCC。调用MultiVersionConcurrencyControl#completeAndWait来推进mvcc，这个动作会使本次更新内容可查询。
9. 运行coprocessor里面post hook。跟HRegion#doPreMutationHook方法里面的pre hook相对地，遍历所有mutation调用RegionCoprocessorHost#postPut或者postDelete方法。






<br/>
## HBase数据读取路径

同样地我们也以一个数据读取Demo程序开始:

```java
public static void readData(String tableName) throws IOException{
    Configuration conf = HBaseConfiguration.create();
    Connection conn = ConnectionFactory.createConnection(conf);
    Table table = conn.getTable(TableName.valueOf(tableName));
    String defaultColumnFamily = "test_cf";
    String key = "key";
    Get g = new Get(Bytes.toBytes(key));
    Result r = table.get(g);
    byte[] v = r.getValue(Bytes.toBytes(defaultColumnFamily),
            Bytes.toBytes("test_col"));
    double vd =  Bytes.toDouble(v);
    System.out.println(vd);
    conn.close();
}
```


这段代码跟数据写入相比不同的地方是把Put替换成了Get，也就是调用了HTable#get方法。


<br/>


### 数据读取路径客户端部分

数据读取路径在客户端这部分的流程跟数据写入路径非常相似，也是先去定位region然后发送RPC调用。不同的部分在于数据读取（GET）没有把操作提交到线程池，所以调用链路没这么长，基本都在HTable#get方法里面实现了，这方法里面的部分比较核心代码如下：

```java
private Result get(Get get, final boolean checkExistenceOnly) throws IOException {
...

  if (get.getConsistency() == Consistency.STRONG) {
    // Good old call.
    final Get getReq = get;
    RegionServerCallable<Result> callable = new RegionServerCallable<Result>(this.connection,
        getName(), get.getRow()) {
      @Override
      public Result call(int callTimeout) throws IOException {
        ClientProtos.GetRequest request =
          RequestConverter.buildGetRequest(getLocation().getRegionInfo().getRegionName(), getReq);
        PayloadCarryingRpcController controller = rpcControllerFactory.newController();
        controller.setPriority(tableName);
        controller.setCallTimeout(callTimeout);
        try {
          ClientProtos.GetResponse response = getStub().get(controller, request);
          if (response == null) return null;
          return ProtobufUtil.toResult(response.getResult());
        } catch (ServiceException se) {
          throw ProtobufUtil.getRemoteException(se);
        }
      }
    };
    return rpcCallerFactory.<Result>newCaller(rpcTimeout).callWithRetries(callable,
        this.operationTimeout);
  }
...
}
```


可以看到get直接在这个方法构建了一个RegionServerCallable, 接着就调用RpcRetryingCaller#callWithoutRetries来执行这个PRC call。

另外region定位是在RegionServerCallable#prepare方法通过调用Connection#getRegionLocator获取，最后还是会调用HConnectionImplementation#locateRegion来获取regoin信息。


当RegionServerCallable#call被执行的时候，会先构建一个ClientProtos.GetRequest，接着获取Stub然后执行Stub里面实现的get方法，Stub会把调用推送给构建时候传入的RpcChannel, 最终会把调用推送给RpcClient#call, 该方法返回的是ClientProtos.GetResponse.

<br/>

### 数据读取RPC接口

跟数据写入一样，数据读取的RPC接口与消息定义也是在Client.proto

```
message GetRequest {
  required RegionSpecifier region = 1;
  required Get get = 2;
}

message GetResponse {
  optional Result result = 1;
}

service ClientService {
  rpc Get(GetRequest)
    returns(GetResponse);
}
```


<br/>

### 数据读取路径服务端部分

当ClientProtos.GetRequest请求被region server接收之后会调用RSRpcServices#get来处理，调用链条如下:



```
|-------------------------------------------------------------------------------|
|1           -  RSRpcServices#get                                              -|
|-------------------------------------------------------------------------------|
|1.1         -      -> RSRpcServices#getRegion                                 -|
|-------------------------------------------------------------------------------|
|1.2         -      -> HRegion#get                                             -|
|-------------------------------------------------------------------------------|
|1.2.1       -          -> RegionCoprocessorHost#preGet                        -|
|-------------------------------------------------------------------------------|
|1.2.2       -          -> Scan#Scan                                           -|
|-------------------------------------------------------------------------------|
|1.2.3       -          -> HRegion#getScanner                                  -|
|-------------------------------------------------------------------------------|
|1.2.3.1     -              -> HRegion#instantiateRegionScanner                -|
|-------------------------------------------------------------------------------|
|1.2.3.1.1   -                  -> HRegion.RegionScannerImpl#RegionScannerImpl -|
|-------------------------------------------------------------------------------|
|1.2.3.1.2   -                  -> HStore#getScanner                           -|
|-------------------------------------------------------------------------------|
|1.2.3.1.2.1 -                      -> HStore#createScanner                    -|
|-------------------------------------------------------------------------------|
|1.2.3.1.3   -                  -> HRegion.RegionScannerImpl#initializeKVHeap  -|
|-------------------------------------------------------------------------------|
|1.2.4       -          -> HRegion.RegionScannerImpl#next                      -|
|-------------------------------------------------------------------------------|
|1.2.4.1     -              -> HRegion.RegionScannerImpl#nextRaw               -|
|-------------------------------------------------------------------------------|
|1.2.4.1.1   -                  -> HRegion.RegionScannerImpl#nextInternal      -|
|-------------------------------------------------------------------------------|
```


这一些列的调用主要做了如下几件事情：

- 用Get构建一个Scan
- 用Scan构建Scanner, Scanner的实现是RegionScannerImpl
- 调用构建好的RegionScannerImpl的next方法获取扫描结果

这里面最核心的一个组件是RegionScannerImpl, 可以下图来加以辅助理解这个类


![]({{ "/images/hbase-path-read-server.png" | absolute_url }})

RegionScannerImpl里面包含了一个KeyValueHeap,KeyValueHeap的核心数据结构是一个优先级队列，队列里面存放了一组StoreScanner，每个StoreScanner对应于该Region的一个Column Family。

每一个StoreScanner也包含了一个KeyValueHeap，里面存放了一个MemStoreScanner和若干个StoreFileScanner，这两种Scanner是实际负责扫描数据的。MemStoreScanner负责扫描该Column Family在MemStore里面的数据，StoreFileScanner负责扫描一个StoreFile里面的数据，StoreFile会有多个，对应的StoreFileScanner也会有多个。在StoreScanner构建过程当中，还会通过调用StoreScanner#selectScannersFrom过滤掉不在本次扫描范围之内Scanner。


KeyValueHeap里面的优先级队列的优先级别是靠传入的comparator来指定，这个是通过调用HStore#getComparator来获取，实现是KeyValue.KVComparator。



当HRegion.RegionScannerImpl#next被调用用来检索扫描结果的时候，首先会获取RegionScanner里面的优先级队列处于队列头的StoreScanner, 继而会调用StoreScanner#peak方法从它的优先级队列里面取出位于队列头的MemStoreScanner或者FileStoreScanner, 然后调用MemStoreScanner或FileStoreScanner里面的peek方法获取所需要的值。


<br/>

## 小结


总的来说不管客户端发起的是读还是写操作，首先都会找到Row所对应的Region，会先尝试在本地Cache里面找，如果没有则会先去zookeeper找到hbase:meta表所在的region，然后在meta region查到Row所对应的Region，拿到region的信息之后会发送相对应的Rpc请求到对应的RegionServer。

在region server端，如果是写操作则先构建WALEdit并追加到region Sever的WAL里面，然后把变更写入MemStore，完了之后sync WAL保证数据flush到HDFS；如果是读操作，则构建Scanner去扫描memstore和storefile。



-------
注：hbase代码基于cloudera hbase, 版本cdh5-1.2.0_5.13.0
