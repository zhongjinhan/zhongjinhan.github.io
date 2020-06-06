---
layout:     post
title:      HBase Region分配详解
date:       2020-06-06
summary:    详细到源码级别的解释
categories: hbase
published:  true
---

在HBase里面，表会按Rowkey水平分成多个Region，Master就是基于这个粒度来管理HBase里面的数据。Region的分配指的是Master按一定的规则把一
个Region分配到一台指定的HBase RegionServer上，Region Server打开并初始化这个Region之后客户端才能访问这个Region。

Region分配发生各种不同的情况下，比如当整个集群重启的时候，Master会对所有的Region进行分配；当一个Region Server崩掉的时候，
Master会把这个RegionSever上的所有Region重新分配到其他在线Region上面；当各个RegionServer在Region Merge和split之后region数量
不均衡的时候，Master里面的Balancer会对一些Region进行重新分配。

这里选其中一个场景，就是当集群重新启动的时候，来详细看下Region是Master如何被分配的。


<br/>

## Region状态和分配整体流程




Master用RegionStates来跟踪所有Region的状态，当Master构建AssignmentManager的时候会构建一个空的RegionStates，
RegionStates会一直存在于Master内存里面进程里面直到Master关闭。

我们这里就以RegionStates里面的状态变化来详细展开看下Region的分配流程，假设现在的场景是整个集群重启，并且RegionServer都已经启动完成了，Master已经完成对hbase:meta的初始化和分配了，那么接下来就要
对用户Region进行分配，从RegionStates初始化一直到Region分配完成，region状态的变化如下：


![]({{ "/images/hbase-region-assign-state.png" | absolute_url }})


那么整体流程可以按重要的状态点分成如下几个阶段：
1. 从RegionStates初始化到状态PENDING_OPEN
2. 从状态PENDING_OPEN到OPENING
3. 从状态OPENING到OPENED



可以用如下的时序状态图来描述整个流程：


![]({{ "/images/hbase-region-assign-sequence.png" | absolute_url }})

从上面这图可以看到除了RegionStates的状态以外，在Region状态转变过程中zookeeper里面（默认路径/hbase/region-in-transition)
也保存着Region的部分状态，这个主要是RegionServer在改变某个Region状态的时候用来通知Master。


下面详细描述下各个阶段.


<br>

## 初始化到状态PENDING_OPEN

当一个Master成功被选举成为ActiveMaster的时候会调用HMaster#finishActiveMasterInitialization来进行成为ActiveMaster的一些必要的初始化工作。
在完成各种初始化工作并且成功上线hbase:meta之后，会调用AssignmentManager#joinCluster进行用户Region的assign工作，这就是region assign的入口，大部分的工作也都是在AssignmentManager完成。


<br/>

### 1.1 初始化RegionStates并置成OFFLINE

首先会对hbase:meta进行全表扫描，获取所有已有region信息用来构建Master内存里面regionStates, 构建之后并把所有的region状态设置成OFFLINE，这是在AssignmentManager#rebuildUserRegions里面完成的。

<br/>

### 1.2 把所有region状态置成CLOSED

接着就调用AssignmentManager#processDeadServersAndRegionsInTransition，这里首先会作一个是否是failover的判断的，也就是判断这次ActiveMaster的初始化是否是standby master变成active master，因为我们这里的假设是集群重启，所以failover是false。


然后继续调用regionStates.closeAllUserRegions过后，regionStates里面的状态除了meta全部变成close，最后清理zookeeper默认路径/hbase/region-in-transition下面的子znode，并对子znode进行watch设置，这样就能监听子znode的各种变化并调用对应的方法，比如AssignmentManager#nodeCreated、AssignmentManager#nodeDataChanged和AssignmentManager#nodeDeleted。 




<br/>

### 1.3 在zk为所有region创建RegionTransition并置成M_ZK_REGION_OFFLINE

到这里为止分配前期的准备工作基本已经完成了，然后就调用AssignmentManager#assignAllUserRegions继而调用AssignmentManager#assign(Map)进入user region的分配工作，这里首先还准备一个分配计划，就是这么一堆region是如何跟一台或者多台region server对应起来的，这里调用的是Balancer#retainAssignment来生成分配计划，从名字也可以看的出来，因为这里是集群重启，这个方法基本上分配方式就是跟重启前保持一直。Balancer本身还有一个定时调度任务定期执行这个region平衡的工作，主要目的是保证各个region server之间的负载均衡，因为在region split或者merge之后可能会产生region在regionserver之间的分配不均衡的问题。


在Balancer#retainAssignment里面，首先会检查是否有master regonin server， 如果有master region server，会优先处理这上面的region，一般来说是不会这么有的。如果region server只有一台，全部assign到这台region server。当有多台region server的时候，一般情况下，region和server的assign关系主要由下面这段逻辑来决定
1. 如果原来的region server对应的host还在，那么直接分配到这个服务器上
2. 如果没了，那么随机分配到现有的region server。
3. 还有可能存在原来region server所在的host上面启动了多个region server的情况，这个时候首先会检查之前的region server（host+port+starttime)是否还在，如果在则assign到region server上面，如果不在，则检查有没有跟之前region server同端口的region server，也就是之前的region server重启了，如果有就assing；否则，随机assign到这个host上面的region server。




分配计划产生之后会进入方法AssignmentManager#assign(int,int,String,Map)方法，在这里面会检查分配计划里
面有几个region server，如果只有一个那么直接调用AssignmentManager#assign(ServerName,List)方法；否则生成一个org.apache.hadoop.hbase.master.GeneralBulkAssigner，并调用其bulkAssign方法来对region server进行逐个分派，最后还是会调用AssignmentManager#assign(ServerName,List)对当个region server进行处理。



在AssignmentManager#assign(ServerName,List)里面, 首先会遍历所有region，这里的AssignmentManager#forceRegionStateToOffline方法返回的其实还是CLOSE状态，接着调用AssignmentManager#asyncSetOfflineInZooKeeper为每个region在zookeeper的/hbase/region-in-transition下面创建子节点RegionTransition，事件类型为EventType.M_ZK_REGION_OFFLINE。


<br/>

### 1.4 把所有region置成PENDING_OPEN状态

在正式向region server发送RPC请求之前，会调用如下代码把所有的用户region状态改成PENDING_OPEN

```java        
for (RegionState state: states) {
  HRegionInfo region = state.getRegion();
  String encodedRegionName = region.getEncodedName();
  Integer nodeVersion = offlineNodesVersions.get(encodedRegionName);
  if (useZKForAssignment && (nodeVersion == null || nodeVersion == -1)) {
    LOG.warn("failed to offline in zookeeper: " + region);
    failedToOpenRegions.add(region); // assign individually later
    Lock lock = locks.remove(encodedRegionName);
    lock.unlock();
  } else {
    regionStates.updateRegionState(
      region, State.PENDING_OPEN, destination);
    List<ServerName> favoredNodes = ServerName.EMPTY_SERVER_LIST;
    if (this.shouldAssignRegionsWithFavoredNodes) {
      favoredNodes = ((FavoredNodeLoadBalancer)this.balancer).getFavoredNodes(region);
    }
    regionOpenInfos.add(new Triple<HRegionInfo, Integer,  List<ServerName>>(
      region, nodeVersion, favoredNodes));
  }
}
```



<br/>

## 状态PENDING_OPEN到OPENING

到此为止，那么Master端的工作完成的差不多了，就剩下最后向Region Server发送RPC调用了。


<br/>

### 2.1 向RegionServer发起OpenRegion的RPC调用

还是在AssignmentManager#assign(ServerName,List)里面，最后会调用ServerManager#sendRegionOpen方法，在ServerManager里面继而调用AdminProtos.AdminService.BlockingInterface#openRegion，regionsever端对应的RPC处理方法是RSRpcServices#openRegion。



<br/>

### 2.2 ZK RegionTransition状态更新成OPENING

RegionServer接收到Master发来的请求之后，Handler线程会调用RSRpcServices#openRegion来遍历处理request的所有region，主要步骤如下：

1. 检查在线region是否包含request里面的region
2. 如果没问题，继续open这个region
3. 调用regionServer.tableDescriptors.get(region.getTable());获取HTableDescriptor, 这个方法会从hdfs上面去读取table的信息。
4. 把region放到regionServer.regionsInTransitionInRS map里面
5. 提交OpenRegionHandler  - regionServer.service.submit(new OpenRegionHandler(regionServer, regionServer, region, htd, masterSystemTime, coordination, ord));
6. 一切顺利的话，返回RegionOpeningState.OPENE


因为主要工作已经提交到OpenRegionHandler去做了，这个RPC会直接返回，master启动主线程接收到所有RPC调用返回之后，调用AssignmentManager#waitForAssignment方法等待直到所有region分配完成。



接下来主要就是OpenRegionHandler里面的工作了，主要是在方法OpenRegionHandler#process里面。在完成基本的检查之后，就会调用调用OpenRegionCoordination#transitionFromOfflineToOpening把zk上面的RegionTransition状态从EventType.M_ZK_REGION_OFFLINE更新到EventType.RS_ZK_REGION_OPENING。

因为Master之前启动了监听zk，所以zk上面状态的更新会触发Master里面的
AssignmentManager#nodeDataChanged方法，最后会调用AssignmentManager#handleRegion
来处理这个变化，对于RS_ZK_REGION_OPENING的处理很简单，只是把把RegionState更新成OPENIN。

到此为止，意味着region已经处于打开过程中了。




<br/>

## 状态OPENING到OPENED


整个打开过程当然是在RegionServer里面完成的，Master那边一直在等待状态的更新。


<br/>

### 3.1 打开并初始化Region

这里主要是在Region#openHRegion方法里面完成，然后经过一系列openRegion之后会调用HRegion#newHRegion, 里面调用构造函数构建了一个HRegion。构建之后会调用调用HRegion#openHRegion(CancelableProgressable)真正打开Region，这里的Region打开过程主要有几个步骤

1. checkClassLoading: 里面包含RegionCoprocessorHost#testTableCoprocessorAttrs的调用，该方法会尝试去load table attribute里面设置的 coprocessor对应的class。
2. initialize: 初始化hregion
3. 往WAL写入open marker: 调用writeRegionOpenMarker(wal, openSeqNum);

在初始化HRegion过程中，首先会初始化Store，然后开始调用org.apache.hadoop.hbase.regionserver.HRegion#replayRecoveredEdits重放WAL。这里的WAL是在RegionServer异常退出之后Master会启动LogSplit过程把原有的WAL按region切割好，最后会在repalyRecoveredEdit里面被读取处理。具体流程：
1. 调用 org.apache.hadoop.hbase.wal.WALSplitter#getSplitEditFilesSorted获取edit file list, 
2. 首先遍历该Region所对应的所有store，然后获取最小的那个sequence id，当获取到这个sequence id之后就可以确定小于这个sequence id的数据都已经从memstore flush到hfile，也就是说，如果split之后的reover log如果它的最大sequqnceid是小于这个sequence ID，那么这个recover log直接可以忽略了。
3. 没有被过滤调的recover log文件会调用HRegion#replayRecoveredEdits被逐个处理。在ReplayRecoveredEdit里面，首先为log文件创建一个ProtobufLogReader，接着逐个读取edit并处理，然后针对每个edit，先运行RegionCoprocessorHost#preWALRestore，接着把WALEdit里面的所有cell写入对应的store，完成之后调用会尝试flush store，最后再调用RegionCoprocessorHost#postWALRestore


<br/>

### 3.2 ZK RegionTransition状态更新成RS_ZK_REGION_OPENED

在Region成功被打开之后，接着在OpenRegionHandler#process里面会调用OpenRegionHandler#updateMeta, 更新region位置，如果是hbase:meta更新hbase里面的信息，如果是其他一般region，更新habse:meta表。

最后调用OpenRegionCoordination#transitionToOpened把zk里面的状态从EventType.RS_ZK_REGION_OPENING过渡到EventType.RS_ZK_REGION_OPENED


当Master监听到状态变化的时候还是会调用AssignmentManager#handleRegion来处理，针对RS_ZK_REGION_OPENED状态，会做如下处理
1. 调用RegionStates#transitionOpenFromPendingOpenOrOpeningOnServer检查目前的state是否是opening或者pendingOpen，然后把regionState改成State.OPEN
2. 调用Master端的OpenedRegionHandler#process去删除处于RS_ZK_REGION_OPENED状态的zk node，表示已经region分配已经完成


而这个删除的动作会触发AssignmentManager#nodeDeleted，里面会提交一个RegionRunnable任务，调用AssignmentManager#regionOnline修改Master里面的一些状态，这些状态的变更最终会导致Master启动线程的AssignmentManager#waitForAssignment方法的等待结束，那么整个分配流程就完成了。








## 小结


Master启动过程中的Region分配是HBase集群初始化的一部分非常重要的工作，因为没有这个分配过程，RegionServer相当于就是空的，没办法给客户端提供任何服务。了解这整个过程，能够让你非常快速地定位到集群启动过程中遇到的各种问题，继而能够迅速解决问题。



-------
注：hbase代码基于cloudera hbase, 版本cdh5-1.2.0_5.13.0
























