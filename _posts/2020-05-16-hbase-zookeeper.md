---
layout:     post
title:      HBase如何使用Zookeeper
date:       2020-05-16
summary:    介绍了Zookeeper作为一个协调者是如何在HBase中起作用的。
categories: hbase
published:  true
---



## Zookeeper 简介

Zookeeper主要为分布式系统提供高可用的配置服务、命名服务和同步服务，ZooKeeper Atomic Broadcast（ZAB)协议是该框架的核心。Zookeeper把数据存放在一个层次命名空间里面，类似于文件系统，它跟文件系统的区别在于一个节点既可以包含数据也可以有子节点，并且path引用必须是绝对路径，没有相对路径。
命名空间里面的节点叫做ZNode，里面维护了一个stat structure，包含版本信息和变动相关信息，一般来说客户端主要是对这个对象进行操作。

在Zookeeper里面还可以创建ephemeral znode用来监控一些进程，emphemeral节点在session结束后会被删除。另外它没有子节点。

<br/>

**Zookeeper基本操作**

客户端的常用操作如下:
1. 对Znode节点的操作，比如创建、删除、设置数据，查询数据等。
2. 设置Watch监听某些znode的变化，这些变化包括节点的创建、删除、变更、子节点的变更等。

<br/>
**Zk Client**

Zookeeper提供一个交互式的命令行工具zkCli.sh，可以用如下方式连接到zookeper集群

```
zkCli.sh -server 127.0.0.1:2181
```

然后用类似操作文件夹的命令来操作zk的znode，

```sh
#创建znode
create /t1
#给znode写入数据
set /t1 xxx
#查看znode数据
get /t1 
#删除znode
rmr /t1
#查看acl
getAcl /t1
```

<br/>
**Zookeeper 4字母管理**

4字母管理指的是一系列4个字母组成的指令，在3.5之前默认可以通过nc或者telnet向zk服务对应的端口默认2181发送，zk服务会返回对应的信息，非常有效的工具。
比如
```shell
echo stat | nc 127.0.0.1 2181
```

在3.5.0，增加了AdminServer，可以直接通过内置的web服务查看对应的4lw指令的信息，而这时候telnet和nc对应的指令都被屏蔽了，需要手工放入到白名单里面。
```
http://localhost:8080/commands
```


几个比较游有用的命令入下:

- stat: 查看服务器状态，包括客户端的连接、数据的接受与发送、总连接数量、延迟数据等等。
- conf: 查看目标服务器的配置信息, 包括port、dataDir、dataLogDir等
- cons: 查看连接到服务器的所有连接的详细情况
- mntr: 查看目标服务器的一些统计信息，包括znode的数量、watch数量、ephemeral数量等。




<br/>


**Zk Client API**

Zookeeper提供了一套Client API来访问zookeeper集群，这套API可以用来对znode进行操作和watch的设置， 下面就是一个简单的Demo:

```java
Watcher myWatcher = new MyWatcher();
ZooKeeper zk = new ZooKeeper("localhost:2181", 3000,myWatcher );
Thread.sleep(2000);
Stat stat =  zk.exists("/t2", true);
 zk.exists("/t3", true);
if (stat != null){
    logger.info("stat: " + stat.toString());
}
Thread.sleep(100000);
zk.close();
```

代码主要做了：
1. 在连接ZooKeeper的时候放入myWatcher, 当连接之后会有一个watched event -> SyncConnected ,这时候watcher触发后就没了
2. 在用zk.exists传入true参数可以再次再/t2上面激活watcher, 这个时候可以对多个znode设置同一个watch，比如设置了t2和t3，那么他
和t3上面都会有watch，并且都是独立触发的



<br/>
## Zk在HBase中的实现

Zookeeper在HBase中的核心类是ZooKeeperWatcher,  这个类里面包含到Zookeeper集群的一个连接，另外ZooKeeperKeepAliveConnection扩展了该类，唯一的变动是覆盖了close方法。
ZooKeeperKeepAliveConnection主要是被HConnection所使用，这意味着整个HBase能连接到Zookeeper的只有这两个类。

相关类图如下：

![]({{ "/images/hbase-zk-watcher.png" | absolute_url }})

<br/>
### HBase到Zookeeper的连接


**Region Server**

对于RegionServer，通常来说会跟Zookeeper保持两个长连接, 分别是
1. 在HRegionServer的构建函数中直接构建的ZooKeeperWatcher, 在创建连接的时候会有如下类似日志, 可以看到它的Process identifier是regionserver:60020
    ```
    INFO org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper: Process identifier=regionserver:60020 connecting to ZooKeeper ensemble=example.com:2181
    ```
2. 在HRegionServer预初始化中调用HRegionServer#setupClusterConnection创建的HConnection中会包含一个到Zookeeper的长连接, 这里的Process identifier是hconnection-0xxxxx


<br/>
**HMaster**


HMaster扩展了RegionServer，所以region server里面的那两个连接在HMaster里面都有，但是除了这里两个，一般情况下，Master还保持了另外两个连接，分别是
1. ServerManager里面保持了一个到Zookeeper的连接，这是在它的构造函数里面调用ConnectionFactory#createConnection创建了一个HConnection
2. Master在初始化的时候在HMaster.startServiceThreads里面构建了LogCleaner，在LogCleaner里面会初始化ReplicationLogCleaner，并调用ReplicationLogCleaner的setConf方法，这里面会创建ZooKeeperWatcher，process identifier是replicationLogCleaner


<br/>

**Client**

Client通常会创建HConnection，那么一般来说保持到zookeeper的长连接也就是HConnection里面的这一个。



<br/>
### Watcher与Listener

ZooKeeperWatcher也是唯一实现了Watcher的一个类，各种监控的实现是通过往这个类里面添加listener来完成，当watch被触发的时候会在process方法里面针对事件类型，遍历listeners并调用相应的处理方法
```java
public void process(WatchedEvent event) {
    LOG.debug(prefix("Received ZooKeeper Event, " +
        "type=" + event.getType() + ", " +
        "state=" + event.getState() + ", " +
        "path=" + event.getPath()));

    switch(event.getType()) {

      // If event type is NONE, this is a connection status change
      case None: {
        connectionEvent(event);
        break;
      }

      // Otherwise pass along to the listeners

      case NodeCreated: {
        for(ZooKeeperListener listener : listeners) {
          listener.nodeCreated(event.getPath());
        }
        break;
      }

      case NodeDeleted: {
        for(ZooKeeperListener listener : listeners) {
          listener.nodeDeleted(event.getPath());
        }
        break;
      }

      case NodeDataChanged: {
        for(ZooKeeperListener listener : listeners) {
          listener.nodeDataChanged(event.getPath());
        }
        break;
      }

      case NodeChildrenChanged: {
        for(ZooKeeperListener listener : listeners) {
          listener.nodeChildrenChanged(event.getPath());
        }
        break;
      }
    }
}
  
```

HBase基本上是通过这些Listner来完成对zookeeper的操作来达到自己的目的，Listener的部分重要实现如下图：



![]({{ "/images/hbase-zk-listener.png" | absolute_url }})

HBase用这些Listener达到的目的大致份两种：
1. 监控：这里主要使用了ephemeral znode的特性，主要包括Master和RegionServer的进程监控，这两个进程的监控是接下来主要内容。
2. 状态存储：包括集群状态、meta位置等等信息的存储，下面主要会讲下meta的reigon server的存储。


<br/>

## Master选举和追踪


**Master选举**

因为HBase允许多个Master共存来保证Master节点的高可用性，但同一时间只允许有一个master是active，其余都是处于standby状态，这个竞选工作就是由zookeeper来完成的。
Active master znode的默认位置是/hbase/master, standby master位置在/hbase/backup-masters。
当HMaster启动的时候，在构造里面会先构建一个ActiveMasterManager，然后调用HMaster#startActiveMasterManager方法。
在startActiveMasterManager里面，首先把当前Master注册成backup master, 即在/hbase/backup-masters下面增加对应znode子节点，节点创建模式是EPHEMERAL。

然后启动一个独立的线程调用ActiveMasterManager#blockUntilBecomingActiveMaster方法等待直到当前节点成为active master，
如果成功成为active master，则调用HMaster#finishActiveMasterInitialization方法完成active master该有的初始化工作。

在ActiveMasterManager#blockUntilBecomingActiveMaster方法里面，首先调用MasterAddressTracker#setMasterAddress静态方法来尝试创建/hbase/master znode并设置地址，这里创建也是ephemeral类型的znode。
如果成功，则说明当前master成为active master，然后检查/hbase/backup-masters，如果有当前服务器地址，那么删掉后退出。

如果MasterAddressTracker#setMasterAddress设置不成功，说明已经有active master了，那么就进入循环等待，直到集群当前的master退出。



<br/>

**RegionServer追踪Master位置**

region server和client通过zookeeper来追踪当前active master的位置，主要是通过MasterAddressTracker这个类完成的。

RegionServer构造函数里面会构建一个MasterAddressTracker， 并且调用它父类ZooKeeperNodeTracker的start方法， 
这里面会先把自己注册到ZookeeperWatcher的listener列表里面，这样就可以监听znode的变化，然后检查master znode是否已经存在，如果存在则获取数据并再次设置watch。
```java
  public synchronized void start() {
    this.watcher.registerListener(this);
    try {
      if(ZKUtil.watchAndCheckExists(watcher, node)) {
        byte [] data = ZKUtil.getDataAndWatch(watcher, node);
        if(data != null) {
          this.data = data;
        } else {
          // It existed but now does not, try again to ensure a watch is set
          LOG.debug("Try starting again because there is no data from " + node);
          start();
        }
      }
    } catch (KeeperException e) {
      abortable.abort("Unexpected exception during initialization, aborting", e);
    }
  }
```


之后ReiongSever在初始化的时候会调用HRegionServer#blockAndCheckIfStopped来等待直到master znode节点创建，
这里面调用的是ZooKeeperNodeTracker#blockUntilAvailable。

等正常运行之后，如果master znode发生任何变化，MasterAddressTracker都会捕捉到这些变化然后调用对应的动作，这些动作都在ZooKeeperNodeTracker里面，
包括nodeCreated，nodeDeleted和nodeDataChanged


<br/>
## Region Server监控


RegionServer在启动的过长中会调用HRegionServer#createMyEphemeralNode在zookeeper的/hbase/rs znode节点下面创建一个子节点，这个子节点包含了这个RegionServer的信息，并且它是ephemeral类型，就是说当这个RegionServer
挂掉的时候，这个节点就会自动删除掉。


另外在Master初始化的时候会创建一个RegionServerTracker来追踪所有RegionServer的状态，在创建之后便调用其start方法把这个Listener注册到ZookeeperWatcher里面。
当/hbase/rs znode下面的子节点有变化的时候，RegionServerTracker#nodeChildrenChanged会被调用，继而调用RegionServerTracker#add去更新
RegionServerTracker#regionServers。


<br/>
## meta region server存储



/hbase/meta-region-server 存放了meta表对应的region server的信息，信息的设置，即写入，是在HMaster#assignMeta方法里面完成的，这里面首先会调用MetaTableLocator#getMetaRegionState来获取zookeeper
上面/hbase/meta-region-server znode里面的信息，这些信息包括hbase:meta region所在的服务器以及region状态，接着会根据获取到的信息按以下条件进一步处理:

- /hbase/meta-region-server znode节点不存在：这种一般来说是新集群启动的时候，那么会调用RegionReplicaUtil.getRegionInfoForReplica(HRegionInfo.FIRST_META_REGIONINFO）生成新的一个meta region，然后
调用AssignmentManager#assignMeta来assign meta region。AssignmentManager在HMaster#initializeZKBasedSystemTrackers里面被构建，构建之后会调用ZooKeeperWatcher#registerListenerFirst来注册这个listener，这个listener会被放到listener列表里面的第一个位置
- /hbase/meta-region-server znode存在，但meta region不存在: 验证meta region调用的是MetaTableLocator#verifyMetaRegionLocation， 如果meta region不存在，
还是会执行assign一个新的meta region，但是在这之前会调用HMaster#splitMetaLogBeforeAssignment切分在region目标服务器上面的meta对应的log。这种情况一般发生在整个集群重启，log split通常是region server重启之后recovery
过程中需要做的一件事情，目的是把之前region server下面的WAL按region切分掉，然后放到各个region的文件夹里面，以供后续recovery过程使用。
- /hbase/meta-region-server存在，meta region 也存在: 需要调用RegionStates#updateRegionState把meta region在内存里面的状态更新为Open，然后调用AssignmentManager#regionOnline来标记meta region为在线状态。


当meta region被region server open之后会调用MetaTableLocator#setMetaLocation方法把meta region server写入到zk 的 /hbase/meta-region-server。



meta region server主要会被客户端所使用，当客户端需要对某张表的某行数据进行操作时，首先会访问/hbase/meta-region-server去获取meta region的位置，然后再去查询meta信息。






<br/>


## 小结

 
 HBase使用zookeeper主要用来达成两个目的，1)使用它来达到监控进程的目的，包括对master和region server的监控，一旦进程失联，其他进程可以马上获取到相应的状态。2) 状态的存储，可以把一些通用的比较重要的信息放在zookeeper里面，因为它非常可靠。


 


-------
注：hbase代码基于cloudera hbase, 版本cdh5-1.2.0_5.13.0
