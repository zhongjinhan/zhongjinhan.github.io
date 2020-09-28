---
layout:     post
title:      Apache Hive 组件深入介绍
date:       2020-09-28
summary:    稍微有点深入的介绍了hive 2.x版本里面的组件
categories: hive
published:  true
---



Apache Hive可能是使用最广泛的SQL on Hadoop, 主要组件如下图所示:



![]({{ "/images/hive-components-overview.png" | absolute_url }})



- 客户端包括Beeline、HiveJDBC和HCatalog：beeline是Hive自带的交互式命令行客户端，跟一般的数据库客户端非常类似；Hive同样也提供了JDBC接口，应用可任意嵌入Hive客户端；HCatalog也是个命令行工具，主要提供了DDL相关的操作。
- 服务端包括HiveServer2、Hive Metastore和WebHCat: HiveServer2是核心服务，SQL的编译和提交都在这里完成；HiveMetastore服务提供了类似数据库的catalog管理，把Hadoop上面的文件像数据库一样组织管理；WebHCat则是把Hive DDL以HTTP REST API的形式暴露出来。另外Hive LLAP没怎么用过就不提了。



<br/>

## Beeline和Hive JDBC


Beeline是一个类似sqlline的SQL交互式命令行客户端，里面包含了一个到HiveServer2的JDBC连接，HiveJDBC底层采用了thrift作为实现方式:

![]({{ "/images/hive-beeline-jdbc.png" | absolute_url }})


<br/>

### Beeline实现细节


Beeline采用如下命令行启动，程序入口是o.a.h.beeline.BeeLine#main方法


```bash
# 无kerberos
$HIVE_HOME/bin/beeline -u "jdbc:hive2://127.0.0.1:10000"
# 有kerberos
$HIVE_HOME/bin/beeline -u "jdbc:hive2://127.0.0.1:10000/default;principal=hive/_HOST@REALM.COM"
```

<br/>

#### Beeline指令


Beeline里面执行的指令有两种，分别是以!开头的sqlline类似命令和一般的Hive QL语句，这两种指令的具体执行方式如下:

- 以!开头的命令：尝试用o.a.h.beeline.BeeLine#commandHandlers列表去匹配对应命令的o.a.h.beeline.ReflectiveCommandHandler，如果匹配得到则执行ReflectiveCommandHandler#execute方法，即调用o.a.h.beeline.Commands里面对应的方法执行。比如!connect和!open调用的都是o.a.h.beeline.Commands#connect(String)方法，因为对应ReflectiveCommandHandler的names是{"connect","open"}，第一个是connect。
- Hive SQL语句：常规JDBC执行方式，用Connection创建Statement然后执行Statement#execute(sql)，具体实现在o.a.h.beeline.Commands#executeInternal方法里面。




<br/>


#### Beeline JDBC连接创建


Beeline创建JDBC连接有两种方式：
- 启动beeline的时候把URL放入-u参数：这个时候会在方法o.a.h.beeline.BeeLine#initArgs初始化参数的时候调用o.a.h.beeline.BeeLine#connectUsingArgs创建到HiveServer2的连接，当然最后还是会调用Commands#connect方法
- 启动beeline之后执行!connect或者!open命令：直接调用Commands#connect方法


到Hive的JDBC连接会放在类o.a.h.beeline.DatabaseConnection里面, 另外又用DatabaseConnections来包裹了一层，在需要使用的时候，比如在Beeline#createStatment里面，会调用DatabaseConnections#current方法获取当前的连接。


通常来说使用beeline不会去指定JDBC Driver, 那么HiveDriver是什么时候被载入的呢？

通过查看源码可以看到，在Commands#connect方法里面会调用BeeLine#findRegisteredDriver方法，这方法里面会
调用DriverManager#getDrivers会触发DriverManager类装载过程，也就出发了DriverManager里面的static block块，DriverManager#loadInitialDrivers方法会以service的形式从
META-INF/services/java.sql.Driver里面装载并初始化driver，而Hive jdbc子项目下面的这个文件里面配置了org.apache.hive.jdbc.HiveDriver，所以HiveDriver就是这个时候注册的。












<br/>




### Hive JDBC 实现细节


Beeline是一个壳，里面实际调用的都是Hive JDBC的方法。在具体实现方面，Hive JDBC采用thrift RPC来实现客户端到服务端的请求与答复，里面涉及到的核心类是o.a.h.service.rpc.thrift.TCLIService.Client，这是thrift自动代码生成所产生的默认客户端实现，实现了接口o.a.h.service.rpc.thrift.TCLIService.Iface，对应的服务端的实现是o.a.h.service.cli.thrift.ThriftCLIService。thrift接口定义文件在service-rpc子项目下面，路径service-rpc/if/TCLIService.thrift。



<br/>


#### Hive Connection的创建


当调用DriverManager#getConnection获取JDBC连接的时候，会调用HiveDriver#connect构建一个HiveConnection，里面大概做了两件事情

- 构建TCLIService.Client：先调用o.a.hadoop.hive.common.auth.HiveAuthUtils#getSocketTransport构建TSocket，如果是kerberos或者delegationtoken，则先创建TSaslClientTransport，然后创建一个TUGIAssumingTransport；接着用TSocket创建TBinaryProtocol；TBinaryProtocal创建一个TCLIService.Client
- 调用TCLIService.Client#OpenSession：这个RPC调用在服务器端对应的是ThriftCLIService#OpenSession, 会在服务器端创建一个HiveSession


<br/>

#### 执行Hive QL


JDBC执行SQL的入口是o.a.h.jdbc.HiveStatement#execute(String), 这里会调用TCLIService.Iface#ExecuteStatement的RPC接口，HiveServer2接收到这个请求后，默认会在编译完SQL之后立刻返回，SQL的运行是异步执行的，所以客户端过不了多久就会接收到答复。

接着客户端会不断调用TCLIService.Iface#GetOperationStatus去服务端polling SQL运行的状态，这是在方法o.a.h.jdbc.HiveStatement#waitForOperationToComplete里面完成，可以看到这里一直在循环调用GetOperationStatus方法直到接收到特定的状态触发退出循环，没有任何等待动作，这是因为等待动作是在服务端做了。

HiveServer2的ThriftCLIService#GetOperationStatus会处理客户端发送过去的RPC调用，这里会调用CLIService#getOperationStatus，在这个方法里面会有一个等待动作，等待时长由参数hive.server2.long.polling.timeout控制，默认5秒, 也就是说客户端获取状态的时候会在服务端等5秒，如果任务还没完成的话。




<br/>

## HiveServer2


HiveServer2是最核心的服务，负责接收客户端的请求后，解析编译SQL并提交任务到集群运行。HiveServer2主要有3个服务，分别是

- ThriftCLIService：基于thrift的一个RPC服务端，负责接收客户端发送的RPC请求
- CLIService: 服务于ThriftCLIService, 里面提供了比较关键的各类Manager
- HttpServer：HiveServer2的web ui，显示服务当前的Session、SQL执行情况等信息




<br/>

### ThriftCLIService



ThriftCLIService是接口o.a.h.service.rpc.thrift.TCLIService.Iface的服务端实现，接口定义文件还是service-rpc/if/TCLIService.thrift, 客户端的实现是前面提到的TCLIService.Client。

但是在HiveServer2初始化的时候根据参数hive.server2.transport.mode(binary或者http)来决定具体是实现ThriftHttpServlet还是ThriftBinaryCLIService，默认启动的是ThriftBinaryCLIService。这两者都扩展了ThriftCLIService，不同之点在于一个采用HTTP方式，另外一个采用基于TCP的RPC。这里就默认的选项ThriftBinaryCLIService展开讲讲，http的类似。

<br/>

#### 启动ThriftBinaryCLIService


启动ThriftBinaryCLIService调用的是方法ThriftCLIService#start，这个方法启动了一个线程，即运行ThriftBinaryCLIService#run方法，里面的实现主要是构建了一个TThreadPoolServer，这是thrift提供的一种基于线程池的RPC服务，构建的关键参数包括：

- ExecutorService，一堆线程池用来处理客户端的请求，基于实现ThreadPoolExecutorWithOomHook
- TTransportFactory，调用方法HiveAuthFactory#getAuthTransFactory获取
- TServerSocket，使用方法HiveAuthUtils#getServerSocket，ssl和non-ssl有着不同的构造方式
- TProcessorFactory，Processor的factory，通过方法HiveAuthFactory#getAuthProcFactory获取，假设没有kerberos的情况下，那么返回的factory是oPlainSaslHelper.SQLPlainProcessorFactory，从这个factory的getProcessor方法可以看到返回的Processor是TSetIpAddressProcessor，而这个Processor是基于传入的this构建，也就是基于ThriftBinaryCLIService对象构建，所以整个RPC的处理逻辑都在它的父类ThriftCLIService里面。




<br/>

### CLIService


CLIService实现了接口ICLIService，从接口定义来看，跟ThriftCLIService非常相似，但是比它更细，比如ThriftCLIService有ExecuteStatement方法，而对应于不同的情况，ICLIService对应地定义了4种相关的方法。



<br/>

#### SessionManager

CLIService里面最重要的一个实例就是SessionManager,在服务初始化的时候会构建它。SessionManager负责管理客户端的session，每个客户端连接之后都会调用SessionManager#createSession创建一个独立的session, session的实现是类是HiveSessionImpl，HQL的编译与执行都是通过session来完成。


<br/>

#### OperationManager

Hive QL在HiveServer2里面执行的时候是一个Operation，比如通常sql语句的具体实现叫SQLOperation, 而这些Operation的管理是通过OperationManager，SessionManager在构建的时候会构建OperationManager。


当HiveSession接收到客户端执行SQL请求时，会调用OperationManager#newExecuteStatementOperation生成一个Operation实例。另外OperationManager保存了OperationHandle和Operation的映射关系，客户端查询Operation状态的时候通过OperationHandle定位到对应的Operation。OperationManager也保存了查询的详细信息，由变量historicalQueryInfos和liveQueryInfos负责。



<br/>


### HttpServer(WebUI)


HiveServer2自带了一个HttpServer来展示Session和操作执行的情况，当用户执行Hive SQL的时候可以实时在页面查看执行情况。HttpServer在HiveSever2初始化HiveServer2#init方法里面被构建，其Web服务采用Jetty实现，构建过程中的各项设置如下

- 调用HttpServer.Builder#setContextAttribute添加attr hive.sm，对应的值是从CLIService获取的SessionManager实例，页面展示的主要动态内容都是通过SessionManager获取
- LLAP页面的path是/llap，由servlet LlapServlet处理
- app根目录定义在方法HttpServer#getWebAppsPath里，当前是hive-webapps/hiveserver2
- Web服务主页面由方法HttpServer.Builder#setContextRootRewriteTarget设置成/hiveserver2.jsp
- 查询页面的path是/query_page，由servlet QueryProfileServlet处理



下面简单介绍下两个主要页面的实现逻辑：Web UI主页和查询详情页面


<br/>

#### Web UI 主页

主页面的内容呈现的具体实现在service/src/resources/hive-webapps/hiveserver2/hiveserver2.jsp, 可以看到jsp里面开始处就调用ServletContext#getAttribute获取hive.sm属性对应的值，也就是SessionManager实例。


主页面的内容有三块：
- Active Session信息：SessionManager#getSessions获取Hive Session列表
- Open Queries信息： sessionManager.getOperationManager().getLiveQueryInfos()获取实时查询情况
- Last Max 25 Closed Queries: sessionManager.getOperationManager().getHistoricalQueryInfos()获取历史查询



<br/>

#### 查询详情页面


查询详情页面的路径是/query_page，对应的servlet是QueryProfileServlet.class, 可以在QueryProfileServlet#doGet方法里面看到返回的是QueryProfileTmpl().render方法所返回的信息。QueryProfileTmpl使用使用jamon生成，java一种template engine，很有年代感的一个框架，从它的主页https://www.jamon.org/ 就可以看出。

template文件位置service/src/jamon/org/apache/hive/tmpl/QueryProfileTmpl.jamon，
jamon的maven插件会在service/target/generated-jamon/org/apache/hive/tmpl下面生成
两个文件QueryProfileTmpl.java和QueryProfileTmplImpl.java


另外从QueryProfileServlet也可以看到查询详情页面里面的动态信息QueryInfo也是从SessionManager里面的OperationManager里面获取的。



<br/>

### Hive QL请求流程

下面序列图展示的是客户端连打开session以及执行SQL请求的情况



![]({{ "/images/hive-jdbc-client-server.png" | absolute_url }})


- 打开session是HiveConnection创建的最后一个阶段，从上图可以看到这个操作的最终目的是创建一个HiveSessionImpl
- 有了对应的HiveSession之后就可以执行SQL了，HiveSessionImpl会调用OperationManager#newExecuteStatementOperation方法创建一个Hive操作对应的Operation，这里假设是SQLOperation。创建之后会立马调用SQLOperation#run方法执行SQL，在run方法里面首先会调用prepare方法编译SQL，默认情况下prepare方法是同步的，运行完成之后会异步提交runQuery方法到后台线程池执行，然后返回。
- 客户端提交ExecuteStatement之后会马上执行GetOperationStatus远程调用查看SQL执行情况，ThriftCLIServic接收到请求之后会调用CLIService#getOperationStatus，这里首先用operationHandle会从OperationManager获取对应的Operation实例，然后调用getBackgroundHandle获取backgroundHandle，并尝试查看后台执行的Operation的状态。



<br/>

#### 运行SQLOperation

SQLOperation#run方法是SQL执行的入口，整个流程可以分为两个阶段：

1. 编译：编译阶段里面主要是解析SQL文本，验证SQL所涉及的数据库对象，构建操作树，优化操作树，最后根据hive执行引擎构建对应的物理计划
2. 执行：执行对应的mapreduce、tez或者spark任务




<br/>

### HiveServer2 工作线程

HiveServer2的主要工作线程有两类：
1. ThriftBinaryCLIService里面的TThreadPoolServer有个线程池，最小线程数和最大线程数分别由参数hive.server2.thrift.min.worker.threads和hive.server2.thrift.max.worker.threads控制，默认值分别为5和500。这个线程里面的主要工作是从socket接收并反序列化消息然后调用CLIService里面相关的函数，一直到SQL的编译默认都是在这里完成。
2. SessionManager里面的backgroundOperationPool，pool大小由参数hive.server2.async.exec.threads设定，默认是100。这些线程的主要工作内容是执行具体任务。


<br/>

## Hive MetaStore


Hive Metastore主要的功能是维护Hive的元数据信息，这些信息包括数据库、表、列和分区。这些信息主要是在Hive语句解析的过程中被用到，当然也可以单独作为一个查询服务。


HiveMetastore主要包含3个部分，分别是客户端、服务以及一个ObjectStore。


<br/>

### MetaStore客户端


Hive Metastore客户端和服务端依然采用thrift实现，接口定义文件为metastore/if/hive_metastore.thrift，主要的接口是ThriftHiveMetastore.Iface，thrift生成的默认客户端实现是ThriftHiveMetastore.Client

MetaStore客户端主要对外类是HiveMetaStoreClient，实现了接口IMetaStoreClient。在具体方法实现的时候，都会调用对应的thrfit client的方法，比如HiveMetaStoreClient#getAllTables主要是调用了ThriftHiveMetastore.Client#get_all_tables方法。


HiveMetaStoreClient在构建thrift client的时候会根据hive.metastore.uris使用不同的实现
- hive.metastore.uris为空的时候，视为内嵌metastore，直接使用类HiveMetaStore.HMSHandler，这个类是metastore server的主要实现
- hive.metastore.uris非空， 则初始化一个thrift默认client - ThriftHiveMetastore.Client，连接到远程metastore server



在使用客户端方面，HiveServer2在查询计划构建的过程中查询元数据使用的是类o.a.hadoop.hive.ql.metadata.Hive，这里面构建了一个SessionHiveMetaStoreClient，这个类扩展了HiveMetaStoreClient。而Spark在操作hive元数据的时候直接在o.a.spark.sql.hive.client.HiveClientImpl里面构建了o.a.hadoop.hive.ql.metadata.Hive。


<br/>
### Metastore 服务端


Metastore服务端相当于一个thrift server，可以看到在HiveMetaStore#startMetaStore里面会构建一个TThreadPoolServer，跟HiveServer2里面的非常类似。
thrift server的handler是HiveMetaStore.HMSHandler，该类实现了ThriftHiveMetastore.Iface，也就是说大部分有关元数据的操作都在这个类里面实现。




<br/>


### Object Store


ObjectStore介于HiveMetaStore.HMSHandler和元数据存储系统之间的中间接口层，HMSHandler会调用ObjectStore对应的方法，ObjectStore里面目前采用JDO来访问不同的存储系统。

比如HMSHandler#get_tables方法，这个方法目的是列出所有表，它会调用ObjectStore#getTables方法，这里面会构建一个JDO查询去事先配置的存储系统，比如MySQL里面，去检索所有table。




<br/>

## HCatalog


HCatalog在目前2.x版本几乎没什么新用户了，因为beeline完全可以替代它，它当前的主要的主要功能是执行DDL，样例命令如下

```bash
hcat -e 'show databases'
hcat -e "select * from test.emp limit 5";
```

从具体实现来看，HCatalog调用的其实是HiveServer2里面的一些类，以下是HCatlog的主要类o.a.h.hcatalog.cli.HCatCli里面的代码端

```java
CliSessionState ss = new CliSessionState(new HiveConf(SessionState.class));
SessionState.start(ss);
HCatDriver driver = new HCatDriver(ss.getConf());
int ret = driver.run(cmd).getResponseCode();
```

可以看到它构建了一个CliSessionState，这个类扩展了SessionState, 而SessionState主要是用在HiveServer2的HiveSessionImpl里面用来存储客户端连接以后HiveSession的状态信息。

HCatDriver扩展了Driver, Driver是SQLOperation里面最重要的类之一，HiveSQL的编译和执行的具体事先都在Driver里面。


看起来就像是个本地命令行版的HiveServer2，但其实不是，因为在HCatCli通过变量hive.semantic.analyzer.hook加入了analyze hook，过滤掉了很多操作，基本只剩下了DDL，具体过滤的信息可以在hook类o.a.h.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer找到。






<br/>

## WebHCat

WebHCat就是在HCatalog外面包括了一层HTTP Rest API。当用户发送HTTP请求到WebHCat服务器，服务器会执行一条HCatalog的命令然后返回信息。

例如下面这个HTTP请求，相当于是"show databases"

```
$ time curl  http://webhcat-server:50111/templeton/v1/ddl/database && echo
{"databases":["default","test"]}
real	0m8.736s
user	0m0.013s
sys	  0m0.013s
```

在不做优化的情况下，这个执行时长也是比较感人的。


<br/>


### WebHCat实现


入口方法是o.a.h.hcatalog.templeton.Main，Web框架用的还是Jetty，API定义在o.a.h.hcatalog.templeton.Server，千篇一律，就不展开说了。



<br/>

## 小结

总之，核心服务是HiveServer2，核心代码是HiveQL的编译与执行，其他都服务于此。


<br/>

-----------
注：hive源码基于cloudera的cdh6.3.2版本，对应apache的2.1.1

