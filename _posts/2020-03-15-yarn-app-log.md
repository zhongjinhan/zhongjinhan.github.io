---
layout:     post
title:      Hadoop YARN 应用日志
date:       2020-03-15
summary:    详细介绍了运行在YARN上面的日志相关内容
categories: hadoop
published:  true
---







<br/>

## 运行时日志位置

Hadoop YARN上面的应用是运行在多个container上面，每个container可以理解为一个进程，那么也就意味着application的日志就是每个container所产生的日志。应用在运行的过程中，每个container在指定的位置产生日志，具体位置由node manager里面的以下配置来指定

```
 yarn.nodemanager.log-dirs
```


产生的日志文件由应用自己来决定，一般来说会把标准输出重定向到stdout, 标准错误重定向到stderr，比如下面的代码就是spark准备application master的启动命令

```scala
val commands = prefixEnv ++
  Seq(Environment.JAVA_HOME.$$() + "/bin/java", "-server") ++
  javaOpts ++ amArgs ++
  Seq(
    "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
    "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
```


所以当application master对应的container运行的时候，标准输出就定向到stdout文件，标准错误就定想到stderr文件。

除了stdout和stderr以外，还可以配置log4j来生成特定的文件，比如在spark里面可以像下面这样配置log4j.properties把日志输出到custom.log文件，这里面的${spark.yarn.app.container.log.dir}变量在运行的时候会被替换成container运行的日志目录

```
log4j.rootCategory=INFO, FILE
log4j.appender.FILE=org.apache.log4j.FileAppender
log4j.appender.FILE.layout=org.apache.log4j.PatternLayout
log4j.appender.FILE.layout.conversionPattern=[%d] %p %m (%c)%n
log4j.appender.FILE.file=${spark.yarn.app.container.log.dir}/custom.log
log4j.appender.FILE.encoding=UTF-8
```


<br/>
## 日志聚合

每个container的日志分散于各个node manager的本地文件系统里面显示是有点问题的，比如当这个节点在应用运行之后突然磁盘损坏，那么日志也就丢失了，所以在Hadop2.0之后就引入了log aggregation的特性，可以把应用在node manager本地日志归集到HDFS上面的一个文件夹里面。


日志聚合的相关类如下：
![]({{ "/images/yarn-app-log.png" | absolute_url }})

下面看看日志聚合的详细过程

<br/>
**日志聚合服务的创建**

NodeManager在启动的时候会调用serviceInit来初始化服务，其中就调用了NodeManager#createContainerManager来创建ContainerManagerImpl实例，并且调用addService把containerManager添加到服务里面，最后调用super.serviceInit(conf)的时候就会调用ContainerManagerImpl#serviceInit来初始化ContainerManagerImpl里面的内容。而在ContainerManagerImpl#serviceInit里面会调用ContainerManagerImpl#createLogHandler来创建LogAggregationService, 然后调用addIfService添加这个服务，并且把LogAggregationService实例注册到AsyncDispatcher里面，由此可以看出一个NodeManager里面只有一个LogAggregationService, 所有在这个node manager上面运行的container的log aggregation都由这一个服务来完成。

```java
protected LogHandler createLogHandler(Configuration conf, Context context,
  DeletionService deletionService) {
  if (conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
    YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
    return new LogAggregationService(this.dispatcher, context,
      deletionService, dirsHandler);
  } else {
    return new NonAggregatingLogHandler(this.dispatcher, deletionService,
      dirsHandler,
      context.getNMStateStore());
  }
}
```

<br/>

**日志聚合服务初始化**

ContainerManagerImpl实现了CompositeService，并且在创建完成LogAggregationService之后添加了这个服务，所以在初始化服务的时候也会调用LogAggregationService#serviceInit进行初始化

```java
protected void serviceInit(Configuration conf) throws Exception {
  this.remoteRootLogDir =
    new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
      YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
  this.remoteRootLogDirSuffix =
    conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
    YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);
  super.serviceInit(conf);
}
```

可以看到这里面主要初始化了两个参数，分别对应于下面两个yarn的参数

```
<property>
<name>yarn.nodemanager.remote-app-log-dir</name>
<value>/tmp/logs</value>
</property>

<property>
<name>yarn.nodemanager.remote-app-log-dir-suffix</name>
<value>logs</value>
</property>
```


<br/>
**日志聚合服务事件处理**

LogAggregationService#handle方法是它的主要工作内容，这里会处理三种事件，分别是：
  - YARN应用的启动事件
  - container完成事件
  - YARN应用完成事件

```java
public void handle(LogHandlerEvent event) {
  switch (event.getType()) {
    case APPLICATION_STARTED:
      LogHandlerAppStartedEvent appStartEvent =
          (LogHandlerAppStartedEvent) event;
      initApp(appStartEvent.getApplicationId(), appStartEvent.getUser(),
          appStartEvent.getCredentials(),
          appStartEvent.getLogRetentionPolicy(),
          appStartEvent.getApplicationAcls(),
          appStartEvent.getLogAggregationContext());
      break;
    case CONTAINER_FINISHED:
      LogHandlerContainerFinishedEvent containerFinishEvent =
          (LogHandlerContainerFinishedEvent) event;
      stopContainer(containerFinishEvent.getContainerId(),
          containerFinishEvent.getExitCode());
      break;
    case APPLICATION_FINISHED:
      LogHandlerAppFinishedEvent appFinishedEvent =
          (LogHandlerAppFinishedEvent) event;
      stopApp(appFinishedEvent.getApplicationId());
      break;
    default:
      ; // Ignore
  }

}
```


下面来详细说明下这3个事件所触发的动作。

1. YARN应用的启动事件的响应
  当一个新的Application在这个NodeManager上面启动的时候，会发送APPLICATION_STARTED事件给dispatcher，最后事件会分发到LogAggregationService，会调用initApp方法初始化这个app，initApp首先会验证下HDFS日志根目录，也就是服务初始化里面所涉及到的参数yarn.nodemanager.remote-app-log-dir，接着调用initAppAggregator方法。在initAppAggregator方法里面会创建AppLogAggregatorImpl并在线程池里面启动它。
  AppLogAggregatorImpl启动之后会根据参数yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds参数的设定而觉得做什么，如果这个参数大于0则会触发滚动聚合，也就是在application过程中进行日志聚合；如果这个参数小于0，则什么都不做。

2. Container完成事件的响应
  当一个container完成的事件触发时，会调用LogAggregationService#stopContainer方法，继而调用AppLogAggregatorImpl#startContainerLogAggregation把containerId加入到pendingContainers这样一个队列当中。

3. YARN应用的运行完成的响应
  当一个YARN应用完成运行的事件触发时，会调用stopApp方法，继而调用AppLogAggregatorImpl#finishLogAggregation方法把appFinishing设置成true，然后唤醒doAppLogAggregation所在的线程，然后调用uploadLogsForContainers来上传日志，在这个方法里面会对pendingContainers这个队列里面的元素进行处理。假设根目录是/tmp/logs/test/logs，并且用户和applicationId分别是test和application_1579288022364_101366, 那么会上传到HDFS如戏，就可以在HDFS如下目录上面找到对应的日志:
  ```
    /tmp/logs/test/logs/application_1579288022364_101366
  ```
  这个目录下面会有多个日志文件，每个文件对应一个node manager节点，在同一个node manager上面的多个container的日志会被合并到一个文件。
  ```
    hdfs dfs -ls /tmp/logs/test/logs/application_1579288022364_101366/
    Found 3 items
    -rw-r----- 3 test hadoop 6310 2020-03-15 21:05 /tmp/logs/test/logs/application_1579288022364_101366/node-01.my-hadoop.com_8041
    -rw-r----- 3 test hadoop 99010 2020-03-15 21:05 /tmp/logs/test/logs/application_1579288022364_101366/node-02.my-hadoop.com_8041
    -rw-r----- 3 test hadoop 57200 2020-03-15 21:05 /tmp/logs/test/logs/application_1579288022364_101366/node-05.my-hadoop.com_8041
  ```


<br/>

## 日志留存时长


聚合日志的保留时长由yarn.log-aggregation.retain-seconds参数控制，比如如下配配置能够让日志在hdfs上面保留一周。

```
<property>
  <name>yarn.log-aggregation.retain-seconds</name>
  <value>604800</value>
</property>
```

<br/>
## 访问日志

从上面可以看到YARN application的日志要么在各个node manager本地磁盘上面，要么在聚合之后在HDFS上面，那如何查看这些日志呢？有两种方式，分别是yarn命令和HTTP Web形式访问。

<br/>
**YARN命令**

用YARN命令行来下载日志，命令如下：
```
yarn logs -applicationId application_1432041223735_0001 > app.log
```

这种方式方便的一点是直接可以在命令行里面操作，不方便的是全部container的日志都混在一个文件里面，不容易查找定位。


<br/>
**HTTP web方式**

每个node manager都开了web服务，可以通过NMWebApp#setup方法看到Node Manager WebUI的controller设置，
```java
public void setup() {
  bind(NMWebServices.class);
  bind(GenericExceptionHandler.class);
  bind(JAXBContextResolver.class);
  bind(ResourceView.class).toInstance(this.resourceView);
  bind(ApplicationACLsManager.class).toInstance(this.aclsManager);
  bind(LocalDirsHandlerService.class).toInstance(dirsHandler);
  route("/", NMController.class, "info");
  route("/node", NMController.class, "node");
  route("/allApplications", NMController.class, "allApplications");
  route("/allContainers", NMController.class, "allContainers");
  route(pajoin("/application", APPLICATION_ID), NMController.class,
      "application");
  route(pajoin("/container", CONTAINER_ID), NMController.class,
      "container");
  route(
      pajoin("/containerlos", CONTAINER_ID, APP_OWNER, CONTAINER_LOG_TYPE),
      NMController.class, "logs");
}
```

从setup方法里面可以看到/containerlogs这个路径会路由到org.apache.hadoop.yarn.server.nodemanager.webapp.NMController#logs方法，可以看到在这个方法里面会先去尝试获取ApplicationId，如果可以获取到，那么表示这个app还在运行中，直接会调用ContainerLogsPage这个类去展示页面；如果获取不到说明app运行结束，会尝试去获取yarn.log.server.url参数。在CDH环境下面，从下载的配置文件里面是卡不到配置选项yarn.log.server.url的，因为这个是服务端的配置，所以可以在运行时生成的yarn-site.xml里面可以看到，比如类似路径/run/cloudera-scm-agent/process/xxxx-yarn-NODEMANAGER/yarn-site.xml里面可以看到，

```
<property>
<name>yarn.log.server.url</name>
<value>http://xxxx:19888/jobhistory/logs/</value>
</property>
```



先这个值指定的是job history的log相关的url，也就是说对于一个已完成的application，它的container的日志会被redirect到这个job history，这里还会检查log aggregation设置，只有enable才会作跳转。





<br/>

**日志URL**

假设一个container id是container_001, 执行用户是test, 那么从NMWebApp#setup这个方法里面还可以拼出一个container的stderr的路径如下,

```
http://node:8041/containerlogs/container_001/test/tderr
```
如果application完成并启动了log aggregation，那么会被转发到history server上面，但是后面的路径不会变。


可以看出来，如果让用户自己去拼凑这样一个url显然是非常不方便的，所以各个应用都会在各自的Web UI上面适当的位置展现这些路径的链接，比如Spark Web UI，可以在Executor Tab里面查看每个executor，即container的日志，也可以在tasks页面找到task对应的executor log， 下面展开来讲下这个链接是如果在executor log页面上面显示出来的。



<br/>

**Spark Executor 日志URL是如何产生的**

当Spark的application master在申请container之后启动container的时候，也就是调用ExecutorRunnable#startContainer的时候， 会有一个准备环境的过程， 调用了
ExecutorRunnable#prepareEnvironment，相关代码如下，可以看到这里为这个新的container准备了两个环境变量，分别是SPARK_LOG_URL_STDERR和SPARK_LOG_URL_STDOUT，这些变量会设置在ContainerLaunchContext的环境变量里面，当container启动的时候就可以直接读取这些变量。

```scala
// Add log urls
container.foreach { c =>
  sys.env.get("SPARK_USER").foreach { user =>
    val containerId = ConverterUtils.toString(c.getId)
    val address = c.getNodeHttpAddress
    val baseUrl = s"$httpScheme$address/node/containerlogs/$containerId/$user"

    env("SPARK_LOG_URL_STDERR") = s"$baseUrl/stderr?start=-4096"
    env("SPARK_LOG_URL_STDOUT") = s"$baseUrl/stdout?start=-4096"
  }
}
```

接下来当这个Executor或者说Container启动的时候,也就是在object CoarseGrainedExecutorBackend#run里面，会初始化CoarseGrainedExecutorBackend，并调用setupEndpoint将这个类注册，然后在CoarseGrainedExecutorBackend#onStart里面把以上两个环境变量抽取出（extractLogUrls方法）来并通过DriverEndpointRef以Spark RPC形式向driver发送RegisterExecutor消息，该消息包含了executor主要信息，包括executorId，hostname，cores以及日志URL。另外这里的onStart方法实现了RpcEndpoint，这个方法会在这个RpcEndpoint在处理其他任何消息前被调用，其实就是接收到OnStart消息之后处理，而OnStart消息是第一条消息。

```scala
ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls))
```


Driver端是在DriverEndpoint的receiveAndReply的方法里面处理RegisterExecutor这个消息，可以看到会用这些信息来创建ExecutorData对象，然后拼装成一个SparkListenerExecutorAdded事件并post到ListenerBus里面，这个是SparkContext里面的一个事件监听总线。

`scala
case RegisterExecutor(executorId, executorRef, hostname, cores, logUrls) =>
...
totalRegisteredExecutors.addAndGet(1)
val data = new ExecutorData(executorRef, executorAddress, hostname,
  cores, cores, logUrls)
...
context.reply(true)
listenerBus.post(
  SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
makeOffers()
}
```


通过追踪可以看到AppStatusListener的onExecutorAdded方法对这个事件作了处理，并且创建一个LiverExecutor对象，接着调用liveUpdate把这个数据更新到AppStatusStore里面，至此executor相关数据，包括logURL就存储到AppStatusStore了。

```scala
override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
  // This needs to be an update in case an executor re-registers after the driver has
  // marked it as "dead".
  val exec = getOrCreateExecutor(event.executorId, event.time)
  exec.host = event.executorInfo.executorHost
  exec.isActive = true
  exec.totalCores = event.executorInfo.totalCores
  exec.maxTasks = event.executorInfo.totalCores / coresPerTask
  exec.executorLogs = event.executorInfo.logUrlMap
  liveUpdate(exec, System.nanoTime())
}
```




当我们打开Spark Web UI Executor Tab这个页面的时候，通过源码可以看到spark是通过o.a.s.ui.exec.ExecutorsTab#render方法来展示executor tab, 从render里面可以看到主要的视线是通过/static/executorspage.js来完成的。


```scala
def render(request: HttpServletRequest): Seq[Node] = {
  val content =
    <div>
      {
        <div id="active-executors" class="row-fluid"></div> ++
        <script src={UIUtils.prependBaseUri("/static/utils.js")}></script> ++
        <script src={UIUtils.prependBaseUri("/static/executorspage.js")}></script> ++
        <script>setThreadDumpEnabled({threadDumpEnabled})</script>
      }
    </div>


  UIUtils.headerSparkPage("Executors", content, parent, useDataTables = true)
}
```



从/static/executorspage.js中可以看到方法createRESTEndPoint组建了一个以allexecutors为结尾的api，然后通过这个api去获取executor所有的信息，包括logURL, 

 
```javascript
function createRESTEndPoint(appId) {
    var words = document.baseURI.split('/');
    var ind = words.indexOf("proxy");
    if (ind > 0) {
        var appId = words[ind + 1];
        var newBaseURI = words.slice(0, ind + 2).join('/');
        return newBaseURI + "/api/v1/applications/" + appId + "/allexecutors"
    }
    ind = words.indexOf("history");
    if (ind > 0) {
        var appId = words[ind + 1];
        var attemptId = words[ind + 2];
        var newBaseURI = words.slice(0, ind).join('/');
        if (isNaN(attemptId)) {
            return newBaseURI + "/api/v1/applications/" + appId + "/allexecutors";
        } else {
            return newBaseURI + "/api/v1/applications/" + appId + "/" + attemptId + "/allexecutors";
        }
    }
    return location.origin + "/api/v1/applications/" + appId + "/allexecutors";
}
```

我们可以在浏览器里面直接调用如下类似的URL去看下返回结果, 如下类似的URL

```
http://rm:8088/proxy/application_1569286042364_79525/api/v1/applications/application_1569286042364_79525/allexecutors
```

最后可以看到在AbstractApplicationResource里面定义了这个api，可以看到调用的是AppStatusStore#executorList来获取executor数据。
```scala
@Path("allexecutors")
def allExecutorList(): Seq[ExecutorSummary] = withUI(_.store.executorList(false))
```

至此，这个logUrl的整个生命周期就完成了，我们可以通过下图整理下整个流程。
![]({{ "/images/spark-log-url-process.png" | absolute_url }})