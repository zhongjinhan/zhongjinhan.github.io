---
layout:     post
title:      编写一个 Hadoop YARN 应用
date:       2019-04-15
summary:    详细介绍了如何编写一个在Hadoop YARN上面运行的分布式应用
categories: hadoop 
published:  true
---
Apache YARN 是一个集群资源管理框架，举最简单的例子讲，你有10个服务器节点的计算资源，你手头有n个分布式计算任务，每个任务所需要的资源都不一样，那么这时候就可以通过YARN来有效地管理和利用资源，既能够满足这些计算的资源需求，又不至于浪费计算资源。

我们平时很少会手动去写一个直接基于yarn的分布式应用，而是直接调用各种计算框架(比如Spark或者MapReduce)提供的高级API, 而这些框架内部都已经帮我们实现了跟yarn交互的细节。

但是为了加深对Yarn整个框架的理解，亲手尝试写一个简单的Yarn应用还是非常有必要的，特别是当你在工作中遇到计算框架在Yarn层面的问题时，这时候就能够快速定位问题并解决。


## 一个YARN作业的生命周期
一个YARN作业(或者叫应用)的生命周期包含:应用的提交、应用运行(计算)和应用运行结束，应用运行和结束方式主要交给YARN应用自己去控制和处理，单应用的提交流程这个基本是固定的，如下图所示：

![]({{ "/images/yarn-submit-app.png" | absolute_url }})

提交流程可以分为4个步骤：
1. 客户端提交应用到Resource Manager
2. RM根据客户端提供的信息向其中一个node namager发起启动一个container的请求，node manager启动application master的container。
3. application master 里面的代码向resource manager请求分配运行slave的container。
4. 当上一步请求成功之后，AM会向node manager发起启动container的请求, 然后node manager执行启动命令启动命令，注意这一步是通过callback函数异步执行的。

## Yarn作业的主要结构
跟生命周期相对应地，按程序(main方法)我们可以把YARN应用分成3部分，分别是
1. Application Client: 提交作业的程序
2. ApplicationMaster: YARN框架里面application master这个特别的容器所需要执行的程序
3. Application Slave: 其他容器所需要执行的程序



## Application Client 部分
Client 部分代码在集群之外的客户端机器上面执行，一般来说提交成功后就推出，当然也可以一直链接着resource manager去poll提交应用的状态。这是非常轻量的一个程序。主要步骤如下：

1. 启动YarnClient去连接ResourceManager并创建一个Application
    ```java
    YarnClient yarnClient = YarnClient.createYarnClient();
    conf = new Configuration();
    yarnClient.init(conf);
    yarnClient.start();
    YarnClientApplication app = yarnClient.createApplication();
    ```

2. 设置ApplicationSubmissionContext 
    - 设置应用名称、应用类型、队列等信息, 这些很多信息是可以直接在YARN Web UI上面看到的。
        ```java
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        Priority pri = Priority.newInstance(1);
        appContext.setApplicationName("My App");
        appContext.setApplicationType("My Type");
        appContext.setPriority(pri);
        appContext.setQueue("default");
        appContext.setKeepContainersAcrossApplicationAttempts(false);
        ```


    - 设置 ContainerLaunchContext, 主要是决定或者控制application master里面运行什么内容和运行环境，包括：
        - 准备application master运行时所需要的环境变量
        - 准备LocalResource，这里面包含application master程序运行所需要jar包，配置文件等信息, 这里还需要把这些资源上传到HDFS
        - 准备运行命令列表：application master该用什么命令来启动

        ```java
        Map<String, String> env = new HashMap<String, String>();
        env.put("CLASSPATH", "./*");

        FileSystem fs = FileSystem.get(conf);
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        addResource(conf, fs, appMasterJar, appId.toString(), localResources);

        List<String> commands = new ArrayList<String>();
        Vector<CharSequence> vargs = new Vector<CharSequence>(10);
        vargs.add("/usr/bin/java my.sample.hadoop.yarn.app.ApplicationMaster");
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/my_app_stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/my_app_stderr");
        StringBuilder command = new StringBuilder();
        for(CharSequence var : vargs){
            command.append(var).append(" ");
        }
        commands.add(command.toString());

        ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);
        ```
    - 设置运行application master所需要的资源，即内存和cpu core数量
        ```java
        int amMemory = 128;
        int amVCores =1;
        Resource capability = Resource.newInstance(amMemory, amVCores);
        appContext.setResource(capability);
        ```

3. 提交ApplicationSubmissionContext 
    ```java
    yarnClient.submitApplication(appContext);
    ```

## Application Master 部分



在分布式计算里面，application master一般作为一个master的角色控制其他slave一起完成计算，所以application master里面的大部分代码都是由应用本身所需要达到的目的所决定，但除此之外还需要一些框架提供的接口，主要有环境变量、AMRMClient和AMNMClient, 下面来详细看下这三块内容。

#### YARN容器环境变量

在容器启动之后，可以从ApplicationConstants.Environment这个枚举类型里面获取已经预设置好的环境变量名称，然后再从读取System.getenv获取环境变量的值，在这里可以获取到比较重要的值，比如container Id, 启动容器的node manager host等

```java
Map<String, String> envs = System.getenv();
String containerIdString = envs.get(ApplicationConstants.Environment.CONTAINER_ID.key());
String nmHost = envs.get(ApplicationConstants.Environment.NM_HOST.key());
```

除了这些特定的环境变量之外，还可以在这里读取到Application Client在提交应用时候在ContainerLaunchContext里面所设置的环境变量。


#### AMRMClient 
AMRMClient主要是application master用来跟Resource manager交互的一个通道，在创建AMRMClient的时候需要提供一个AMRMListener, 这里面提供了几个callback函数接口，供应用实现container分配时候的功能
```java
public class AMRMListener implements AMRMClientAsync.CallbackHandler {

    public void onContainersAllocated(List<Container> allocatedContainers) {}
    public void onShutdownRequest() {}
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}
    public void onContainersCompleted(List<ContainerStatus> statuses) {}
    public void onError(Throwable e) {}
    public float getProgress() { }

}

```
这几个方法里面最重要的是onContainerAllocated，我们一般会在这里去准备ApplicationSlave的ContainerLanuschContext, 然后使用NodeManagerClient去启动这个容器。准备ContainerLaunchContext的过程跟Application Client准备Application Master的ContainerLaunchContext的过程大致相同，也包括环境变量的准备、Resource的准备以及一系列启动命令。


AMRMClient除了提供callback函数，还可以向Resource Manager注册ApplicationMaster
```java
String appMasterHostName = NetUtils.getHostname();
RegisterApplicationMasterResponse response =
        amRMClient.registerApplicationMaster(appMasterHostName,
                0, "");
```
可以向Resource Manager申请container, ContainerRequest里面设置了cpu、内存等资源相关信息,

```java
AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
amRMClient.addContainerRequest(containerAsk);
```
最后注销application master
```java
FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
amRMClient.unregisterApplicationMaster(finalStatus, "yeah~~", "");
```
#### NodeManagerClient
AM向RM申请的容器成功分配之后，通过NodemanagerClient去启动相应的容器。创建NMClient的时候也可以提供相应的Listener,这个Listner提供如下callback方法:

```java
public class NMCallbackHandler implements NMClientAsync.CallbackHandler{

    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {}
    public void onContainerStopped(ContainerId containerId) {}
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {}
    public void onStartContainerError(ContainerId containerId, Throwable t) {}
    public void onStopContainerError(ContainerId containerId, Throwable t) {}
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {}
}

```
这些方法调用的时候基本可以从其命名里面看的出来。



## Application Slave 部分

Application Slave容器里面运行的内容则全部由应用自己来决定



## 总结
总的来说一个YARN应用通过以下三个类跟集群进行交互，
- YarnClient: 申请一个application，准备Application Master运行上下文，然后提交应用
- AMRMClient: AM向RM申请Container资源，并在申请之后准备Container的运行上下文
- NMClient: 启动container


而分布式应用本身的通信方式和运行内容都需要应用开发者自己去编写。

