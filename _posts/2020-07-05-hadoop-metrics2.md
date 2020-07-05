---
layout:     post
title:      Hadoop Metrics V2 框架
date:       2020-07-05
summary:    Hadoop Metrics V2 框架简介
categories: hadoop
published:  true
---




通过Metrics框架我们可以了解Hadoop各个服务的运行状态，如下图所示整个框架大致由MetricsSystem, MetricsSource和MetricsSink组成。MetricsSource在各个服务内部生成各种metrics，MetricsSystem通过一个定时循环线程从MetricsSource获取metrics然后放到MetricsSink里面，MetricsSink负责把这些指标写到外部数据源。
另外MetricsSource还通过MetricsSourceAdapter注册到了MBeanServer, 我们可以通过JMX客户端来查询各项指标。



![]({{ "/images/hadoop-metrics2-metrics-system.png" | absolute_url }})



<br/>

 相关类图如下：

![]({{ "/images/hadoop-metrics2-class.png" | absolute_url }})

<br/>

## MetricsSystem实现


MetricsSystem实现在类MetricsSystemImpl里面，通过DefaultMetricsSystem#initialize来初始化，DefaultMetricsSystem是个单例，也就是在一个进程里面，只有一个MetricsSystemImpl。DefaultMetricsSystem#initialize这个初始化动作一般在服务启动之初就会调用，比如NameNode服务是在NameNode#createNameNode方法里面调用的。




<br/>

### MetricsSystemImpl初始化流程


DefaultMetricsSystem在初始化的时候会调用MetricsSystemImpl#init方法，里面的主要初始化内容在MetricsSystemImpl#start方法里面，包括

1. 配置
2. 启动Timer线程


<br/>
#### MetricsSystemImpl配置

<br/>
**读取配置文件**

首先调用MetricsConfig#create(java.lang.String)来读取配置文件，默认配置文件是hadoop-metrics2.properties, 另外还会从hadoop-metrics2-{prefix}.properties读取。

配置格式如下:

```
[prefix].[source|sink].[instance].[options]
```

比如可以把namenode的metrics输出到文件并且文件名为namenode-metrics的配置如下：
```
namenode.sink.file.class=org.apache.hadoop.metrics2.sink.FileSink
namenode.sink.file.filename=namenode-metrics.out
```

<br/>

**配置MetricsSink**

在MetricsSystemImpl#configureSinks方法里面，先读取"sink"类型的配置, 对于读取的每个Sink配置先调用MetricsConfig#getPlugin生成实例，用这些配置加MetricsSink实例构建MetricsSinkAdapter，这里面包含一个线程, 构建完成之后调用MetricsSinkAdapter#start方法启动线程。最后所有的sink都放到sinks这个Map里面。

<br/>

**配置MetricsSource**

在MetricsSystemImpl#configureSources里面，读取sourcefilter和sourceconfig，这里的sourceConfig主要还是包含了filter相关的信息，MetricsSource并不像MetricsSink那样通过配置可以反射出实例，因为MetricsSource是源头，一般安排在各个服务里面。

配置读取完成之后会调用org.apache.hadoop.metrics2.impl.MetricsSystemImpl#registerSystemSource构建一个sysSource然后启动它，sysSource就是MetricsSystemImpl本身，MetricsSystemImpl也实现了MetricsSource。



<br/>

#### 启动Timer线程



构建一个Timer来定时执行任务，任务的具体内容在onTimerEvent方法，这里面首先调用sampleMetrics从所有注册的Metricsource#getMetrics方法获取metric snapshot，然后调用publishMetrics发布metrics到sink。publishMetrics遍历每个MetricsSinkAdapter，然后调用MetricsSinkAdapter#putMetrics把metrics扔到MetricsSinkAdapter队列里面。

MetricsSystem在初始化完之后主要的工作内容就是在Timer这个线程里面。



<br/>

## MetricsSink

MetricsSink通过配置来启用，在MetricsSystemImpl初始化的时候读取配置来初始化，在MetricsConfig#getPlugin里面装载、构建Sink实例并执行init方法，最后通过调用newSink方法构建MetricsSinkAdapter。



MetricsSinkAdapter包含一个进程和一个队列SinkQueue,这个队列是半阻塞队列，即消费阻塞，生产不阻塞，如果队列满了，直接弃掉从生产者来的消息。





![]({{ "/images/hadoop-metrics2-sink.png" | absolute_url }})

MetricsSystemImpl Timer线程会不断定时往队列里面扔Metrics，MetricsSinkAdapter sink线程从队列里面消费metrics然后调用MetricsSink的putMetrics方法。



<br/>

## MetricsSource

MetricsSource在hadoop里面有三种实现形式
1. 直接实现MetricsSource接口，比如JvmMetrics， 实现内容都在方法getMetrics里面，一般来说metric相对来说比较固定的
2. 另外一种是在field和method上面标记annotation，使用MetricsRegister作为中间收集metrics, 这种方法较为灵活, 比如DataNodeMetrics, NameNodeMetrics等


<br/>

### MetricsSource注册


一般在服务初始化的时候调用MetricsSystem#register来注册对应的MetricsSource，比如NameNodeMetrics就是在NameNode#initialize的时候调用NameNodeMetrics#create方法来注册的。

<br/>

#### 构建MetricsSource
注册的时候先构建一个MetricsSourceBuilder，用来生成MetricsSource实例，这里首先会生成一个MetricsRegistry。


<br/>

**构建MetricsRegistry**

在initRegistry方法里面实现，根据传入的对象生成Register有几种方法：

- 获取传入Object的所有Field，查看有没有MetricsRegistry类型的field，如果有并且不为空，获取MetricsRegistry实例并且hasRegistry=true, 这是对应于MetricsSource的第二种实现。
- 获取传入Object对应的class的所有annotations， 如果有Metrics annation，则用该anntation的信息创建MetricsRegistry，这也是对应上面的第二种实现，只是没有定义MetricsRegistery field，而是使用class annotation。
- 如果即没有MetricsRegistry field又没有class annatation, 则直接构建MetricsRegistry, 这是对应上面MetricsSource的第一种实现。


```java
private MetricsRegistry initRegistry(Object source) {
    Class<?> cls = source.getClass();
    MetricsRegistry r = null;
    // Get the registry if it already exists.
    for (Field field : ReflectionUtils.getDeclaredFieldsIncludingInherited(cls)) {
      if (field.getType() != MetricsRegistry.class) continue;
      try {
        field.setAccessible(true);
        r = (MetricsRegistry) field.get(source);
        hasRegistry = r != null;
        break;
      }
      catch (Exception e) {
        LOG.warn("Error accessing field "+ field, e);
        continue;
      }
    }
    // Create a new registry according to annotation
    for (Annotation annotation : cls.getAnnotations()) {
      if (annotation instanceof Metrics) {
        Metrics ma = (Metrics) annotation;
        info = factory.getInfo(cls, ma);
        if (r == null) {
          r = new MetricsRegistry(info);
        }
        r.setContext(ma.context());
      }
    }
    if (r == null) return new MetricsRegistry(cls.getSimpleName());
    return r;
```


<br/>

**增添被比较为Metric 的 Filed和method**

在MetricsSourceBuilder#add方法里面，遍历所有的field和method，把有Metric annotation的添加到MetricsRegestry里面


<br/>

**生成MetricsSource**

在build方法里面，如果传入的对象实现了MetricsSource，则直接返回该对象；否则，会构建一个匿名MetricsSource, getMetrics的实现里面调用MetricsRegistry#snapshot方法把该MetricsRegistry里面的所有metrics
写入collector里面。


<br/>

#### 注册MetricsSource

在获取MetricsSource实例之后会调用MetricsSystemImpl#registerSource注册它，
主要就是生成一个MetricsSourceAdapter然后调用它的start方法，start方法会调用startMBeans方法把当前MetricsSourceAdapter实例注册到MBeanServer上面，MBean的object名称是

```
Hadoop:service=<serviceName>,name=<nameName>
```









-----------
注：源码基于hadoop版本2.7.3


