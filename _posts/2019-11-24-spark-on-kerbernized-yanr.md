---
layout:     post
title:      Spark on YARN 安全认证详解
date:       2019-11-24
summary:    详细介绍了Spark是如何运行在配置了Kerberos的YARN上面
categories: spark
published:  true
---



在开启kerberos之后，hadoop之上的应用框架都需要一些额外的代码来处理这一层认证流程，这里详细讲下spark是如何处理安全这一块内容的，按照作业运行的不同阶段可以分成两部分来讲解，分别是客户端提交阶段和ApplicationMaster运行阶段


<br>
## 客户端提交阶段的认证流程

Spark作业提交模式分为client和cluster，区别在于driver位于client端还是cluster里面，这里选择在cluster模式下的作业提交场景，client类似。主要的步骤如下图所示

![]({{ "/images/spark-security-client.png" | absolute_url }})



**1.客户端到Kerberos KDC 认证**

Spark Client启动之后主要是在SparkSubmit#prepareSubmitEnvironment方法里面通过调用UGI.loginUserFromKeytab(这里只考虑使用keytab的情况)登录Kerberos, 具体代码如下,

```scala
if (clusterManager == YARN || clusterManager == LOCAL || clusterManager == MESOS) {
  if (args.principal != null) {
    if (args.keytab != null) {
      require(new File(args.keytab).exists(), s"Keytab file: ${args.keytab} does not exist")
      // Add keytab and principal configurations in sysProps to make them available
      // for later use; e.g. in spark sql, the isolated class loader used to talk
      // to HiveMetastore will use these settings. They will be set as Java system
      // properties and then loaded by SparkConf
      sparkConf.set(KEYTAB, args.keytab)
      sparkConf.set(PRINCIPAL, args.principal)
      UserGroupInformation.loginUserFromKeytab(args.principal, args.keytab)
    }
  }
}
```
登录之后spark就可以访问HDFS，会把作业所用到的jar包和配置文件上传到HDFS，这里也keytab文件，这样后续的application master便可使用keytab文件。



**2. 客户端Delegation token获取**

跟大部分其他框架使用delegation token类似，Spark获取它的主要目的是避免executor去登录kerberos，从而减轻Kerberos KDC的压力。Token的获取是在客户端准备资源的时候执行的，在方法org.apache.spark.deploy.yarn.Client#prepareLocalResources里面调用了

```scala

private val credentialManager = new YARNHadoopDelegationTokenManager(
  sparkConf,
  hadoopConf,
  conf => YarnSparkHadoopUtil.hadoopFSsToAccess(sparkConf, conf))
.....
val nearestTimeOfNextRenewal = credentialManager.obtainDelegationTokens(hadoopConf, credentials)
.....
```
在YARNHadoopDelegationTokenManager里面先会初始化一个HadoopDelegationTokenManager, 这个类是用来管理HadoopDelegationTokenProvider , provider的实现有
- HBaseDelegationTokenProvider
- HiveDelegationTokenProvider
- HadoopFSDelegationTokenProvider

分别用来获取hbase, hive和hdfs的delegation token。
上面代码里面的credentialManager.obtainDelegationTokens会调用HadoopDelegationTokenManager#obtainDelegationTokens方法，在这方法里面会去遍历所有注册的provider，先调用delegationTokensRequired，如果需要的话再调用provider的obtainDelegationTokens获取token, 下面以 HadoopFSCredentialProvider#obtainDelegationTokens为例看下具体实现

1. 首先在方法fetchDelegationTokens里面调用fs.addDelegationTokens获取token并把token放到credentail里面。
2. 然后用getTokenRenewalInterva方法获取刷新时间长度，需要用当前登陆用户作为renewer再次去获取token, 因为上一步骤用yarn作为renewer获取的，如果对这个token进行renew操作会报错，所以才会再次去获取token。获取token之后调用renew方法获取下次过期时间，再从token的identifier获取token的分发时间，然后把这两个时间一减，就是刷新时间长度。
3. 用步骤2里面计算出来的时间长度加上步骤1里面生成的token的分发时间计算出下次刷新时间。


所以在获取delegation token 以后，也获取到了下次最新刷新时间，最后会用这个时间去算出实际下次刷新时间和更新时间，分别是减去当前时间之后乘以0.75和乘以0.8

```scala
if (loginFromKeytab && nearestTimeOfNextRenewal > System.currentTimeMillis() &&
  nearestTimeOfNextRenewal != Long.MaxValue) {

  // Valid renewal time is 75% of next renewal time, and the valid update time will be
  // slightly later then renewal time (80% of next renewal time). This is to make sure
  // credentials are renewed and updated before expired.
  val currTime = System.currentTimeMillis()
  val renewalTime = (nearestTimeOfNextRenewal - currTime) * 0.75 + currTime
  val updateTime = (nearestTimeOfNextRenewal - currTime) * 0.8 + currTime

  sparkConf.set(CREDENTIALS_RENEWAL_TIME, renewalTime.toLong)
  sparkConf.set(CREDENTIALS_UPDATE_TIME, updateTime.toLong)
}
```


**3. 提交token到Resource Manager**

Delegation token可以通过调用ContainerLaunchContext的setTokens方法放到ContainerLaunchContext里面，这一步是在org.apache.spark.deploy.yarn.Client#setupSecurityToken完成的。

```scala
private def setupSecurityToken(amContainer: ContainerLaunchContext): Unit = {
  val dob = new DataOutputBuffer
  credentials.writeTokenStorageToStream(dob)
  amContainer.setTokens(ByteBuffer.wrap(dob.getData))
}
```

然后再用containerLuanchContext创建ApplicationSubmissionContext, 最后调用YarnClient#submitApplication提交到ResourceManager，至此，delegation token就存储在RM里面了。
```scala
val appContext = createApplicationSubmissionContext(newApp, containerContext)
yarnClient.submitApplication(appContext)
```

因为只考虑Cluster模式，所以这时候client就可以退出了，而Application Master启动的时候所需要的token就是这个阶段设置的token，application master会用这个token去hdfs上面下载所需的资源。

总的来说在提交阶段，client会用keytab作一次kerberos认证，然后获取delegation token，之后把delegation token通过container launch context传递给application master。











<br>
## ApplicationMaster运行阶段

因为这里只讨论cluster模式，所以客户端在spark应用提交之后就退出了，后面剩下的主导权全在application master里面。而关于认证的主要类是AMCredentialRenewer, 这个类是在org.apache.spark.deploy.yarn.ApplicationMaster被初始化的时候创建并启动的

```scala
private val credentialRenewer: Option[AMCredentialRenewer] = sparkConf.get(KEYTAB).map { _ =>
  new AMCredentialRenewer(sparkConf, yarnConf)
}

private val ugi = credentialRenewer match {
  case Some(cr) =>
    // Set the context class loader so that the token renewer has access to jars distributed
    // by the user.
    val currentLoader = Thread.currentThread().getContextClassLoader()
    Thread.currentThread().setContextClassLoader(userClassLoader)
    try {
      cr.start()
    } finally {
      Thread.currentThread().setContextClassLoader(currentLoader)
    }
  case _ =>
    SparkHadoopUtil.get.createSparkUser()
}
```
从上面的代码可以看到，AMCredentialRenewer的初始化条件是sparkConf里面存在keytab变量，也就是说在提交spark应用的时候必须指定princial和keytab参数。创建完成之后调用它的start方法启动并把返回值赋给ugi变量。

下面以AMCredentialRenewer#start方法为切入点讲下主要几个步骤。


**Kerberos登录认证和认证刷新**

为了使spark应用程序能够长时间运行下去，application master里面必须要做kerberos登录认证，认证相关代码在AMCredentialRenewer#start, 代码如下:

```scala
  val ugi = doLogin()

  val tgtRenewalTask = new Runnable() {
    override def run(): Unit = {
      ugi.checkTGTAndReloginFromKeytab()
    }
  }
  val tgtRenewalPeriod = sparkConf.get(KERBEROS_RELOGIN_PERIOD)
  renewalExecutor.scheduleAtFixedRate(tgtRenewalTask, tgtRenewalPeriod, tgtRenewalPeriod,
    TimeUnit.SECONDS)
```

登录是通过调用doLogin方法，该方法调用UserGroupInformation.loginUserFromKeytabAndReturnUGI方法。并在登录之后设定循环调度不断去检查kerberos ticket然后尝试重新登录，调用的是UserGroupInformation.checkTGTAndReloginFromKeytab方法，另外循环时间是从参数spark.yarn.kerberos.relogin.period获取，默认是1分钟。



**Delegation Token获取以及刷新流程**

在AMCredentialRenewer#start里面调用了obtainTokensAndScheduleRenewal方法来获取和刷新Delegation token
```scala
private def obtainTokensAndScheduleRenewal(ugi: UserGroupInformation): Credentials = {
  ugi.doAs(new PrivilegedExceptionAction[Credentials]() {
    override def run(): Credentials = {
      val creds = new Credentials()
      val nextRenewal = credentialManager.obtainDelegationTokens(hadoopConf, creds)

      val timeToWait = SparkHadoopUtil.nextCredentialRenewalTime(nextRenewal, sparkConf) -
        System.currentTimeMillis()
      scheduleRenewal(timeToWait)
      creds
    }
  })
}

这里调用credentialManager.obtainDelegationTokens来获取delegationToken，这个跟客户端首次获取delegation的方法一样。接着算出下次刷新token所需要等待的时间timeToWait， 默认是token过期时间 - 当前时间之后再乘以0.75，然后在scheduleRenewal方法里面利用ScheduledExecutorService设定一个调度，这个调度会在等待所需的时间之后调用updateTokensTask方法，
```scala
private def updateTokensTask(): Unit = {
  try {
    val freshUGI = doLogin()
    val creds = obtainTokensAndScheduleRenewal(freshUGI)
    val tokens = SparkHadoopUtil.get.serialize(creds)

    val driver = driverRef.get()
    if (driver != null) {
      logInfo("Updating delegation tokens.")
      driver.send(UpdateDelegationTokens(tokens))
    } else {
      // This shouldn't really happen, since the driver should register way before tokens expire
      // (or the AM should time out the application).
      logWarning("Delegation tokens close to expiration but no driver has registered yet.")
      SparkHadoopUtil.get.addDelegationTokens(tokens, sparkConf)
    }
  } catch {
    case e: Exception =>
      val delay = TimeUnit.SECONDS.toMillis(sparkConf.get(CREDENTIALS_RENEWAL_RETRY_WAIT))
      logWarning(s"Failed to update tokens, will try again in ${UIUtils.formatDuration(delay)}!" +
        " If this happens too often tasks will fail.", e)
      scheduleRenewal(delay)
  }
}
```
updateTokensTask方法的大致流程如下
1. 跟start方法的步骤1和步骤2类似，调用doLogin和obtainTokensAndScheduleRenewal，这时候又会schedule下次的updateTokensTask。
2. 序列化获取到的delegation token
3. 向driver这个endpointRef发送UpdateDelegationTokens消息, 这里就涉及到delegation的更新问题了，下面详细介绍下。



**Executor Container初始Delegation Token**

因为考虑到Kerberos KDC的压力，所以YARN应用在普通container里面是不会跟Application Master container一样去认证Kerberos，取而代之的是使用更为轻量化的hadoop delegation token，Spark也不例外。所以这里就涉及到delegation token必须以某种方式传送给executor，spark目前按初始设置和后续更新使用了两种方式。这里的初始设置也是通过ContainerLaunchContext的setTokens方法完成，是在申请资源成功后启动container的时候设置的，具体是在代码ExecutorRunnable#startContainer方法里面。

```scala
def startContainer(): java.util.Map[String, ByteBuffer] = {
  val ctx = Records.newRecord(classOf[ContainerLaunchContext])
    .asInstanceOf[ContainerLaunchContext]
  val env = prepareEnvironment().asJava
  ctx.setLocalResources(localResources.asJava)
  ctx.setEnvironment(env)

  val credentials = UserGroupInformation.getCurrentUser().getCredentials()
  val dob = new DataOutputBuffer()
  credentials.writeTokenStorageToStream(dob)
  ctx.setTokens(ByteBuffer.wrap(dob.getData()))
```

**Executor Container Delegation Token更新流程**


Execuotr Container 的delegation token的更新是通过application master上面更新后的delegation token分发完成的，具体的代码入口是前面提到的updateTokensTask的第三步，也就是向driver这个endpointRef发送UpdateDelegationTokens消息，这里的driver其实是YarnSchedulerEndpoint对应的endpointRef，可看到YarnSchedulerEndpoint#receive方法里面是这么处理这个消息的

```scala
override def receive: PartialFunction[Any, Unit] = {
...
  case u @ UpdateDelegationTokens(tokens) =>
    // Add the tokens to the current user and send a message to the scheduler so that it
    // notifies all registered executors of the new tokens.
    SparkHadoopUtil.get.addDelegationTokens(tokens, sc.conf)
    driverEndpoint.send(u)
}
```

这里先调用addDelegationTokens把反序列化后的token添加到UGI里面，然后再向 driverEndpoint转发这个消息，driverEndpoint是DriverEndpoint对应的endpointRef，DriverEndpoint#receive方法如下
```scala
case UpdateDelegationTokens(newDelegationTokens) =>
  executorDataMap.values.foreach { ed =>
    ed.executorEndpoint.send(UpdateDelegationTokens(newDelegationTokens))
  }
```
executorDataMap里面存了所有executor的信息，包含executorEndpointRef信息，所对应的Endpoint是的CoarseGrainedExecutorBackend，它的receive方法如下，跟上面的一样，反序列化然后更新到本地。
```scala
case UpdateDelegationTokens(tokenBytes) =>
  logInfo(s"Received tokens of ${tokenBytes.length} bytes")
  SparkHadoopUtil.get.addDelegationTokens(tokenBytes, env.conf)
```

总的来说Application Master运行阶段的安全认证相关流程如下图所示：

![]({{ "/images/spark-security-am.png" | absolute_url }})


<br/>

## 解决一些问题或增加特性


在了解了Spark内部的安全认证机制后，在增加一些特性或者解决相关问题的时候思路就非常清晰了，比如Spark当前还不支持访问多个不同KDC的HDFS集群，那么这个时候就很清晰了，我们需要写一个类似HadoopFSCredentialProvider的类，主要的实现点如下：
1. 需要设定一个spark参数，里面包含principal，keytab，以及对应的hdfspath
2. 默认spark driver的kerberos信息是存储在全局UserGroupInformation里面，spark目前也是如此，如果需要一个额外的kerberos login并不影响UserGroupInformation，这时候需要UserGroupInformation的一个函数loginFromKeytabRetrunGUI, 并存储在一个临时变量里面。
3. 用步骤2里面的UGI和hdfs path一起去获取额外的这个hdfs的credentail



<br/>

注：本篇文章的spark源码是基于2.4.0版本，更确切地说是基于tag v2.4.0-rc5