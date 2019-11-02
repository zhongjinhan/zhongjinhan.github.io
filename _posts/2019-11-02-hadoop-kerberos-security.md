---
layout:     post
title:      Hadoop Kerberos 安全认证详解
date:       2019-11-02
summary:    详细介绍了Hadoop是如何使用kerberos完成认证的
categories: hadoop
published:  true
---



<br>
## 非安全模式

当一个hadoop集群没有启用Kerberos的时候，集群基本就是裸奔的，任何能访问HDFS name node服务对应的ip和端口的人都可以删除掉上面的文件，这是因为kerberos是hadoop集群依赖的一个认证系统，没有认证意味着没有办法验证访问者到底是不是它自己所声明的身份，比如hadoop客户端可以设置环境变量HADOOP_USER_NAME或者直接su到一个系统用来声明自己的身份。所以一般来说生产集群都会开启kerberos，然后再配上一个权限管理服务，比如sentry或者ranger，来加强集群的安全。


<br>
## 如何配置一个安全的hadoop集群
按照官方文档去操作，比如https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SecureMode.html，但是真的繁琐且容易出错，所以还是用ambari或者cloudera manager，页面上面点击几下就可以完成配置。当然为了加深对hadoop安全的理解，这文档还是必须得看。


<br>
## HDFS客户端demo

先看一个如何访问一个安全的HDFS服务的demo

```java
    val conf = new Configuration()
    UserGroupInformation.loginUserFromKeytab("test", "/path/to/test.keytab")
    val fs = FileSystem.get(conf)
    fs.listStatus(new Path("/"))
      .foreach{f => println(f.getPath.getName)}
```
非常简单的一个用scala写的HDFS demo程序，
1. 首先创建一个Configuration，这个会从classpath里面读取到core-site.xml和hdfs-site.xml的信息
2. 然后调用loginUserFromKeytab，参数分别是principal和keytab路径，这是一个非常常见的程序化的kerberos登陆方式
3. 用conf去获取一个FileSystem
4. 把根目录下面的文件和文件夹信息打印出来



<br>
## Kerberos 登录
kerberos登陆按方式可以分为直接调用UserGroupInformation.loginUserFromKeytab和使用kerberos ticket cache，下面分别对这两种登陆方式进行详解

**loginUserFromKeytab**

UserGroupInformation是一个跟用户、组以及认证相关的核心的一个类，这个类主要使用了JAAS 相关的API, 下面来看看loginUserFromKeytab的代码和主要流程描述如下：
```java
static void loginUserFromKeytab(String user,
                                String path
                                ) throws IOException {
  if (!isSecurityEnabled())
    return;

  keytabFile = path;
  keytabPrincipal = user;
  Subject subject = new Subject();
  LoginContext login; 
  long start = 0;
  try {
    login = newLoginContext(HadoopConfiguration.KEYTAB_KERBEROS_CONFIG_NAME,
          subject, new HadoopConfiguration());
    start = Time.now();
    login.login();
    metrics.loginSuccess.add(Time.now() - start);
    loginUser = new UserGroupInformation(subject);
    loginUser.setLogin(login);
    loginUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS);
  } catch (LoginException le) {
    if (start > 0) {
      metrics.loginFailure.add(Time.now() - start);
    }
    throw new IOException("Login failure for " + user + " from keytab " + 
                          path+ ": " + le, le);
  }
  LOG.info("Login successful for user " + keytabPrincipal
      + " using keytab file " + keytabFile);
}
```
1. 新建subject实例，这是JAAS API里面比较关键的一个类，用来存储credential。
2. 新建一个LoginContext实例用来登陆，newLoginContext方法里面调用了构造函数 LoginContext(appName, subject, null, loginConf), 这里callbak函数传入的是null，意味着不需要认证的时候不需要用户介入了（callback一般用来输入不能预先确定的数据，或者各个登陆模块比较特殊的数据)
3. 调用loginContext#login方法尝试到KDC的AS去认证，认证成功之后会把credentail放到subject里面。
4. 用subject构建UserGroupInformation并assign给loginUser变量。


这里比较重要的一个信息是LoginContext创建时候所使用的配置信息，hadoop重新定义了一个配置类UserGroupInformation.HadoopConfiguration，这个类跟org.apache.hadoop.Configuration 没什么关系，而是扩展了javax.security.auth.login.Configuration, 实现了 getAppConfigurationEntry方法，在这方法里面会根据传入的appName选择相应的配置,比如使用keytab登陆所采用的appName是hadoop-keytab-kerberos，用这个appName在getAppConfigurationEntry会获取到KEYTAB_KERBEROS_CONF，这个配置包含了两个登录模块，分别是Krb5LoginModule和HadoopLoginModule，前者用来登陆kerberos，后者主要用来获取在不同安全模式下的用户名。在调用loginContext的login方法时，会依次执行这两个登录模块的login和commit方法，至此，kerberos登录就完成了。


**使用kerberos ticket cache完成认证**

有时候我们会使用kinit命令进行交互式的kerberos 登陆，然后再运行hadoop应用或者直接hdfs相关命令，这时候这些命令或应用都会读取kerberos登陆后的cache然后进行相应的认证, 比如我们有如下程序


```java
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    fs.listStatus(new Path("/"))
      .foreach{f => println(f.getPath.getName)}
```
在build之后先用kinit登录，然后再运行这段代码所在的程序，那么一般情况结果跟上面用keytab登录是一样的。
这是因为kinit登录之后会把ticket信息存在一个cache里面,在执行上面的FileSystem.get方法会调用FileSystem.Cache.get方法，这里会尝试new一个FileSystem.Cache.Key，在Key的构建函数里面有一个UserGroupInformation.getCurrentUser的动作
```java
Key(URI uri, Configuration conf, long unique) throws IOException {
  scheme = uri.getScheme()==null ?
      "" : StringUtils.toLowerCase(uri.getScheme());
  authority = uri.getAuthority()==null ?
      "" : StringUtils.toLowerCase(uri.getAuthority());
  this.unique = unique;
  
  this.ugi = UserGroupInformation.getCurrentUser();
}

```

而到这个点位置是没有执行任何登录相关的动作，所以执行getCurrentUser的时候会调用UGI里面的loginUserFromSubject,这个方法跟上面的loginUserFromKeytab非常类似，不同的地方在于获取LoginContext的配置信息时，使用appName = hadoop-user-kerberos去获取，这个appName是在UGI#initialize里面调用SecurityUtil.getAuthenticationMethod的时候决定的，可以看到枚举类型AuthenticationMethod kberos所对应的loginAppName正是hadoop-user-kerberos。接着会用这个appName去获取USER_KERBEROS_CONF， 这个配置包含了3个登录模块，分别是
- OS登录模块，视具体操作系统，比如UnixLoginModule, 主要获取OS级别的登陆系统
- Krb5LoginModule，跟上面的keytab登录几乎一样，有些参数做了改动，比如"useTicketCache = true"，意味着从ticket cache获取ticket，然后还从环境变量KRB5CCNAME里面获取cache的存储位置信息，这也是为什么通过环境变量修改ticket cache位置，hdfs命令能识别得到，而通过/etc/krb5.con里面重新设置cache位置之后hdfs命令就识别不到这个cache了，因为只考虑了环境变量。
- HadoopLoginModule - 跟keytab登录类似。


在构建LoginContext之后，上面一样也调用LoginContext#login方法去登录kerberos。





<br>
## 客户端到服务端的认证

Kerberos的登录只是从Kerberos KDC获取到一个ticket，但如果只是这个获取动作是没有任何意义的，我们要达到的目的是需要通过kerberos与服务端进行认证。Hadoop里面客户端到服务端的安全认证是在RPC级别实现的，对应于RPC的客户端和服务端，认证的实现也分为客户端和服务端，而具体的实现主要还是使用了Java的SASL API。

**RPC客户端认证实现**
这里以上面例子里面的fs.listStatus方法为例进行说明，在FileSystem.get被调用时会根据环境信息生成相应的具体文件系统，假设运行时有core-site.xml在classpath里面，并且里面有hdfs的fs.defaultFS的设置，所以这里会生成DistributedFileSystem, 所以这里调用的是DistributedFileSystem#listStatus。

在DistributedFileSystem里面包含一个DFSClient, 当listStatus调用时，会去调用DFSClient.listPaths方法, 接着会调用namenode.getListing， 这个namenode其实是基于ClientProtocol的一个proxy，也就是基于hadoop rpc的客户端prox，这个proxy是在DFSClient的构造函数里面创建的，
```java
proxyInfo = NameNodeProxies.createProxy(conf, nameNodeUri,
    ClientProtocol.class, nnFallbackToSimpleAuth);
this.dtService = proxyInfo.getDelegationTokenService();
this.namenode = proxyInfo.getProxy();
```
所以当调用namenode.getListing的时候，其实是作了一个RPC call， 因为是基于ProtocalBuff的RPC call，所以在客户端实际执行的是ProtobufRpcEngine.Invoker#invoke方法，而在这里面会去调用Client#call方法，然后会调用Connection#getConnection去创建一个到服务端的Connection，在这里面会执行Connection#setupIOstreams方法去设置input和output,而这里面有如下一段代码
```java
try {
  authMethod = ticket
      .doAs(new PrivilegedExceptionAction<AuthMethod>() {
        @Override
        public AuthMethod run()
            throws IOException, InterruptedException {
          return setupSaslConnection(in2, out2);
        }
      });
} catch (Exception ex) {
  authMethod = saslRpcClient.getAuthMethod();
  if (rand == null) {
    rand = new Random();
  }
  handleSaslConnectionFailure(numRetries++, maxRetriesOnSasl, ex,
      rand, ticket);
  continue;
}
```
这里调用传进来的ticket的doAs方法执行了一个PriviledgeExceptinAction的匿名实例(在这个例子里面，ticket，也就是UGI是在NameNodeProxies#createProxy里面用UGI#getCurrentUser生成的)，在run方法的实现里面调用了setupSaslConnection, 这个方法包含了客户端到服务端认证的流程，首先创建了一个SaslRpcClient的实例，然后调用SaslRpcClient#saslConnect完成和服务端的认证，具体流程如下
1.  首先向服务端发送一个 negotiate请求 - sendSaslMessage(outStream, negotiateRequest);
2. 接收从服务器返回的答复消息，正常情况下这个消息的状态是NEGOTIATE，然后就会调用selectSaslClient方法创建一个SaslClient实例， 这是Java SASL API里面一个客户端类，主要是用来跟服务端认证以及数据交互，也就是说hadoop使用了SASL API来完成客户端到服务端的认证。
3. SaslClient创建完成之后就调用saslClient.hasInitialResponse()检查当前的认证方式有没有初始response，GSSAPI也就是kerberos一定是有的，所以接着就调用aslClient.evaluateChallenge生成初始的responseToken，这个过程其实是向Kerberos KDC的ticket granting server发送请求获取service ticket。
4. 用response token生成sasl response 消息，这时候会把消息状态设置成INITIATE，然后发送给服务端 sendSaslMessage(outStream, response.build());
5. 接收从服务器返回的答复消息，正常情况下这个消息的状态是CHALLENGE, 然后会调用saslEvaluateToken方法去evalue challenge, 然后再发送消息给服务端，等这样来来回回往返几次之后，不出错的话那么客户端到服务端的认证就完成了。





**RPC服务端认证实现**

RPC服务端的认证实现跟客户端非常类似，主要实现是在Server.Connection#processSaslMessage方法里面，详细步骤
1. 接收到客户端的negotiate请求后调用buildSaslNegotiateResponse，如果安全配置的是kerberos，则向客户端返回的消息状态也是negotiate，这是在Server#buildNegotiateResponse里面做的决定。
2. 第二次接收到客户端的消息是 INITIATE，所以会调用createSaslServer创建SaslServer, 并且在方法processSaslToken里面对客户端发送过来的token(response)进行evaluate(saslServer.evaluateResponse),  然后把evaluate后的challege发送给客户端，这时候的消息状态是CHALLENGE
3. 第三次接收到的消息状态是RESPONSE，但是依然会调用processSaslToken来处理，不出意外的话这个时候返回给客户端的消息是SUCCESS，那么认证基本就完成了。


客户端和服务端认证消息交互状态如下

![]({{ "/images/rpc-sasl-auth.png" | absolute_url }})


至此，客户端和服务端的认证过程就完成了。



<br>
## Delegation Token
Hadoop 除了kerberos以外，还存在另外一种认证方式之外，就是delegation token，它的出现主要是因为当集群规模达到一定程序时，频繁地启动与结束mapreduce 或其他数据处理的任务会对Kerberos KDC造成比较大的压力。Delegation Token跟Kerberos 的ticket非常相似，也是可以在本地存储一段时间之后过期，也可以renew，默认24个小时会失效，不断renew之后的最长时间也不超过1周。跟kerberos的 ticket不同的是，它更轻量化，可以用如下方法就获取到
```java
fs.getDelegationToken
```

在实现方法，其实它调用的也是DFSClient里面相对应的，最后通过RPC其实调用的是远程namenode服务上面的方法，具体实现在FSNamesystem#getDelegationToken

```java
Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
    throws IOException {
  Token<DelegationTokenIdentifier> token;
  checkOperation(OperationCategory.WRITE);
  writeLock();
  try {
    checkOperation(OperationCategory.WRITE);
    checkNameNodeSafeMode("Cannot issue delegation token");
    if (!isAllowedDelegationTokenOp()) {
      throw new IOException(
        "Delegation Token can be issued only with kerberos or web authentication");
    }
    if (dtSecretManager == null || !dtSecretManager.isRunning()) {
      LOG.warn("trying to get DT with no secret manager running");
      return null;
    }

    UserGroupInformation ugi = getRemoteUser();
    String user = ugi.getUserName();
    Text owner = new Text(user);
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }
    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(owner,
      renewer, realUser);
    token = new Token<DelegationTokenIdentifier>(
      dtId, dtSecretManager);
    long expiryTime = dtSecretManager.getTokenExpiryTime(dtId);
    getEditLog().logGetDelegationToken(dtId, expiryTime);
  } finally {
    writeUnlock();
  }
  getEditLog().logSync();
  return token;
```



<br>
## Block Token
Block Token主要是在客户端访问data node的时候检查是否有权限访问，在服务器端配置完成后对用户基本无感。



<br>
## 小结

任何生产环境都应该配置kerberos，而在为各种客户端配置kerberos的时候总是会遇到各种各种的问题，而当你对hadoop认证方面有了一个基本了解之后，解决起来就显得轻松愉快了。


