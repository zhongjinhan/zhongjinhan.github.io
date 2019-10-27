---
layout:     post
title:      如何用Java实现Kerberos的安全认证
date:       2019-10-27
summary:    用 Java JAAS API 和 SASL API 实现基于 Kerberos 的客户端与服务端之间的认证
categories: kerberos
published:  true
---


现在越来越多的分布式框架都采用了kerberos作为其安全认证，而在java的世界里想要构建基于kerberos认证的系统就不可避免的要使用JAAS API和SASL API，搜了下没搜到现成的DEMO程序，所以就有了这篇文章了。

<br>

## Kerberos

Kerberos是一个认证，假设现在有一个客户端想通过kerberos认证到服务端，那么大致的认证过程如下：

![]({{ "/images/krb-auth.png" | absolute_url }})
认证过程总共可以分为6步：

1. 客户端通过principal和key向KDC的Authentication Server发起登陆请求，这里使用的解密加密key就是客户的密码
2. AS向客户端发送TGT, TGT 是通过AS和TGS之间的共享key加密过的。
3. 客户端发送TGT到Ticket Granting Server， 这里还附带了服务端所使用的principal信息。
4. TGS如果能成功解密，则发送到Server的ticket，这是通过一个TGS和Server之间共享的key加密过的。
5. 客户端发送Server ticket到服务端。这里要注意的是服务端在启动之初也是向AS发启动了一个登陆动作。
6. Server成功解密ticket之后完成认证

经过这几步认证之后，client和server就可以进行后续的消息交互了，接下来我们用一个demo程序来详细了解JAAS和SASL API是如何参与这6步认证过程以及后续的安全数据交互。

<br>

## JAAS API 

JAAS全称Java Authentication and Authorization Service, 是java里面用来作认证和授权的一个通用API，这里我们只关注认证方面的功能。JAAS底层支持多种认证方式，当然也包括Kerberos。

一般来说使用JAAS API来认证kerberos需要经过以下3个步骤：
1. 构建LoginContext
2. 调用LoginContext#login
3. 调用Subject.doAs来执行后续对服务器的认证

**构建LoginContext**

可以采用如下方法构建loginContext, 当然这只是其中一个构建函数，在这里面有两个信息比较关键，分别是配置信息和callbackhandler。

```java
LoginContext lc = new LoginContext(name, new TextCallbackHandler());
```


- 配置信息

    默认情况下配置文件是通过jvm参数java.security.auth.login.config传入，比如
    ```bash
    -Djava.security.auth.login.config=conf/jaas.conf
    ```
    然后会根据传入的name去这个配置文件里面找到相应的配置区域，假设 name = DemoClient, 那么下面的DemoClient里面的内容就会被读取

    ```conf
    DemoClient {
    com.sun.security.auth.module.Krb5LoginModule required
    principal="client01@TEST-KRB-SERVER.COM";
    };
    ```

    配置信息里面指定了登陆模块为kerberos，当然还有很多其他设置，比如useKeytab，keytab等等。

    除了使用默认的传入配置文件这种方式之后，我们还可以程序化地去组装配置信息，只要扩张这个类javax.security.auth.login.Configuration就可以了，然后再使用带Configuration参数的LoginContext构造函数
    ```java
    public LoginContext(String name, Subject subject,
                        CallbackHandler callbackHandler,
                        Configuration config)
    ```


- CallbackHandler

    CallbackHandler是Login模块用来跟用户来交互的，比如这里我们传入了TextCallbackHandler的实例，那么当kerberos需要用户密码的时候，就会弹出提示让用户输入密码。当然一旦在配置文件里面指定了keytab之后，就不需要交互了，这时候我们可以直接传入null。



**调用LoginContext#login**

当login方法被调用时，LoginContext首先new一个新的javax.security.auth.Subject，然后先调用LoginModule的login方法，如果在这之前发现这个LoginModule还没有初始化，那么先用subject, callbackHanlder, state以及module option初始化这个LoginModule, 然后再执行login方法，login执行完成之后，接着执行commit方法。这些具体的invoke动作都在LoginContext#invoke方法里面执行。

这里以com.sun.security.auth.module.Krb5LoginModule为例讲讲这三个方法
 -  Krb5LoginModule#initialize
初始化方法里面主要是对options的解析和检查，比如keytab, principalname, useTicketCache等kerberos常见的配置，如果开启debug，会打印如下信息
    ```java
    System.out.print("Debug is  " + debug
                    + " storeKey " + storeKey
                    + " useTicketCache " + useTicketCache
                    + " useKeyTab " + useKeyTab
                    + " doNotPrompt " + doNotPrompt
                    + " ticketCache is " + ticketCacheName
                    + " isInitiator " + isInitiator
                    + " KeyTab is " + keyTabName
                    + " refreshKrb5Config is " + refreshKrb5Config
                    + " principal is " + princName
                    + " tryFirstPass is " + tryFirstPass
                    + " useFirstPass is " + useFirstPass
                    + " storePass is " + storePass
                    + " clearPass is " + clearPass + "\n");
    ```
 -  Krb5LoginModule#login
总的来说就是根据选项的不同尝试各种不同的登陆方式，主要的实现在attemptAuthentication方法，里面的逻辑大致如下，首先useTicketCache=true, 则会去ticke cahce里面直接获取credential；否则，如果usekeytab=true，则去读取相应的keytab然后发送到kdc认证，如果keytab没有，则会尝试弹出prompt输入密码(prompt密码具体实现是在方法Krb5LoginModule#promptForPass里面，这里面同时也涉及到了前面提到的callbackhandler函数), 然后发送到kdc认证。认证过程都需要初始化一个KrbAsReqBuilder，然掉用其action方法去发送给kdc的as认证，最后调用KrbAsReqBuilder#getCreds获取credentails。

-  Krb5LoginModule#commit
如果login认证成功之后，commit会把当前获取到的princpal和credentail分别放到subject的principals和privateCredentails里面。相当于把结果暴露给了LonginContext。至此，如果没异常发送，login就成功了，也就是从KDC的AS成功获取到了TGT。


总的来说JAAS其实就是执行了Kerberos 6步认证里面的第一步和第二步，这时候client的subject对象里面存储了从KDC返回的TGT，可以供后续代码使用。

**Subject#doAs**

如果要基于login过后所获取的TGT做后续认证操作的话，那么需要实现一个ava.security.PrivilegedAction，并且在其run方法里面调用具体实现代码，然后把这样一个实例传入Subject.doAs里面，这里另外的一个参数是subject, 可以用LoginContext#.getSubject()获取，里面包含了实际的credential，也就是TGT。
```java
public static <T> T doAs(final Subject subject,
                    final java.security.PrivilegedAction<T> action)
```
而在doAs方法里面，先会用subject去创建一个AccessControlContext, 然后再调用java.security.AccessController.doPrivileged方法。


<br>

## Java SASL API

SASL 全称Simple Authentication and Security Layer，是一种网络标准（https://www.ietf.org/rfc/rfc2222.txt)，其定义了一种在客户端和服务端之间认证以及数据安全传输的协议。Java SASL API是其在java的一种实现，java开发者可以使用这个API构建安全认证及安全数据交互的客户/服务端应用。Java SASL API支持多种认证，包括GSSAPI(Kerberos), DIGEST-MD5等等。

**SaslClient和Server构建**

在API使用层面，首先要构建SaslClient和SaslServer

可以用如下方式构建SaslClient
```java
Map<String, String> props = new HashMap<String, String>();
props.put(Sasl.QOP, "auth-conf");
String[] mechanisms = new String[]{"GSSAPI"};
SaslClient sc = Sasl.createSaslClient(mechanisms, null, "test",
        "myserver", props, null);
```
因为这里只考虑Kerberos认证，所以mechanism里面只有GSSAPI，而protocal(test)和server(myserver)实际上指定了服务端所使用的Kerberos Principal名称，在这里服务端的principal名称就是test/myserver。props属性设置了Sasl.QOP, 这个跟后面的安全数据交互有关。

SaslServer构建方法跟SaslClient构建方法类似, 但是多了一个CallbackHandler，在这里面必须处理AuthorizeCallback类型的callback。
```java
SaslServer ss = Sasl.createSaslServer("GSSAPI", "test",
        "myserver", props, new ServerCallbackHandler());
```

因为认证方式指定为GSSAPI, 所以在构建saslClient和saslserver的时候实际上会生成GssKrb5Client和GssKrb5Server,可以看到在这两个类里面的实现并没有多少代码，而是大量调用了GSSAPI接口。



**SASL API 认证过程**

SASL是一个challenge-response的协议，如果客户端有初始response的话（可以用saslClient.hasInitialResponse得到)，就调用evaluateChallenge方法得到一个初始reponse发送给服务端，否则发送一个空的response给服务端，服务端接收到response之后使用evaluateResponse得到一个challenge发送给客户端，客户端接受到之后调用evaluateChallenge方法之后得到一个response再发送给服务端，这个过程一致循环下去直到完成(isComplete方法），这样认证过程就完成了。

对于GSSAPI，也就是kerberos来说，首先它是有初始Reponse的, 通过调用saslClient.evaluateChallenge(new byte[]{})就可以获取初始Response，这个初始response的获取过程其实就是kerberos 6步认证里面的第三步和第四步，也就是从KDC里面的Ticket Granting Server获取service ticket的过程，下面看下这个过程的实现细节。

- 首先是如何获取JAAA login后的kerberos TGT

    这个主要发生在GSSAPI在创建GSSContext的时候调用在Krb5MechFactory#getCredentialElement获取的，然后会调用如下Krb5InitCredential.getTGT函数, 在这里调用AccessController#getContext()获取到的context里面其实就已经包含subject信息了，这个跟之前调用Subject.doAs形成呼应, 如果sasl这些代码不是在PrivilegedAction里面运行，则这里获取到的AccessControlContext就不会包含subject。最终通过调用Krb5Util.getTicket方法获取到KerberosTicket, 
    ```java
    private static KerberosTicket getTgt(GSSCaller var0, Krb5NameElement var1, int var2) throws GSSException {
        final String var3;
        if (var1 != null) {
            var3 = var1.getKrb5PrincipalName().getName();
        } else {
            var3 = null;
        }

        final AccessControlContext var4 = AccessController.getContext();

        try {
            final GSSCaller var5 = var0 == GSSCaller.CALLER_UNKNOWN ? GSSCaller.CALLER_INITIATE : var0;
            return (KerberosTicket)AccessController.doPrivileged(new PrivilegedExceptionAction<KerberosTicket>() {
                public KerberosTicket run() throws Exception {
                    return Krb5Util.getTicket(var5, var3, (String)null, var4);
                }
            });
        } catch (PrivilegedActionException var7) {
            GSSException var6 = new GSSException(13, -1, "Attempt to obtain new INITIATE credentials failed! (" + var7.getMessage() + ")");
            var6.initCause(var7.getException());
            throw var6;
        }
    }
    ```
- 其实是如何获取Service Ticket

    在Krb5Context#initSecContext里面调用Credentials.acquireS4U2proxyCreds方法获取service credentail。可以看到在acquireS4U2proxyCreds方法里面，首先构建一个KrbTgsReq实例，然后KrbTgsReq#sendAndGetCreds方法获取credentail，而当这句代码执行之后，会看到服务端有如下日志
    ```
    krb5kdc[871](info): TGS_REQ (3 etypes {17 16 23}) 192.168.1.2: ISSUE: authtime 1571064514, etypes {rep=17 tkt=16 ses=17}, client01@TEST-KRB-SERVER.COM for service_test@TEST-KRB-SERVER.COM
    ```


    ```java
    public static Credentials acquireS4U2proxyCreds(String var0, Ticket var1, PrincipalName var2, Credentials var3) throws KrbException, IOException {
        KrbTgsReq var4 = new KrbTgsReq(var3, var1, new PrincipalName(var0));
        Credentials var5 = var4.sendAndGetCreds();
        if (!var5.getClient().equals(var2)) {
            throw new KrbException("S4U2proxy request not honored by KDC");
        } else {
            return var5;
        }
    }
    ```





**安全数据交换**

当没有使用SSL等安全协议的时候，SASL API也提供了数据安全交换的方式，比如发送数据的时候可以先wrap下
```java
byte[] out = sc.wrap(original, 0, original.length);
writeByte(outStream, out);
```
而接收到数据之后需要需要调用unwrap方法
```java
byte[] unwrapped = sc.unwrap(resp, 0, resp.length);
```

针对于kerberos，这两个方法都调用了GSSAPI里面GSSContextImpl相应的方法。



<br>

## 样例代码
代码位置 - https://github.com/zhongjinhan/demo/tree/master/java-sasl-api

程序结构
- Util.java - 包含了通用的JAAS login和writeByte方法
- SaslClientAction.java 和 SaslClientAction  - 分别是client和server的具体实现
- DemoClient 和 DemoServer - 分别是client和server的main函数入口

如何运行
1. 先需要一个外部Kerberos KDC，以及可以工作的kerberos客户端配置，然后创建client和server端的principal 信息
2. 编译 mvn package
2. 启动服务端 bash run_server.sh, 输入服务端principal相应的密码
3. 启动客户端 bash run_client, 输入客户端principal相应的密码

<br>

## 小结
总的来说，通过JAAS API可以实现kerberos认证的第一步和第二步，使用SASL API可以实现三四五六步。
