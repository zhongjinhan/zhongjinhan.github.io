---
layout:     post
title:      深入 Hadoop RPC
date:       2019-10-06
summary:    从一个简单的例子深入Hadoop RPC的底层实现
categories: hadoop
published:  true
---




RPC，即远程服务调用，可以很大程序上简化分布式应用的开发，了解一个应用的底层RPC可以比较有效地帮你了解整个系统，也有助于阅读当个模块/服务的源代码，这里就看看看Hadoop RPC，先来看下如何使用hadoop RPC

## 如何使用Hadoop RPC
Hadoop RPC在Hadoop的hadoop-common这个子项目里面，可以独立拿出来使用，下面就用它来实现一个简单的echo服务。


首先来设计一个协议，也就是具体实现功能的接口，比如这里创建了一个接口SomeProtocol, 这里定义了一个echo方法。这里接口必须要扩展VersionedProtocol, 然后给定一个版本号，这是Hadoop RPC本身的要求。

```java
interface SomeProtocol extends VersionedProtocol {
    long versionID = 1L;
    String echo(String value) throws IOException;
}
```

创建一个类SomeProtocolImpl，用来实现SomeProtocol

```java
public class SomeProtocolImpl implements SomeProtocol {
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return SomeProtocol.versionID;
    }

    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return new ProtocolSignature(SomeProtocol.versionID, null);
    }

    public String echo(String value) throws IOException {
        return "echo: " + value;
    }
}
```

再来构建服务端并且启动

```java
Configuration conf = new Configuration();
RPC.Server server = new RPC.Builder(conf)
        .setProtocol(SomeProtocol.class)
        .setInstance(new SomeProtocolImpl())
        .setBindAddress("0.0.0.0")
        .setPort(8889)
        .setNumHandlers(2)
        .build();
server.start();
```


客户端构建

```java
        Configuration conf = new Configuration();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 8889);
        SomeProtocol proxy = RPC.getProxy(SomeProtocol.class, SomeProtocol.versionID,address, conf);
        System.out.println(proxy.echo("hello"));
```




一个RPC的应用就创建好了，总体来说接口还是比较清晰的，下面将以这个echo应用为例来解释下hadoop rpc底层的实现。总体流程将会从客户端调用开始，到服务端的接受请求并处理请求，最后返回给客户端结果。个人感觉这个解释流程还是比较符合一般人的流程化思维的，非常有助于理解与记忆。


[demo代码](https://github.com/zhongjinhan/demo/tree/master/hadoop-rpc)



## 客户端调用

在上面这个例子的客户端代码里面可以看到调用方法只是简单地执行了proxy.echo("hello")，其实这里用了代理设计模式，使用了java的动态代理。 RPC.getProxy 生成一个proxy，这个getProxy的最终是调用了java里面的Proxy.newProxyInstance， 而这个生成proxy的核心内容都是在实现InvocationHandler接口里面，也就是说proxy.echo("hello")被执行的时候，其实是执行了InvocationHandler接口里面的invoke方法，而在hadoop RPC里面，实现了这个接口的类有两种ProtobufRpcEngine.Invoker和WritableRpcEngine.Invoker。ProtobufRpcEngine和WritableRpcEngine是Hadoop RPC在远程调用过程中的两种不同实现，这里首先讲讲为什么会有这两种实现。

## Writable 与 Protobuf
RPC之所以能够简化分布式的开发复杂度，主要是因为能够像调用本地的方法一样调用远程的方法，但我们知道这其实只是把调用的方法、参数等对象序列化成字节流，然后通过网络传输到远程服务器端，服务器接收到字节流后反序列化得到相应的信息，然后根据这些信息调用相应的方法然后返回。而根据这个序列化的方式不同，Hadoop RPC在具体实现的时候分为两种: Writable和Protobuf.

- Writable接口 从hadoop1.x开始就支持，是基于java.io.DataInput 和 java.io.DataOutput的序列化对象，write方法把field序列化到DataOutput，readField方法从input反序出一系列field，所以在实现的时候一定要注意顺序，序列和反序的顺序要保持一致。
- Protobuf则是开源的序列化框，Hadoop2.x后开始引进。相对于于Writable, 该框架性能优良，并且支持多种语言，自己拥有一套IDL来支持复杂的接口。


## invoke 实现
下面以WritableRpcEngine.Invoker为例，来讲讲invoke方法里面的具体实现。
首先在Invoker的构造函数里面获取了一个Client实例, 如果ClientCache里面没有对应的socketfactory的client实例，则会创建一个。
在Invoker#invoke里面，代码不是很多，主要的实现是调用了client.call
```java
client.call(RPC.RpcKind.RPC_WRITABLE, 
            new Invocation(method, args),
            remoteId, fallbackToSimpleAuth);
```
这里参数和方法名称(在这个例子里面就是echo方法和hello参数)被封装在WritableRpcEngine.Invocation里面，而Invocation正是实现了Writable接口，所以在WritableRpcEngine的RPC实现里面，方法名称和参数值的序列化和反序列，即write和readFileds方法，都是在Invocation里面实现的。


## Client.call
client#call主要分为以下几步
1. 创建Call实例，Call类封装了rpc request, 也就是上面的Invocation，代表一个RPC call
2. 创建Connection - 当Client.getConnection被调用时，会先到一个hash表里面用ConnectionId作为key去查询。 ConnectionId由目标地址、protocal和ticket(UGI)构成，也就是说这三者唯一指定了一个Connection。如果在hash表里面查询的到，则直接返回，如果查询不到，创建一个新的Connection，然后返回。
Connection是客户端的核心类，主要作业是连接服务端、向服务端发送请求以及接收服务器的请求，可以看到Connection扩展了Thread，主要是启动线程来接收服务器端的返回。
在获取Connection之后，会调用当前connection的addCall, 把第一步里面创建的call加到当前connection的calls hash表里面，以callId为key。在这之后，会调用setupIOstreams方法去创建socket，连接服务端，设置data input /output stream，最后启动线程接收服务端。
3. 最后调用Connection#sendRpcRequest向服务端发送请求。一个rpc request向服务器传输的内容，也就是服务器接收到的内容，主要包含三部分，1) Rpc 请求头和Rpc 请求的总体长度 2）Rpc请求头，这个是用Call去生成 3) Rpc请求，实际上是调用了 Invocation#write，也就是Writable#write的实现。 当内容组装完成之后，用connection里面的output stream向服务端写入数据。
4. 调用call#wait()方法进入等待状态


至此，客户端发送RPC请求就完成了，下面看下服务端是如何接收请求并处理的。



## RPC Sever
在了解服务端是如何处理RPC请求之前，先了解下RCP服务。
在上述例子中可以看到RPC服务端创建是通过调用RPC.Builder的build方法完成的。前面也提到RPC实现方面分为两种：WritableRPCEngine和ProtoRPCEngine, 在这build方法里面，也是调用相应的实现类型下面的Server类，比如WritableRPCEngine，最后生成的Server就是WritableRPCEngine.Server实例。当然不管是哪种实现的Server，都是扩展了org.apache.hadoop.ipc.Server,。

从ipc.Server的构建函数与start()方法可以看出整个Server的大体处理流程入下:

![]({{ "/images/ipc-server.png" | absolute_url }})


接下去看下服务端是如何利用这个框架来接收与处理RPC请求。

## 服务端接收RPC Request
服务器在RPC Request的接收主要由Listener完成，Listener是一个线程，主要是对客户端的连接进行监听，一旦接受到客户端的连接之后，就会把这个连接交给Reader处理，目前采用简单轮训方式，主要实现的内容在Server.Listener#doAccept里面。可以看出Listener里面的操作是属于非常轻量级别的操作，所以只有一个线程来作分发。

服务端解析RPC Request
解析工作主要由多个Reader完成。Readerd会反序列化并解析客户端发送过来的请求，最后生成一个服务端的Server.Call实例，然后放到CallQueue队列中。Reader是在Server.Listener的构建函数启动的, 个数由Server构建时候传入numReaders参数决定，如果传入值为-1则，读取配置ipc.server.read.threadpool.size的值，默认是1，其他几个参数，如hanlder的个数和最大队列长度，获取过程与reader个数参数类似。
Reader也包含一个selector，并且注册了读取操作，当select有返回时就会调用doRead方法。在doRead里面首先调用selectionkey的attachment方法获取connection实例(这个connection是在Listener#doAccept里面调用ConnectionManager#register方法时候创建的)，接着调用Connection#readAndProcess, 在这里经过读取数据、检验格式、反序列化、生成Server.Call, 最后放到CallQueue队列中。

## 服务端执行Server.Call
 Handler会从CallQueue中拿到Server.Call（阻塞性地), 然后调用Server#call方法执行这个rpc Call, 执行完成后调用Responder的doRespond方法。而Server#call的具体调用根据RPCEngine的不同实现分别是ProtobufRpcEngine.Server.ProtoBufRpcInvoker#call和 WritableRpcEngine.Server.WritableRpcInvoker#call，这里以WritableRpcInvoker#call为例说下具体过程
1. 首先把远程rpcrequest（writable）转换成WritableRpcEngine.Invocation, 这个就是客户端在序列化方法名称和参数值等信息的时候用到的类
2. 通过调用RPC.Server#getProtocolImplMap方法用protocol名称找到对应的protocol实现和相应的对象, 也就是拿到这个Protocol的具体实现，这个具体实现是在RPC.Server在初始化的时候调用registerProtocolAndImpl添加进去的。
3. 在Protocol实现的对象上面使用反射机制执行客户端的方法，然后返回执行结果

## 服务端返回RPC处理结果
当Handler处理完成之后，会调用reponder来发送Call的执行结果给客户端，如果reponse的数量较小，则直接发送，如果一次发送不能发送全部数据，则注册到writeSelector等后续再来发送。

## 客户端接收处理结果
Client.Connection在启动的时候就调用了Client.Connection#receiveRpcResponse来接收服务端发来的响应。RPC Response的结构跟Request类似，也是由长度、响应头和响应本体组成，所以可以看到客户端接收响应的时候也是依次长度、响应头和本体。在成功读区RPC Response之后，会把这个call从calls(hash表)里面删除掉，意味着这个call已经从服务器端接收到消息了, 然后调用Call#setRpcResponse方法，最后会调用call.notify唤醒call(客户端发送request之后会调用call.wait方法进入等待状态)，然后Client#call返回从服务器接收到的response, 也就意味着客户端的代理方法会取到返回值。



## 小结


总的来说，Hadoop RPC的客户端采用了代理模式，可以使得客户端的RPC调用看起来跟本地调用一样；而服务端接受到客户端的请求之后，利用java里面反射机制执行具体的远程实现，其实服务段采用了一个典型的Reactor架构，底层是用java nio来实现，可以说是一个高性能的RPC服务实现，并且可以根据实际情况调整一些参数，比如namenode可以优化dfs.namenode.handler.count来控制handler的个数。
