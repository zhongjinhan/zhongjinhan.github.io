---
layout:     post
title:      Java 文件IO和Socket
date:       2020-11-08
summary:    简单介绍了java的文件IO和Socket相关API
categories: java
published:  true
---


## IO基础

IO即Input/Output，输入和输出，因为计算机计算的大部分场景都需要从外部数据源读取数据，常见的外部数据源包括文件、网络、键盘输入、控制台输出等等，这里主要考虑文件和网络。

<br/>

### 文件IO

Linux系统文件IO是通过系统调用完成，也就是open-read-write-close模式，例子如下：


```c
...
inputFd = open(filePath, O_RDONLY);
...
numRead = read(inputFd, buf, BUF_SIZE)
...
write(outputFd, buf, numRead)
...
close(inputFd)
```


从上面代码里面可以看到，站在用户程序的角度来看，数据的输入输出简单来讲其实就是摆弄buffer，读取的时候buffer被填满，写入的时候buffer被消耗。


下图张描述的是read系统调用的逻辑图，write调用流程跟read类似，只需要把上图箭头的方向反过来就差不多了。


![]({{ "/images/java-file-io-socket-io.png" | absolute_url }})

read流程可分为如下几个步骤：
1. 当用户进程需要读取数据的时候，会发起一个read()方法，read是操作系统提供的API，在执行的时候作了一次系统调用，向内核系统发起一个请求。在需要计算机系统资源的时候，都需要发起系统调用。
2. 内核接收到read之后，接会发送一个命令到磁盘控制器让它去获取磁盘的数据。
3. 磁盘控制器在获取数据后往kernel space的buffer使用DMA直接写入数据，这个写入过程是不需要CPU介入的。
4. 最后内核把buffer里面的数据拷贝到用户空间里面的buffer。


<br/>

### 网络IO

大部分语境下面，网络IO就是基于TCP/IP协议的Socket API，最早是在Unix BSD系统上面实现，考虑到网络交互的复杂性，并没有用传统的open-close-read-write范式，而是新增了系统调用。另外因为设计socket的时候，TCP/IP并没这么流行，所以也要考虑其他协议，所以设计成了一个通用的API，最典型的例子就是IP地址并不是一个简单的binary，而是要制定地址类型，但是现在大家默认他们是一对一关系。


Socket API列表如下，分客户端和服务端:

<table>
<tr>
    <th>API</th>                          
    <th>客户端</th>
    <th>服务端</th>
    <th>描述</th>
</tr>
<tr>
    <td>socket(pfam, type, protocol)</td>                 
    <td>●</td>                 
    <td>●</td>                 
    <td>socket方法生成了一个为应用程序提供可用于网络交互的描述符，pfam参数可以指定ipv4或v6，type 可指定TCP或者UDP</td>                 
</tr>

<tr>
    <td>close(descriptor)</td>                 
    <td>●</td>                 
    <td>●</td>                 
    <td>关闭一个socket的描述符</td>                 
</tr>
<tr>
    <td>bind(descriptor, localaddr, addrlen)</td>                 
    <td></td>                 
    <td>●</td>                 
    <td></td>                 
</tr>

<tr>
    <td>还有好几个，不想列举了</td>                 
    <td></td>                 
    <td></td>                 
    <td></td>                 
</tr>

</table>



<br/>

## Java文件IO


按package不同，Java文件IO相关API可分为

1. java.io下面的类，标准IO处理类，这里面进一步可以分为两类，分别是处理字节流的Stream相关类和处理字符流的Reader/Writr相关类 
2. java.nio下面的类，java1.4引入新的IO类，用buffer和channle来处理文件数据


<br/>

### 字节流IO

字节流处理的当然就是纯字节数组，读取和写入的都是字节数组，相关的两个抽象类分别是InputStream和OutputStream。

InputStream里面的方法有：


<table>
<tr>
    <th>方法</th>                          
    <th>描述</th>
</tr>
<tr>
    <td>int read()</td>
    <td>读取一个字节，返回一个int，范围是0-255</td>                 
</tr>
<tr>
    <td>int read(byte b[])</td>
    <td>读取一串字节</td>                 
</tr>

<tr>
    <td>int read(byte b[], int off, int len)</td>
    <td>从输入流读取一串字节，填充到字节数组里面，从off开始，填充len长度，但不能保证，最后读取的实际长度是方法返回值</td>                 
</tr>
<tr>
    <td>long skip(long n)</td>
    <td>在输入流中往前跳n个字节</td>                 
</tr>

<tr>
    <td>void close()</td>
    <td>关闭输入流并释放相应的资源</td>                 
</tr>
<tr>
    <td>int available() </td>
    <td>预估在没有阻塞的情况下还有多少字节可以读取</td>                 
</tr>
<tr>
    <td>void mark(int readlimit)  </td>
    <td>正常情况情读取会一直往前走，在需要重读的情况下可以用mark来标记位置，然后调用reset方法回到这个位置，是否可以mark需要调用markSupported()来确认，因为不一定所以子类都实现了</td>                 
</tr>
</table>




<br/>

OutputStream有如下5个:
 

<table>
<tr>
    <th>方法</th>                          
    <th>描述</th>
</tr>
<tr>
    <td>void write(int b) </td>
    <td>输出一个字节，范围是0-255</td>                 
</tr>
<tr>
    <td>void write(byte[] data) </td>
    <td>输出一个字节数组，一般来说文件使用1024字节长度，网络输出使用128字节</td>                 
</tr>

<tr>
    <td>void write(byte[] data, int offset, int length) </td>
    <td>输出一串字节，从offset位置开始，长度为length</td>                 
</tr>
<tr>
    <td>void flush() </td>
    <td>强制数据写入，这个buffer跟操作系统或者硬件级别的buffer不一样，这些buffer必须要调用FileDescriptor的sync方法。</td>                 
</tr>
<tr>
    <td>void close()</td>
    <td>关闭输出stream并释放对应的资源</td>                 
</tr>
</table>


<br/>


<br/>

#### Stream子类

下里列举了部分实现了InputStream和OutputStream的类

- FileInputStream/FileOutputStream：从文件读取数据流或者写入数据流
- FilterInputStream/FilterOutputStream：在具体的Stream，比如文件Stream外面包括一层逻辑，作一些过滤和转换
- BufferedInputStream/BufferedInputStream：扩展了FilterStream，内置了一个可指定大小的buffer，输入或者输出的数据会先放到这个buffer里面。
- DataInputStream/DataOutputStream：扩展了FilterStream，可以读取或者写入Java原生数据类型




<br/>

### 字符流IO

很多时候需要直接读取字符或者把处理好的字符输出到目标，而略去对字节的处理，字符流相关类就是为此而生。字符流的两个抽象超类分别是Reader和Writer，里面的抽象方法跟InputStream和OutputStream非常类似，只是把字节数组换成字符数组。


字符流IO相关部分子类如下：

- InputStreamReader/OutputStreamWriter：字节流和字符流之间的转换器，所以这里面肯定设置了字符集，用来在字节流和字符流之间的编码和解码
- FileReader/FIleWriter：扩展了InputStreamReader/OutputStreamReader，直接用文件名称生成了inputstream和outputstream。
- BufferedReader/BufferedWriter: 内置了一个字节buffer，跟BufferedStream类似



<br/>


### Buffer和Channel

Java在1.4的引入了新的IO相关的API，放在package java.nio下面，其中有两个重要的概念是Buffer和Channel。


<br/>

#### Buffer

Buffer相当于是普通Java类和Channel之间的连接，buffer存储了固定大小的数据，跟标准IO里面read和write方法字节数组参数非常类似，但是形成了一个类，并且提供了一些相当遍历的方法，省去了去字节数组的操作，另外同一个Buffer可以被输入和输出同时使用。

Buffer有4个属性：
- capacity： 总大小，buffer创建的时候指定，不能更改。
- limit: 活数据,初始值等于capacity
- position: 初始值等于0
- mark: 初始为未定义，定义过后，在调用reset之后position会退到mark的位置


下面看下数据写入过程中buffer这些属性是如何变化的，也就是先把数据写入buffer，然后再把buffer里面的数据通过channel写入到IO服务。
1. 数据写入buffer，postiion移动到写入的长度位置，limit还是等于capacity
2. 在channel从buffer读取数据之前，先调用flip(),这时候position等于0, limit = position + 1
3. channel从buffer读取数据




<br/>

#### Channel

 
Channel是一个全新的概念，不是标准IO里面抽象类的增强或者扩展，它提供了到IO服务的一个直接连接，可以把它看作是Buffer和IO服务间的一个管道，IO服务通常是一个文件或者socket，并且在大多数情况下channel和文件描述符之间是一对一关系。另外跟标准IO不同的是，Channel是线程安全的。


下面是实现了Channel主要类的类图：


![]({{ "/images/java-file-io-socket-channel.png" | absolute_url }})


这些类都是抽象类，具体实现是操作系统各自native实现，主要分为两类

- 文件IO相关: FileChannel
- Socket相关:ServerSocketChannel, SocketChannel和DatagramChannel



Socket相关后面会提到，FileChannel可以通过标准IO包里面的FileInputStream、RandomAccessFile等类的getChannel方法获取，获取之后就可以进行读写操作了。

```java
RandomAccessFile raf = new RandomAccessFile(path, "r");
FileChannel fileChannel = raf.getChannel();
ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4);
fileChannel.read(byteBuffer)
```

<br/>

#### nio 性能

相比标准IO库中的stream和char API，用nio的buffer和channel来处理文件在实际使用场景中性能会有一定的提升，但不是很大，比如可能会有10%或者20%的提升，nio最大的影响是socket API。

<br/>

## Java Socket IO

在nio出来之间，Java网络编程使用的是java.net下面的Socket API，这些API只能实现阻塞IO，也就是说服务端需要为每个客户端创建一个线程来读取客户端的数据，数据没有的时候必须阻塞着，没有办法检查socket数据是否准备好了。Java 1.4以后，java.nio提供了非阻塞IO的API，所以java.net下面的socket API后面基本就没什么人用了。


<br/>

### Selector

nio里面的非阻塞IO的核心是Selector类，selector最终调用了系统api select()。Selector作用在SelectableChannel上面, 继承了SelectableChannel的类都可以注册selector，相对地，文件channel就不可以使用selector了。

一个selector可以被注册到多个selectablechannel上面，比如一个进程里面的两个socketserverchannl绑定在不同的两个端口上面可以注册同一个selector，后面就可以用同一个selector来处理连接。



selectablechannel可以让socket channel选择block或者非block模式。SelectableChannel 采用如下方式设置block或者non blocking
 - SelectableChannel configureBlocking(boolean block):  设置blocking, true为block
 - boolean isBlocking() : 检查是否blocking
 - blockingLock(): 用来阻止其他线程修改blocking状态


继承SelectableChannel有如下三个Channel

- ServerSocketChannel: 在TCP/IP 协议里面，ServerSocketChannel就像是服务端(server), 用来监听来自客户端的连接。
- SocketChannel：SocketChannel是这3个里面用的最多的，当相遇是TCP里面的客户端。
- DatagramChannel：可以使用DatagramChannel实现UDP传输




<br/>

#### Selection Key

Selection Key代表一个SelectableChannel和Selector之间的注册关系。当一个SelectableChannel调用register方法注册一个selector之后，一个Selectionkey 会被创建，当一个key调用cancel、或者对应的channel关闭或者对应的selector关闭的时候，SelectionKey会被视为无效。

一个Selection Key包含两个Operation 集合
1. interest key集合：注册的时候被指定
2. ready key集合: 初始化成0，selection操作的时候ready key会被更新，不能直接被用户更新


每个SelectableChannel都有自己定义的有效operation-set bit， 可以用方法validOps查看。用户可以定义一些应用数据以附件的形式添加到SelectionKey上面，这是通过方法attach和attachment来完成。


一个Selector维护了3个集合的selection key
1. 方法keys返回，这些key代表了selector和selectablechannel的关系。当selectable channel注册selector后会返回一个SelectionKey，这个Selectionkey就会保存在这个集合里面。一个selector可以被多个channel注册，这些selection key都会保存在这个集合里面。
2. selectedKeys方法返回，是上面keys里面的集合：当所关联的channel所涉及到的操作准备好的时候，对应的key就会在这个集合里面。也就是在selection的过程中产生的。
3. cancelledKeys，调用cancel方法或者对应的channel关闭的时候，selectionkey会放到这个集合里面。取消key之后，下一个selection流程会注销掉对应的channel，把这个取消的selection key从keys集合里面移除。


<br/>

#### Selection流程


调用Selector#select或者select(timeout)或者selectNow()方法触发selection流程，流程有3步

1. 把cancel key集合里面的key从总集合里面移除并注销对应的selectable channel，这个步骤完成之后cancel集合里面为空。
2. 调用操作系统API检查keys对应channel的各个操作的状态，如果一个channel的任意一个操作处于准备状态，则会执行以下其中一个动作
    1. 如果channel的key不在select key set里面，则添加channel key到select key set并设定ready operation set
    2. 否则，在select key set里面保留现有的channel key，然后添加对应ready operation set
3. 如果步骤2执行过程中有新的元素添加cancel key集合，则再次执行步骤1










