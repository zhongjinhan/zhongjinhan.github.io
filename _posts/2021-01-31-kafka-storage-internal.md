---
layout:     post
title:      Kafka日志存储
date:       2021-01-31
summary:    介绍了Kafka日志存储内部构造
categories: kafka
published:  true
---

<!-- TOC depthTo:2 -->

- [1. Kafka日志子系统](#1-kafka日志子系统)
- [2. Kafka日志存储目录和文件](#2-kafka日志存储目录和文件)
- [3. LogManager](#3-logmanager)
    - [3.1. 日志的装载](#31-日志的装载)
    - [3.2. 日志获取与创建](#32-日志获取与创建)
    - [3.3. 日志的清理](#33-日志的清理)
- [4. Log 对象](#4-log-对象)
    - [4.1. Log Append](#41-log-append)
    - [4.2. Log Read](#42-log-read)
- [5. LogSegment](#5-logsegment)
    - [5.1. 日志记录文件FileRecords](#51-日志记录文件filerecords)
    - [5.2. Offset索引文件](#52-offset索引文件)
    - [5.3. 时间索引文件](#53-时间索引文件)
    - [5.4. Segment Append操作](#54-segment-append操作)
    - [5.5. Segment Read操作](#55-segment-read操作)
- [6. 日志格式](#6-日志格式)

<!-- /TOC -->



# 1. Kafka日志子系统

![]({{ "/images/kafka-storage-sub-system.png" | absolute_url }})

Kafka日志子系统主要包含以下部件

- LogManager：日志子系统的入口，在KafkaServer启动的时候被初始化，负责Log的管理
- Log：一个TopicPartition对应一个Log对象，逻辑上可以看作可以连续不断append的日志文件，但实际上Log进一步按段分成LogSegment
- LogSegment：一个Log里面的一段数据，LogSegment包含一个log、以及对应的offset index文件、time index以及跟事务相关的transaction index。

<br/>

# 2. Kafka日志存储目录和文件

KafKa Broker可以根据参数log.dirs配置多个数据文件夹，一个TopicPartition的数据会存放于数据文件夹下面的子文件夹，子文件夹的命名直接依据TopicPartition，样例如下：


![]({{ "/images/kafka-storage-log-dir.png" | absolute_url }})

可以看到一个TopicPartition下面会有多个LogSegment，每个LogSegment会包含log、index以及timeindex文件，这些文件除了后缀不一样，前面文件名称部分是一样的，20位数字的文件名称表示当前这个LogSegment的基础offset(baseOffset)，这个可以位offset查询快速定位到某个LogSegment。


<br/>

# 3. LogManager

LogManager是日志子系统的入口，外部子系统通过调用LogManager对应的方法来访问日志数据

<br/>

## 3.1. 日志的装载

当KafkaBroker启动的时候会去构建LogManager，LogManager会调用kafka.log.LogManager#loadLogs来装载所有日志，具体是通过扫描log.dirs配置的这些文件夹下面的partition文件夹，用这些文件夹下面的数据去构建Log对象，最后放到LogManager#currentLogs和futureLogs这两个map里面，map的key是TopicPartition对象，TopPartition是从partition文件夹名称解析出来，外部系统要引用这个TopicPartition的日志的时候就是通过这个map变量。

在装载过程中，并不是串行一一装载，而是往一个线程池里面提交kafka.log.LogManager#loadLog方法，线程个数由num.recovery.threads.per.data.dir控制，默认是一个dir有一个处理线程。

<br/>

## 3.2. 日志获取与创建

外部子系统是通过kafka.log.LogManager#getOrCreateLog方法来获取TopicPartition对应的日志对象，首先会调用kafka.log.LogManager#getLog查看是否存在，如果不存在则会创建日志。

一个Broker可以用log.dirs指定多个数据文件夹，新建日志默认情况下调用kafka.log.LogManager#nextLogDir来指定一个文件夹，规则非常简单，选取partition数量最小的那个文件夹。

具体Log对象的创建跟Log装载里面一样，调用的是kafka.log.Log#apply方法。


<br/>





## 3.3. 日志的清理

在KafkaServer里面构建LogManager之后会调用它的startup方法，这里面启动了多个后台线程来处理日志的清理。


<br/>

# 4. Log 对象

一个Log对象对应一个Kafka Topic Partition分片，由多个LogSegment构成，当需要对某个TopicPartition进行写入或者读取的时候会调用对应Log的append和read方法。


<br/>

## 4.1. Log Append

Log写入调用的是Log#append方法，主要流程如下图：


![]({{ "/images/kafka-storage-log-append-process.png" | absolute_url }})

1. analyzeAndValidateRecords: 调用Log#analyzeAndValidateRecords来验证传入的记录MemoryRecords。具体是遍历MemoryRecords里面的每个batch并加以验证，比如验证batch头里面的crc32。
2. 通过变量assignOffsets来确认是否需要分配offset：如果是写到leader partition，那么需要a分配offset，如果写到follower，那么不需要分配offset，直接使用record batch里面的值，这个非常容易理解，因为follower的日志是从leader replica过来，当然可以直接使用。
3. validateMessagesAndAssignOffsets: 调用LogValidator#validateMessagesAndAssignOffsets来验证消息并分配offset,这里面会根据是否有压缩、是否旧版本等因素调用不同的方法来处理，这里简单列下无压缩最新版本记录格式下的简单步骤，调用的是LogValidator#assignOffsetsNonCompressed方法：
    - 遍历每个batch，调用LogValidator#validateBatch继续验证batch内容
    - 遍历batch里面的每条record(LogValidator$#validateRecord), 每遍历一条record便给offsetCounter加上一个计数，外层使用了LongRef这个数据类型传入到方法里面以保证可引用，每当遍历一条record便加一，最后这个值 -1 之后赋值给了appendInfo.lastOffset
    - 调用MutableRecordBatch#setLastOffset来设置batch的第一个参数baseoffset，这个值等于传入的offset减去lastOffsetDelta
    - 调用MutableRecordBatch#setPartitionLeaderEpoch
    - 根据TimeStamp类型设置MutableRecordBatch#setMaxTimestamp

4. 是否滚动log: 就是是否创建一个新的LogSegment，需满足以下任意条件
    - segment大小到达上线，上限由由segment.bytes控制，默认1 * 1024 * 1024 * 1024
    - 超过一定的时间，如果这个segment的第一个batch有时间，则检查当前时间减去这个时间是否超过了限定时间，限定时间由由segment.ms控制，默认24 * 7 * 60 * 60 * 1000L；如果segment的第一个batch没有时间，则检查当前时间减去segment创建时间是否超限
    - 关联的offset index满了
    - 关联的time index满了
    - offset不能再append到offset index上面
  
5. 滚动日志：调用Log#roll方法，具体步骤如下
    - 获取newOffset，即即将要append的record的offset
    - 用dir和newOffset创建一个File，文件名里面包含offset
    - 检查当前segments map里面是否已经包含newOffset并作出处理
    - 如果不包含的话，创建offset index， 调用Log$#offsetIndexFile
    - 创建 time index， 调用Log#timeIndexFile
    - 创建transactionalindex，调用Log$#transactionIndexFile
    - 调用LogSegment#onBecomeInactiveSegment对log、time index和offset index作 trim操作
    - ProducerStateManager#updateMapEndOffset
    - 调用LogSegment$#open 来创建LogSegment并添加到segments


6. Segment Append: 调用LogSegment#append，后续LogSegment会详细描述
7. updateLogEndOffset: 调用updateLogEndOffset来更新end offset，这个offset是assign offset流程里面计算出来的赋给appendInfo.lastOffset + 1，再加上baseOffset等信息构建了LogOffsetMetadata








<br/>

## 4.2. Log Read

客户端或者其他broker需要消费partition数据的时候会调用对应的Log#read方法，简单流程如下:


![]({{ "/images/kafka-storage-log-read-process.png" | absolute_url }})

1. 从nextOffsetMetadata获取next offset
2. currentNextOffsetMetadata并跟startOffset对比，如果相等则直接返回空的FetchDataInfo
3. 调用NavigableMap#floorEntry方法从segements里面获取小于等于startOffset的最大那个segment
4. 先计算maxPosition，如果是lastSegment，maxPosition设置成lastoffset对应的physicalposition；如果不是lastsegment，设置成segment#size。因为读取的过程中可能会有segment rollout发生，所以需要判断两次
5. 调用LogSegment#read获取FetchDataInfo并返回



<br/>


# 5. LogSegment


Log由多个LogSegment组成，每个LogSegment包括日志记录文件、offset索引文件、时间索引文件以及可能会有的事务索引文件，每个文件名的后缀不一样，但是文件名是一样的，由这个LogSegment的基础offset补0到20位数字得出。


<br/>

## 5.1. 日志记录文件FileRecords

FileRecords代表日志在磁盘上面的对象，在内存里面的是MemoryRecords，日志记录最终会通过FileRecords写到磁盘上的文件。FileRecords主要包含一个文件句柄和一个文件Channel，另外提供了一些方法用以数据读取和数据写入。


<br/>

## 5.2. Offset索引文件

OffsetIndex文件的存储实现在AbstractIndex里面，Kafka把index文件mmap到内存里面，index文件的读写操作都通过操作系统page cache。

AbstractIndex最重要的变量mmap，可以看到mmap操作是通过java nio FileChannle#map方法实现。

```scala
  protected var mmap: MappedByteBuffer = {
    val newlyCreated = file.createNewFile()
    val raf = if (writable) new RandomAccessFile(file, "rw") else new RandomAccessFile(file, "r")
    try {
      /* pre-allocate the file if necessary */
      if(newlyCreated) {
        if(maxIndexSize < entrySize)
          throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
        raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize))
      }

      /* memory-map the file */
      _length = raf.length()
      val idx = {
        if (writable)
          raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, _length)
        else
          raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, _length)
      }
      /* set the position in the index for the next entry */
      if(newlyCreated)
        idx.position(0)
      else
        // if this is a pre-existing index, assume it is valid and set position to last entry
        idx.position(roundDownToExactMultiple(idx.limit(), entrySize))
      idx
    } finally {
      CoreUtils.swallow(raf.close(), this)
    }
  }
```

OffsetIndex以数组的方式组织数据，数组元素长度是固定的8个byte，由两个int类型的数值构成，两个数值分别代表relative offset和log文件的物理位置。



<br/>

### OffsetIndex写入

在OffsetIndex#append里面，步骤如下
1. 验证传入的offset，必须大于_lastOffset，_lastOffset是这个index里面最后一个entry的offset
2. 调用kafka.log.AbstractIndex#relativeOffset把offset转换成相对offset，相对offset = offset 减去 这个segment的baseoffset
3. 调用java.nio.ByteBuffer#putInt(int)方法把相对offset和position依次写入mmap
4. _entries加1，_lastOffset重置


<br/>

### OffsetIndex查找

在kafka.log.OffsetIndex#lookup实现:
1. duplicate mmap
2. kafka.log.AbstractIndex#largestLowerBoundSlotFor, 首先会查看目标offset是否在warnEntryies范围内，_warmEntries 大小= 8192/8 = 1024，指的是从最后开始往前推1024个。 如果是在warmentryies范围之内，则只在这个范围内作二分查找，否则全局二分查询，这算是根据实际场景的一个优化。
3. 调用kafka.log.OffsetIndex#parseEntry 把结果解析成OffsetPosition


<br/>

## 5.3. 时间索引文件

TimeIndex entry由timestamp和offset，这里要先弄清除timestamp和offset的关系是什么？关系是小于offset里面最大的时间为timestamp，任何时间戳大于teimstapm的记录的offset都大于OFFSET。

单个索引条目大小为12byte，由8个byte的timestamp和4个byte相对offset构成，相对offset


<br/>

### 时间索引写入

调用kafka.log.TimeIndex#maybeAppend:

1. 只处理timestamp大于最后一个索引条目的timestamp，保证timestamp是单向递增的
2. 往nmap写入时间戳，写入计算好后的相对offset
3. 递增entries，重置 _lastEntry


<br/>

### 时间索引查找

跟OffsetIndex一样


<br/>

## 5.4. Segment Append操作

kafka.log.LogSegment#append


1. 调用FileRecords#append 追加records
2. 如果累计的record bytes超过index.interval.bytes(默认4096)的时候，调用kafka.log.OffsetIndex#append往
   OffsetIndex里面追加一条offset和phycial location的纪录，另外调用kafka.log.TimeIndex#maybeAppend往time index里面追加一条entry。最后重置 bytesSinceLastIndexEntry
3. 累计bytesSinceLastIndexEntry


<br/>

## 5.5. Segment Read操作

kafka.log.LogSegment#read

1. 调用kafka.log.LogSegment#translateOffset方法来把offset转换成log文件里面对应的position，具体是通过kafka.log.OffsetIndex#lookup方法完成
2. 计算fetchSize
3. 调用FileRecords#slice，并和其他信息一起构建最后的FetchDataInfo








<br/>

# 6. 日志格式


在Kafka里面，日志格式粒度从粗到细分为三层，如下图所示(为简单起见只考虑基于2.2.1版本的最新格式):


![]({{ "/images/kafka-storage-log-format.png" | absolute_url }})

- Records: 代表一段日志，在磁盘和内存的实现分别是FileRecords和MemoryRecords，本身并没有格式，只是一段byte序列，逻辑上由多个连续的batch构成，调用Records#batches会返回RecordBatch的迭代器
- DefaultRecordBatch：RecordBatch的最新版本的默认实现，包含了元数据字段和Record序列，元数据字段总共占用了61个字节，具体schema如下：
    ```scala
     * RecordBatch =>
     *  BaseOffset => Int64
     *  Length => Int32
     *  PartitionLeaderEpoch => Int32
     *  Magic => Int8
     *  CRC => Uint32
     *  Attributes => Int16
     *  LastOffsetDelta => Int32 // also serves as LastSequenceDelta
     *  FirstTimestamp => Int64
     *  MaxTimestamp => Int64
     *  ProducerId => Int64
     *  ProducerEpoch => Int16
     *  BaseSequence => Int32
     *  RecordsCount => Int32
     *  Records => [Record]
    ```


- DefaultRecord: Record的默认实现，DefaultRecordBatch#iteator会返回一个DefaultRecord的迭代器。DefaultRecord里面的元数据长度不固定，因为为了提高存储效率，多个字段采用可变长类型，可变长类型的读取和写入实现上面借鉴了google protocal buffer http://code.google.com/apis/protocolbuffers/docs/encoding.html。元数据最长为21个字节，具体格式如下：
    ```scala
    * Record =>
    *   Length => Varint
    *   Attributes => Int8
    *   TimestampDelta => Varlong
    *   OffsetDelta => Varint
    *   Key => Bytes
    *   Value => Bytes
    *   Headers => [HeaderKey HeaderValue]
    *     HeaderKey => String
    *     HeaderValue => Bytes
    ```




---------------------------------
注：kafka源码基于apache kafka 2.2.1 
