---
layout:     post
title:      Spark是如何读写Parquet文件
date:       2020-03-29
summary:    详细介绍了spark读写parquet的过程
categories: spark
published:  true
---
{:toc}


Paruqet文件格式目前是大数据圈子里用的比较多的列式文件存储格式，Spark写入HDFS默认就是这种格式，虽然在写入的性能比起文本要差些，但是读取性能太好了，对于大数据平台上面分析场景居多的情况下，基本就是不二之选了。

因为是文件格式，属于比较底层的一种实现，虽然每天都会使用这个文件格式，一般来说不会去深究它的底层实现，但是随着使用的深入，平时工作中还是不可避免地会遇到与之相关的问题，比如有时候在读取或者写入特定parquet文件的时候会发生OOM的情况，所以对这个文件格式进行一定层次的了解还是非常有必要，不至于在遇到这类问题的时候无从下手。

在具体描述Spark是如何写入和读取文件之前，先来看下Paruqet的文件格式。


<br/>

## Parquet文件格式

以下是官方网站上面提供的一张文件格式描述图，从图中可以看出一个parquet文件可以由4部分构成

1. 文件头的MagicNumber：4字节 “PAR1"，用来验证是否是parquet文件
2. Row Groups：数据内容按Row水平切分成若干个Row Group
    - ColumnChunk：在一个Row Group里面，对于每一列都有一个ColumnChunk
        - Page: 每个Column Chunk包含若干个Page, 从压缩和编码层面来讲，Page属于不可再分割的最小单元了。Page包含Page header、repetitionlevel数据、definition level数据以及编码后的存储数据。关于reptition level和definition level的概念可以从dremal相关的这篇论文里面详细了解 https://research.google/pubs/pub36632
3. Footer：包含文件metadata和footer自身所占用的长度
    - FileMetaData： metadata也分为三种级别，对应于数据的Row Group、ColumnChunk和Page。
    - Footer length：用4个字节，也就是一个int来存储footer的长度。
4. 文件尾的MagicNumber：4字节“PAR1”,用来验证是否是parquet文件


![]({{ "/images/spark-parquet-format.png" | absolute_url }})






<br/>





## Paruqet Tool

有时候需要快速直接查看一个parquet 文件的metadata，那么就可以使用parquet tool这么一个官方提供的命令行工具。这工具可以直接从maven仓库下载，比如

```
wget https://repo1.maven.org/maven2/org/apache/parquet/parquet-tools/1.8.2/parquet-tools-1.8.2.jar
```

下载完成之后可以用java或者hadoop命令来运行, 从help信息里面可以看到parquet tool除了可以查看metadata，还可以使cat、head或者dump选项来查看数据。
```
java -jar ./parquet-tools-1.8.2.jar --help
hadoop jar ./parquet-tools-1.8.2.jar --help
```


使用如下命令查看metadata，从结果里面也可以进一步地了解parquet文件格式，比如我们可以看到每个row group的信息和Column Chunk的信息
- Row Group： 逐个显示每个Row Group的 总行数(RC)、总大小(TS)以及在文件中的偏移量
- Column Chunk：显示每个column的名称、类型、压缩方式、大小(压缩、非压缩和压缩比例) 等等信息

```
hadoop jar ./parquet-tools-1.8.2.jar meta /test.snappy.parquet

row group 1:  RC:3762 TS:178905908 OFFSET:4 
---------------------------------------------------------------------------
ID:            INT64 SNAPPY DO:0 FPO:4 SZ:15091/30149/2.00 VC:3762 ENC:PLAIN,BIT_PACKED,RLE
DATA1:         INT96 SNAPPY DO:0 FPO:15095 SZ:6259/10047/1.61 VC:3762 ENC:PLAIN_DICTIONARY,BIT_PACKED,RLE
DATA2:         INT32 SNAPPY DO:0 FPO:21354 SZ:343/359/1.05 VC:3762 ENC:PLAIN_DICTIONARY,BIT_PACKED,RLE
DATA_TYPE1:    INT32 SNAPPY DO:0 FPO:21697 SZ:1026/1019/0.99 VC:3762 ENC:PLAIN_DICTIONARY,BIT_PACKED,RLE
DATA_TYPE2:    INT32 SNAPPY DO:0 FPO:22723 SZ:1030/1023/0.99 VC:3762 ENC:PLAIN_DICTIONARY,BIT_PACKED,RLE
...

row group 2:  RC:1382 TS:64439450 OFFSET:134217728 
---------------------------------------------------------------------------
ID:            INT64 SNAPPY DO:0 FPO:134217728 SZ:5610/11108/1.98 VC:1382 ENC:PLAIN,BIT_PACKED,RLE
DATA1:         INT96 SNAPPY DO:0 FPO:134223338 SZ:3474/7060/2.03 VC:1382 ENC:PLAIN_DICTIONARY,BIT_PACKED,RLE
DATA2:         INT32 SNAPPY DO:0 FPO:134226812 SZ:146/147/1.01 VC:1382 ENC:PLAIN_DICTIONARY,BIT_PACKED,RLE
...

```


<br/>

## Spark写入Parquet文件

<br/>
**spark 写入parquet相关类图**

跟其他数据源一样，parquet格式在spark里面也实现了FileFormat的ParquetFileFormat，相关的主要类的类图如下:


![]({{ "/images/spark-parquet-write-class.png" | absolute_url }})


<br/>

**方法prepareWrite**


prepareWrite方法实现的是FileFormat这个接口相应的方法，spark官方支持的源类型都实现了这个接口，在parquet的实现里面，主要是做一些文件写入之前的准备工作，包括设置committer类、设置WriterSupportClass、设置一些parquet相关的参数等，然后返回一个OutputWriterFactory。这里要注意的是，到这个点为止的代码都是在Spark Driver上面执行的，而接下去的OutputWriterFactory的newInstance方法是在SingleDirectoryWriteTask或者DynamicPartitionWriteTask里面被调用，也就是在Spark Executor上面被执行的。

OutputWriterFactory#newInstance方法返回一个ParquetOutputWriter,ParquetOutputWriter包含一个由recordWriter，最后调用这个recordWriter的write方法来写入数据，recordWriter是由ParquetOutputFormat#getRecordWriter返回。而在ParquetOutputFormat#getRecordWriter里面，实际返回的是用ParquetRecordWriter包裹的InternalParquetRecordWriter, 这个才是record写入实现的核心类。



```scala
private[parquet] class ParquetOutputWriter(path: String, context: TaskAttemptContext)
  extends OutputWriter {

  private val recordWriter: RecordWriter[Void, InternalRow] = {
    new ParquetOutputFormat[InternalRow]() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        new Path(path)
      }
    }.getRecordWriter(context)
  }

  override def write(row: InternalRow): Unit = recordWriter.write(null, row)

  override def close(): Unit = recordWriter.close(context)
}
```
<br/>
**InternalParquetRecordWriter**

当InternalParquetRecordWriter被创建的时候会调用initStore初始化ColumnChunkPageWriteStore, 然后根据版本来初始化ColumnWriteStoreV1或者ColumnWriteStoreV2，接着以RecordConsumer的形式传入ParquetWriteSupport

```java
private void initStore() {
  pageStore = new ColumnChunkPageWriteStore(compressor, schema, pageSize);
  columnStore = parquetProperties.newColumnWriteStore(
      schema,
      pageStore,
      pageSize);
  MessageColumnIO columnIO = new ColumnIOFactory(validating).getColumnIO(schema);
  writeSupport.prepareForWrite(columnIO.getRecordWriter(columnStore));
}
```


随后当有记录需要写入文件的时候调用ParquetWriteSupport#write方法把记录写入store，也就是内存缓冲区，因为涉及到内存使用，所以不会一直写下去，必须要在满足特定条件之后就把缓冲区里面的数据flush到磁盘，这部分工作是在方法checkBlockSizeReached里面完成。

```java
public void write(T value) throws IOException, InterruptedException {
  writeSupport.write(value);
  ++ recordCount;
  checkBlockSizeReached();
}
```
<br/>
**检查缓冲区大小的条件**

判断是否flush磁盘的检查条件当然就是内存使用情况，但是检查内存使用情况的消耗比较大，所以不会在每次写完一条数据的时候就检查，而是当写入记录条数大于变量recordCountForNextMemCheck的时候检查下内存大小，这个变量的初始值是MINIMUM_RECORD_COUNT_FOR_CHECK，也就是固定值100, 也就是说第一次写入100条的时候会检查内存使用情况，

```java
private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
```


而后续recordCountForNextMemCheck会根据是否flush磁盘设定不同的值,
- 如果flush没有被触发，会用nextRowGroupSize（初始值是RowGroupSize,默认是128 * 1024 * 1024)除以已经读取数据的平均大小再除2，也就是读到RowGroupSzie限制内的一半记录时再检查，但是上限不会超过MAXIMUM_RECORD_COUNT_FOR_CHECK
- 如果flush被触发，则会再检查额外的当前记录数的一半数量的记录，并且不会低于MAXIMUM_RECORD_COUNT_FOR_CHECK

```java
  private void checkBlockSizeReached() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) {
      long memSize = columnStore.getBufferedSize();
      long recordSize = memSize / recordCount;
      if (memSize > (nextRowGroupSize - 2 * recordSize)) {
        LOG.info(format("mem size %,d > %,d: flushing %,d records to disk.", memSize, nextRowGroupSize, recordCount));
        flushRowGroupToStore();
        initStore();
        recordCountForNextMemCheck = min(max(MINIMUM_RECORD_COUNT_FOR_CHECK, recordCount / 2), MAXIMUM_RECORD_COUNT_FOR_CHECK);
        this.lastRowGroupEndPos = parquetFileWriter.getPos();
      } else {
        recordCountForNextMemCheck = min(
            max(MINIMUM_RECORD_COUNT_FOR_CHECK, (recordCount + (long)(nextRowGroupSize / ((float)recordSize))) / 2), 
            recordCount + MAXIMUM_RECORD_COUNT_FOR_CHECK 
            );
        if (DEBUG) LOG.debug(format("Checked mem at %,d will check again at: %,d ", recordCount, recordCountForNextMemCheck));
      }
    }
  }
```


这种检查方式一般情况下是没有问题的，但是遇到某些非常特殊的数据就可能会出现内存使用超量的情况造成OOM，比如说数据源总共由6000条数据，前面100条数据平均每条不到1KB，而剩余的5900条数据平均大小达到了40KB，那么下次检查的时候全部6000条数据会被读进内部，buffer的内存使用量就很大了，对于executor内存设置较小的情况下很可能就会造成OOM。解决这种特殊场景的问题其实也很简单，只要能定制MAXIMUM_RECORD_COUNT_FOR_CHECK和MINIMUN_RECORD_COUNT_FOR_CHECK这两个变量就可以了。


当然导致OOM最关键的还是这块内存不被Spark的MemoryManager所管理，所以是不被感知的，如果只是一个job在运行，那么一般来说不会有什么问题，但是由多个并行的Job在做shuffle的时候就会有比较大的几率造成OOM。
<br/>

**缓冲区flush条件**

当检查内存之后，如果发现缓冲区内存大于(nextRowGroupSize - 2 * recordSize)就会调用flushRowGroupToStore方法把数据从缓冲区flush到磁盘，nextRowGroupSize的初始值是rowGroupSize，默认是128 * 1024 * 1024, 在spark里面我们可以通过两种方法设置
1. 通过hadoopConf设置全局值: spark.sparkContext.hadoopConfiguration.setInt("parquet.block.size", 64 * 1024 * 1024)
2. 通过write.option设置只影响单次写入: write.option("parquet.block.size","67108864")


nextRowGroupSize在方法flushRowGroupToStore里面flush之后会被重新设置，新的值从ParquetFileWriter.PaddingAlignment#nextRowGroupSize方法里面获取, 从这个方法里面可以看到，这里根据条件会返回 (dfsBlockSize - out.getPos % dfsBlockSize), out.getPos就是文件当前的offset，dfsBlockSize是HDFS默认的block大小,
```java
public long nextRowGroupSize(FSDataOutputStream out) throws IOException {
  if (maxPaddingSize <= 0) {
    return rowGroupSize;
  }
  long remaining = dfsBlockSize - (out.getPos() % dfsBlockSize);
  if (isPaddingNeeded(remaining)) {
    return rowGroupSize;
  }
  return Math.min(remaining, rowGroupSize);
}
```
<br/>
**padding至HDFS block长度**

可以看出来在flush row group到文件里面去的时候，大小会尽量往HDFS block上面靠，并且默认情况下会作padding填充至HDFS block的大小，也就是说当离HDFS block长度小于8 * 1024 * 1024的时候就会调用padding把这8M填满。padding也是在ParquetFileWriter这个类里面来做，可以从下面代码看到ParquetFileWriter#startBlock方法里面会调用alignForRowGroup方法来填充一系列的zeros(new byte[4096]), 然后再写入column chunk数据
```java
public void startBlock(long recordCount) throws IOException {
  state = state.startBlock();
  if (DEBUG) LOG.debug(out.getPos() + ": start block");
  alignment.alignForRowGroup(out);
  currentBlock = new BlockMetaData();
  currentRecordCount = recordCount;
}

public void alignForRowGroup(FSDataOutputStream out) throws IOException {
  long remaining = dfsBlockSize - (out.getPos() % dfsBlockSize);
  if (isPaddingNeeded(remaining)) {
    if (DEBUG) LOG.debug("Adding " + remaining + " bytes of padding (" +
        "row group size=" + rowGroupSize + "B, " +
        "block size=" + dfsBlockSize + "B)");
    for (; remaining > 0; remaining -= zeros.length) {
      out.write(zeros, 0, (int) Math.min((long) zeros.length, remaining));
    }
  }
}
```


这所以这么做的主要原因是不想让row group跨block，因为当应用比如spark从HDFS读取的时候，每个task会处理一个row group，如果row group跨block，那么有可能这个task会消耗网络带宽从别的节点上面读取数据。

下图是一个用spark生成的parquet文件的meta信息，HDFS block大小是128M，写入时候parquet.block.size设置的64M。可以看到总共生成了4块128M的HDFSblock，分别在row group 6，11，16，21的时候将大小填充至一块HDFS

```
row group 1:    RC:350526 TS:163330195  OFFSET:4 
row group 2:    RC:53452  TS:25494613   OFFSET:95986697 
row group 3:    RC:31313  TS:15026416   OFFSET:111013114 
row group 4:    RC:17966  TS:8581273    OFFSET:119874361 
row group 5:    RC:11527  TS:5734682    OFFSET:124947494 
row group 6:    RC:340948 TS:163070685  OFFSET:134217728  <- 128M * 1
row group 7:    RC:55749  TS:24411187   OFFSET:229709983 
row group 8:    RC:31318  TS:14131209   OFFSET:244461074 
row group 9:    RC:19584  TS:8931647    OFFSET:252936432 
row group 10:   RC:12729  TS:5974964    OFFSET:258300597 
row group 11:   RC:369604 TS:161490908  OFFSET:268435456  <- 128M * 2
row group 12:   RC:62534  TS:26444779   OFFSET:361730955 
row group 13:   RC:34116  TS:15666027   OFFSET:377648627 
row group 14:   RC:18341  TS:8915917    OFFSET:387003385 
row group 15:   RC:12601  TS:6311100    OFFSET:392301759 
row group 16:   RC:383736 TS:162615849  OFFSET:402653184  <- 128M * 3
row group 17:   RC:59742  TS:25511045   OFFSET:497724375 
row group 18:   RC:33458  TS:14791297   OFFSET:513104849 
row group 19:   RC:18782  TS:8686375    OFFSET:521972409 
row group 20:   RC:12277  TS:5772742    OFFSET:527171008 
row group 21:   RC:103110 TS:42707030   OFFSET:536870912  <- 128M * 4
```



<br/>
## Spark读取Paruqet文件

Spark读取parquet时候相关主要类的类图如下：

![]({{ "/images/spark-parquet-read-class.png" | absolute_url }})


读取parquet的入口方法是ParquetFileFormat#buildReaderWithPartitionValues，该方法返回一个类型位(PartitionedFile) => Iterator[InternalRow]的值函数，最后会在FileScanRDD#compute方法里面被调用，也就是说读取的task会对每一个PartitionFile里面调用这个函数一次，首先我们来看一下Spark再读取文件的时候这个PartitionedFile是如何生成的。



**Parquet InputSplit**

不管是Mapreduce还是Spark在读取HDFS文件的时候都会先把文件按一定的规定分成若干个Inputsplit，然后再用若干个task去分布式地处理这些input split。那么对于parquet文件也不例外，spark也会事先生成一批inputsplit，具体实现是在FileSourceScanExec#createNonBucketedReadRDD, 当然这里面的逻辑不仅仅适用于parquet数据源，也适用于文本等其他数据源。

首先比较关键的是maxSplitBytes这个变量的生成, 由若干个参数决定，总的来说这些参数是要把这个值控制在一定的范围之内，不会太大，也不会大小：
- 初始值：用源文件总体大小除以spark.default.parallelism
- 不会太大：不会大于spark.sql.files.maxPartitionBytes，默认是128M
- 不会太小: 不会小于spark.sql.files.openCostInBytes，默认是4M


```scala
val defaultMaxSplitBytes =
  fsRelation.sparkSession.sessionState.conf.filesMaxPartitionBytes
val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
val defaultParallelism = fsRelation.sparkSession.sparkContext.defaultParallelism
val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
val bytesPerCore = totalBytes / defaultParallelism

val maxSplitBytes = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
  s"open cost is considered as scanning $openCostInBytes bytes.")
```


然后接下来以maxSplitBytes为一个单位，把大文件拆分成若干个等分，把小文件合并到一个等分。如下图所示，这个过程分成两个阶段：
1. 第一阶段从源文件生成一系列PartitionedFile, PartitionedFile会包含一个文件、开始offset和长度。 对于File1这样比较大的文件，会按maxSplitBytes切分成两个PartitionedFile, 假设文件大小1024，maxSplitBytes为512，那么第一个的开始offset从0,大小为512,第二个从512到文件结束；对于File2、3、4这样的小文件，则不会做切割，直接生成PartitionedFile, start offset为0，长度就是文件长度。
2. 第二个阶段从PartitionedFiles生成FilePartition, 如果PartitionedFile长度等于maxSplitBytes直接生成一个FilePartition； 如果多个PartitionFile小到可以可以装进一个maxSplitBytes, 则会合并生成一个FilePartition




![]({{ "/images/spark-parquet-input-split.png" | absolute_url }})




刚接触到这段代码的时候，PartitionedFile和FilePartition可能会让人有点晕。总的来说RDD里面的一个partition就是一个FilePartition，也就是每个读数的task会处理一个FileParition，而一个FileParition里面会包含多个PartitionedFile, 所以前面提到的ParquetFileFormat#buildReaderWithPartitionValues所返回的函数会逐个处理这些PartitionedFile, 可以看到在这个值函数里面会把PartitionedFile转换成ParuqetInputSplit

```scala
val fileSplit =
  new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty)

val split =
  new _root_.parquet.hadoop.ParquetInputSplit(
    fileSplit.getPath,
    fileSplit.getStart,
    fileSplit.getStart + fileSplit.getLength,
    fileSplit.getLength,
    fileSplit.getLocations,
    null)
```
<br/>
**Vectorized Parquet Decoding**

Spark提供了两种读取Parquet的方式，分别是源生的ParquetRecordReader和Spark定制的VectorizedParquetRecordReader，由参数spark.sql.parquet.enableVectorizedReader控制，默认是选择VectorizedParquetRecordReader，因为在解码的时候效率会高一些，但是内存消耗也会稍微高一些，如果在内存比较吃紧的场景下，可以选择使用源生的ParuquetRecordReader。
<br/>

**Parquet读取的并行度**

从官方网站的文档上面我们可以看到一个并行度处理级别上的说明

```
Unit of parallelization
  · MapReduce - File/Row Group
  · IO - Column chunk
  · Encoding/Compression - Page
```

从小到大来看，编码和压缩可以在Page级别来做，IO可以在Column Chunk进行并行从左，mapreduce任务可以在Row Group级别进行并行操作，这意味着Spark读取也可以在Row Group级别进行操作, 那么问题就来了，spark是如何拆分parquet文件到row group的? 或者说ParuqetInputSplit是如何跟RowGroup关联起来的?
<br/>

**读取Footer信息**

首先不管是VectorizedParquetRecordReader还是ParquetRecordReader, 都会调用initialize方法对split进行初始化操作，在初始化里面会调用ParquetFileReader#readFooter方法读取metadata信息，这段代码如果跟Parquet文件格式联合起来看，那么就非常容易理解了

- 首先读取文件总大小，减去长度为4 bytes的一个int，这个int是用来存储footer长度，然后再减4，这个是magic number,也就是PAR1，这样就找到footer长度的位置了。
- 调用seek找到footer长度位置之后，读取4 bytes的int，也就是footer的长度，然后在读取magic number并验证
- 用footer长度位置减去footer的长度就可以得到footer开始的位置，这样就可以开始读取footer里面的metadata信息了，这里调用了convert.readParuqetMetadata

```java
  public static final ParquetMetadata readFooter(Configuration configuration, FileStatus file, MetadataFilter filter) throws IOException {
    FileSystem fileSystem = file.getPath().getFileSystem(configuration);
    FSDataInputStream f = fileSystem.open(file.getPath());
    try {
      long l = file.getLen();
      if (Log.DEBUG) LOG.debug("File length " + l);
      int FOOTER_LENGTH_SIZE = 4;
      if (l < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) {
        throw new RuntimeException(file.getPath() + " is not a Parquet file (too small)");
      }
      long footerLengthIndex = l - FOOTER_LENGTH_SIZE - MAGIC.length;
      if (Log.DEBUG) LOG.debug("reading footer index at " + footerLengthIndex);

      f.seek(footerLengthIndex);
      int footerLength = readIntLittleEndian(f);
      byte[] magic = new byte[MAGIC.length];
      f.readFully(magic);
      if (!Arrays.equals(MAGIC, magic)) {
        throw new RuntimeException(file.getPath() + " is not a Parquet file. expected magic number at tail " + Arrays.toString(MAGIC) + " but found " + Arrays.toString(magic));
      }
      long footerIndex = footerLengthIndex - footerLength;
      if (Log.DEBUG) LOG.debug("read footer length: " + footerLength + ", footer index: " + footerIndex);
      if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
        throw new RuntimeException("corrupted file: the footer index is not within the file");
      }
      f.seek(footerIndex);
      return converter.readParquetMetadata(f, filter);
    } finally {
      f.close();
    }
  }
```

<br/>
**过滤Footer**

在ParquetMetadataConverter#readParquetMetadata方法可以看到会用filter对footer的内容进行过滤，这里还使用了访问者模式，因为之前传进来的其实是RangeMetdataFiler, 也就是文件offset的range，所以这里调用的是filterFileMetadata(_, RangeMetadataFilter)这个函数

```java
public ParquetMetadata readParquetMetadata(final InputStream from, MetadataFilter filter) throws IOException {
  FileMetaData fileMetaData = filter.accept(new MetadataFilterVisitor<FileMetaData, IOException>() {
    @Override
    public FileMetaData visit(NoFilter filter) throws IOException {
      return readFileMetaData(from);
    }
    @Override
    public FileMetaData visit(SkipMetadataFilter filter) throws IOException {
      return readFileMetaData(from, true);
    }
    @Override
    public FileMetaData visit(RangeMetadataFilter filter) throws IOException {
      return filterFileMetaData(readFileMetaData(from), filter);
    }
  });
  if (Log.DEBUG) LOG.debug(fileMetaData);
  ParquetMetadata parquetMetadata = fromParquetMetadata(fileMetaData);
  if (Log.DEBUG) LOG.debug(ParquetMetadata.toPrettyJSON(parquetMetadata));
  return parquetMetadata;
}
```


在这个方法我们终于可以看到offset range和row group是如何确定关联关系的，其实也非常简单，如果这个offset开始和结束包含row group的中点，就把这个row group划分在这个range之内，也就是把这个row group划分在这个input split之内。


```java
  static FileMetaData filterFileMetaData(FileMetaData metaData, RangeMetadataFilter filter) {
    List<RowGroup> rowGroups = metaData.getRow_groups();
    List<RowGroup> newRowGroups = new ArrayList<RowGroup>();
    for (RowGroup rowGroup : rowGroups) {
      long totalSize = 0;
      long startIndex = getOffset(rowGroup.getColumns().get(0));
      for (ColumnChunk col : rowGroup.getColumns()) {
        totalSize += col.getMeta_data().getTotal_compressed_size();
      }
      long midPoint = startIndex + totalSize / 2;
      if (filter.contains(midPoint)) {
        newRowGroups.add(rowGroup);
      }
    }
    metaData.setRow_groups(newRowGroups);
    return metaData;
  }
```

<br/>

**ParquetInputSplit和RowGroup**

从上面可以看出ParuqetInput和RowGroup的关系在读取metadata信息的时候就确定了，下面来看下具体的一个例子。

假设有一个文件Parquet文件大小为384M，包含3个RowGroup, 每个分别是128M(简单起见，不考虑metadata和其他的大小了), 生成的ParquetInputSplit由10个，每个长度分别是38.4M， 那么这10个input split和3 个row group的对应关系如下：


![]({{ "/images/spark-parquet-split-rg.png" | absolute_url }})

这里row group2 的中位点刚好卡在两个split之间，但是从代码里面可以看到大于等于开始offset并且小于结束offset，所以属于后面这个split。

```java
    boolean contains(long offset) {
      return offset >= this.startOffset && offset < this.endOffset;
    }
```


<br/>

**具体读取细节**


就是对row group的读取，然后对page的解压和解码，比较无聊，就不赘述了。



<br/>
<br/>

-------
注：spark代码基于cloudera spark，版本release 2.3.0.cloudera3， parquet代码基于cdh5.13.3-release