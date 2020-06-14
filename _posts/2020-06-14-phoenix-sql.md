---
layout:     post
title:      Apache Phoenix SQL解析与执行
date:       2020-06-14
summary:    phoenix sql全流程详解
categories: phoenix
published:  true
---


Apache Phoenix让你可以在Apache HBase上面执行SQL语句，提升了使用便利度，除此之外还添加了二级索引等特性，并在一些场景下面增加了一些性能优化的特性。

Phoenix提供了JDBC接口，也就是说我们可以调用通过这么一个接口来执行SQL，而HBase拥有自己独立的一套数据增删改查API，所以Phoenix内部主要的一个工作就是把SQL转换成HBase的Client API。这里我们以一个最简单的select语句来从头到位看看一个SQL如何一步步变成HBase原生API的。


<br/>

## Demo SQL与数据

现用Phoenix客户端创建表并插入数据
```sql
create table test.t1(c1 integer primary key, c2 integer, c3 varchar);
 upsert into test.t1 values(1,1,'a');
 upsert into test.t1 values(2,1,'b');
 upsert into test.t1 values(3,1,'c');
```


然后用如下示例Java代码来检索表，可以看到这些就是非常普通的基于JDBC接口代码，执行了一个select星加上where条件的语句。

```java
String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
Class.forName(driver);
String phoenixUrl = "jdbc:phoenix:127.0.0.1:2181:/hbase";
long start = System.currentTimeMillis();
Connection conn = DriverManager.getConnection(phoenixUrl);
try{
    Statement stmt = conn.createStatement();
    ResultSet result = stmt.executeQuery("select * from test.t1 where c1 = 1");
    while (result.next()){
        int c1 = result.getInt(1);
        int c2 = result.getInt(2);
        String c3 = result.getString(3);
        logger.info("row: "+ c1 + ", " + c2 +", " + c3);
    }
}finally {
    conn.close();
}
```











<br/>



## JDBC连接创建

这里先来看下JDBC连接的创建，也就是下面这段代码 


```java
String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
Class.forName(driver);
String phoenixUrl = "jdbc:phoenix:127.0.0.1:2181:/hbase";
long start = System.currentTimeMillis();
Connection conn = DriverManager.getConnection(phoenixUrl);
```





Class.forName会尝试用classloader把PhoenixDriver这个class装载到内存里面，在这个过程中PhoenixDriver里面的static这段代码会被执行，可以看到这里初始化了PhoenixDriver然后调用DriverManager的registerDriver方法注册PhoenixDriver这个实例。

```java
INSTANCE = new PhoenixDriver();
try {
    Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
            .....
        }
    });
    ...

    DriverManager.registerDriver(INSTANCE);
}            
```




DriverManager#getConnection会调用对应Driver实例的connect方法，也就是PhoenixDriver#connect方法。


```java
for(DriverInfo aDriver : registeredDrivers) {
    if(isDriverAllowed(aDriver.driver, callerCL)) {
        try {
            println("    trying " + aDriver.driver.getClass().getName());
            Connection con = aDriver.driver.connect(url, info);
            if (con != null) {
                // Success!
                println("getConnection returning " + aDriver.driver.getClass().getName());
                return (con);
            }
        } catch (SQLException ex) {
            if (reason == null) {
                reason = ex;
            }
        }
    } else {
        println("    skipping: " + aDriver.getClass().getName());
    }
}
```


DriverManager#getConnection会调用对应Driver实例的connect方法，也就是PhoenixDriver#connect方法, 继而会有一系列的方法嵌套调用，但这里面核心的一个类是ConnectionQueryServiceImpl, 它代表一个Phoenix客户端到HBase集群的连接，这里面的关键性工作都是在它的init方法里面，主要有如下几点：
1. 调用ConnectionQueryServices#openConnection()，里面主要是调用HBaseFactoryProvider.getHConnectionFactory().createConnection来创建导集群的连接,即返回一个HConnection，并赋给ConnectionQueryServices.connection, 这时候Client到Hbase集群的连接就建立完成了。
2. HConnection创建之后获取一个HBaseAdmin, 用它来检查表SYSTEM.CATALOG是否存在，这是一个phoenix维护的metadata表，里面存了所有table schema信息。
3. 调用checkClientServerCompatibility检查客户端服务端的兼容性
4. 调用ensureSystemTablesMigratedToSystemNamespace来移植系统表到SYSTEM命名空间



在ConnectionQueryServiceImpl完成初始化之后，最后用PhoenixConnection包裹起来然后返回给客户端代码。








<br/>

## 执行SQL查询整体流程

连接创建完成并且创建statement之后就开始执行SQL了，跟大部分SQL执行引擎类似，Phoenix把执行SQL的过程拆分若干个阶段整体流程可以从下面这张图来表示:


![]({{ "/images/phoenix-sql-flow.png" | absolute_url }})

也就是可以分为以下4个阶段，下面将逐个细过
1. SQL解析
2. 编译阶段
3. 优化阶段
4. 执行阶段 



<br/>
<br/>

## SQL解析流程



SQL解析流程的目的都是一样的，就是用一种编码语言的特定数据结构来表示SQL文本，phoenix这里使用了ANTLR框架，这是一个使用比较广泛的语言解析框架，比如ApacheSpark和Prestol都使用到了这个框架来解析SQL语句。Phoenix使用了ANTLR3，语法和解析规则定义在文件PhoenixSQL.g里面，可以看的出跟ANTLR4这种把语法定义和操作完成分离的手法相比，ANTLR3里面这种把语法和解析规则混在一起在可读性与扩展性方面还是要弱了很多。

整个解析过程中主要涉及到的两个类是PhoenixSQLParser和ExecutableNodeFactory。



<br/>

**PhoenixSQLParser**



PhoenixSQLParser由PhoenixSQL.g文件产生,同时产生的还有PhoenixSQLLexer，两者都是作用在文本解析阶段。文本SQL解析的入口是PhoenixStatement#parseStatement，这个方法里面会调用SQLParser#parseStatement，继而就会调用PhoenixSQLParser#statement，这个statement方法在PhoenixSQL.g的定义如下：
```
// Parses a single SQL statement (expects an EOF after the select statement).
statement returns [BindableStatement ret]
    :   s=oneStatement {
        		try {
    			$ret = s;
    		} finally {
    			udfParseNodes.clear();
    		}
    	} EOF
    ;
```

可以看到它等于oneStatement并返回了一个BindableStatement。oneStatement也是定义在这里的一个规则，整个ANTLTR就是这么一个规则互相应用的过程。


<br/>

**ExecutableNodeFactory**

ExecutableNodeFactory的作用是控制SQL解析过后返回的数据结构，它扩展了ParseNodeFactory。在PhoenixSQL.g文件里面可以直接使用了ParseNodeFactory来做各种规则解析，比如对于single_select规则:

```
single_select returns [SelectStatement ret]
@init{ contextStack.push(new ParseContext()); }
    :   SELECT (h=hintClause)? 
        (d=DISTINCT | ALL)? sel=select_list
        (FROM from=parseFrom)?
        (WHERE where=expression)?
        (GROUP BY group=group_by)?
        (HAVING having=expression)?
        { ParseContext context = contextStack.peek(); $ret = factory.select(from, h, d!=null, sel, where, group, having, null, null,null, getBindCount(), context.isAggregate(), context.hasSequences(), null, new HashMap<String,UDFParseNode>(udfParseNodes)); }
    ;
```

这里factory就是在文件头部分定义的ParseNodeFactory, single_select返回的是ParseNodeFactory#select方法，也就是ExecutableNodeFactory的select方法:

```java
@Override
public ExecutableSelectStatement select(TableNode from, HintNode hint, boolean isDistinct, List<AliasedNode> select, ParseNode where,
        List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit, OffsetNode offset, int bindCount, boolean isAggregate,
        boolean hasSequence, List<SelectStatement> selects, Map<String, UDFParseNode> udfParseNodes) {
    return new ExecutableSelectStatement(from, hint, isDistinct, select, where, groupBy == null ? Collections.<ParseNode>emptyList() : groupBy,
            having, orderBy == null ? Collections.<OrderByNode>emptyList() : orderBy, limit, offset, bindCount, isAggregate, hasSequence, selects == null ? Collections.<SelectStatement>emptyList() : selects, udfParseNodes);
}
```



在PhoenixSQLParser和ExecutableNodeFactory相互作用下，Demo代码里面的SQL语句解析的结果如下：


![]({{ "/images/phoenix-sql-parse.png" | absolute_url }})





<br/>

## SQL编译流程

编译流程的主要作用是把CompilableStatement这个解析后的中间结构转换为可执行的执行计划QueryPlan。针对SQL的每个部分（或者叫子句), 都会有一个Compiler来处理，比如groupby对应的是GroupByCompiler, where对应的是WhereCompiler, from对应的是FromCompiler，这里我们将针对Demo语句详细看下from子句、where子句和select子句。

<br/>

**FromCompiler**

编译的入口是CompilableStatement#compilePlan，因为demo语句对应的CompilableStatement是ExecutableSelectStatement, 所以具体实现是在
ExecutableSelectStatement#compilePlan里面。

这里面会首先调用FromCompiler#getResolverForQuery去遍历from里面的table节点并构建一个ColumbResolver供后续SQL其他部分resolve column用。也就是说这里会验证from里面的表，如果表不存在，这时候就会抛出错误。

产生的ColumnResolver会在QueryCompiler里面用来构建StatementContext，后续列的resolve就是调用StatementContext的resolver来完成。

<br/>

**WhereCompiler**


QueryCompiler里面对于SubQuery、Join和Union分别会有方法来处理，但到最后会都调用QueryCompiler#compileSingleFlatQuery来处理单一平铺SQL，where子句和select就是在这里面被编译的。

Where子句被解析过后编程ParseNode，这是一个树形结构，所以在WhereCompiler#compile里面用WhereExpressionCompiler使用访问模式来visit树里面的节点并做转换, 转换前后的对比如下：


![]({{ "/images/phoenix-sql-compile.png" | absolute_url }})




where表达式转换完成之后会调用WhereOptimizer#pushKeyExpressionsToScan把where条件转换成HBase原生Scan里面的startrow和endrow，这是通过设置StatementContext里面的ScanRanges达成的。因为如果用原生HBase API去过滤扫描数据，你也是要通过设置Scan API里面的Filter来完成数据的过滤，但PHoenix在这个过程当中还会作一些优化，比如说SkipScan, 它会针对复杂的filter条件计算出一部分不需要扫描的rowkey范围然后过滤掉，从而提供性能。


<br/>

**ProjectionCompiler**


对于select子句，会调用ProjectionCompiler#compile方法来处理，主要是把所有相关列找出来resolve以下，就是验证下列是否存在。然后把对应的ColumnFamily也给蒸出来添加到ScanContext里面，所以可以看到这里也会处理WhereExpression，因为有些列可能在where条件里面而不再select里面。


等SQL语句里面的每个部分都完成时，最后会生成一个QueryPlan，这里的Demo语句对应的是ScanPlan。




<br/>

## 执行计划优化阶段

执行计划的入口是QueryOptimizer#optimize，这里首先会检查Scan是否是点查询，如果是的话直接就返回了，因为没有优化的空间了。这里的Demo语句就是这样一个例子，pointLookup这个值就是在where编译过程中在WhereOptimizer#pushKeyExpressionsToScan里面设置的。


```java
if (!useIndexes 
        || (dataPlan.getContext().getScanRanges().isPointLookup() && stopAtBestPlan)) {
    return Collections.singletonList(dataPlan);
}
```



如果不是点查询，接着主要是针对hint和index的处理，可以看到这里调用IndexStatementRewriter#translate方法把SQL语句里面的所有列都转换成index对应的列，具体优化流程就不赘述了。






<br/>

## 执行QueryPlan

QueryPlan的执行是通过各类iterator来完成的，入口是QueryPlan#iterator， 返回的是ResultIteraotr, 接着用PhoenixResultSet把这个ResutlIterator包裹起来供客户端使用，这里以DemoSQL为例，涉及到的各类Itrator如下:



![]({{ "/images/phoenix-sql-iterator.png" | absolute_url }})


PhoenixResultSet的next触发的时候，会调用RoundRobinResultIterator的next方法。RoundRobinResultIterator如果发现还没初始化，会通过ParallelIterators#submitWork方法创建若干个LookAheadResultIterator并提交执行。接着LookAheadResultIterator#peek方法被执行，调用了TableResultIterator的next方法，里面会继续调用ScanningResultIterator的，最终调用了ClientScanner的next方法，ClientScanner是HBase原生API，这个next方法返回的值被存在LookAheadResultIterator的netx tuple里面，至此，初始化就完成了。


当RoundRobinResultIterator初始化完底下的iteraotr之后，它的next方法会轮训从各个LookAheadResultIterator的next方法获取它里面预先取得的值。




关于ParallelIterators里面LookAheadResultIterator个数，也就是并行度，这个值是在BaseResultIterators#getParallelScans(byte[], byte[])里面决定的，决定因素有region个数、region的startkey、ftiler条件等等，具体算法就不赘述了，完成之后会返回一个List<List<Scan>>类型, 接着在ParallelIterators#submitWork被打平，然后为每个Scan提交一个LookAheadResultIterator。




<br/>

## 小结

Phoenix从SQL语句一直到最后转换成HBase操作的整套流程，相对来说，并没有Spark SQL那套来的复杂，主要还是因为Spark是计算引擎，为了保持计算框架的可扩展性，所以中间会有更多的抽象层，用来大篇幅的抽象与代码来优化查询。而Phoenix只是简单的查询和更新，优化的空间没那么大，可预见的变化相对来说会有小点，所以相对来说会简单许多。



-------
注：hbase代码基于cloudera hbase, 版本cdh5-1.2.0_5.13.0


