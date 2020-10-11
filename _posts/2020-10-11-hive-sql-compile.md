---
layout:     post
title:      Apache Hive SQL 编译流程详解
date:       2020-10-11
summary:    用一个demo sql来展示apache hive sql编译全流程
categories: hive
published:  true
---




当Hive JDBC把SQL语句提交到HiveServer2之后，Hive SQL语句从解析到执行会经历如下阶段：

![]({{ "/images/hive-sql-compile-overview.png" | absolute_url }})

- 文本解析：把SQL文本分析成抽象语法树(AST)
- 元数据验证：从AST里面按规则提取各类对象存放于QueryBlock，然后连接到HiveMetastore验证数据库、表、列等数据库对象。
- 生成操作树：在对AST进行优化之后生成操作树
- 生成任务树：遍历操作数根据执行引擎生成对应的任务树，每个任务都会对应于一些操作
- 执行任务树：提交执行任务


这里会用一段DEMO SQL为例来看下每个阶段的变化和中间产物，SQL语句如下，这语句的目的是算出各个部门31岁以上员工的平均年龄，最后按平均年龄排序：

```sql
SELECT d.dept_name,
       avg(e.age) AS avg_age
FROM
  (SELECT *
   FROM test.emp
   WHERE age > 31) e,
     test.dept d
WHERE e.dept_id = d.dept_id
GROUP BY d.dept_name
ORDER BY avg_age DESC;
```


涉及到的表结构如下：


```sql
create database test;
create table test.emp(
    name string,
    age int,
    dept_id int
);
create table test.dept(
    dept_id int,
    dept_name string
);
```


<br/>

## 文本解析

<br/>

### 解析流程

SQL执行的第一步是解析SQL文本，跟很多SQL-on-Hadoop项目一样，apache hive解析SQL采用了ANTLR框架，用的版本是ANTLR3，跟Spark采用的ANTLR4比起来还是得逊色一点，把解析规则和处理逻辑混合在一起。

解析过程在Driver#compile被调用，调用的入口是ParseUtils.parse方法，实际解析流程在方法ParseDriver#parse里面，核心代码片段如下：



```java
public ASTNode parse(String command, Context ctx, String viewFullyQualifiedName)
  ...
  HiveLexerX lexer = new HiveLexerX(new ANTLRNoCaseStringStream(command));
  TokenRewriteStream tokens = new TokenRewriteStream(lexer);
  ...
  HiveParser parser = new HiveParser(tokens);
  ...
  parser.setTreeAdaptor(adaptor);
  HiveParser.statement_return r = null;
  try {
    r = parser.statement();
  } catch (RecognitionException e) {
    ...
  }
  ASTNode tree = (ASTNode) r.getTree();
  ...
  return tree;
}
```

从上面可以看到解析流程有如下几步：
1. 词法解析：把文本流按预定义好的关键字拆成一连串token，由类HiveLexerX负责，这个类扩展了HiveLexer
2. 语法解析：把词法解析输出流按预定义好的解析规则解析成一棵解析树，规则由类HiveParser负责
3. 获取AST：从解析树调用getTree方法获取Abstract Syntax Tree，默认情况下返回的是CommonTree，这里事先通过setTreeAdaptor方法把结果改成了ASTNode


<br/>

### 关于HiveLexer和HiveParser

这两个类由预定义好文件用maven插件antlr3-maven-plugin产生代码，文件用antlr3的语法编写而成


- HiveLexer: 对应文件ql/src/java/org/apache/hadoop/hive/ql/parse/HiveLexer.g
- HiveParse: 主要对应文件HiveParser.g，另外在这里面导入了剩余的3个文件
  - ql/src/java/org/apache/hadoop/hive/ql/parse/HiveParser.g
  - ql/src/java/org/apache/hadoop/hive/ql/parse/IdentifiersParser.g
  - ql/src/java/org/apache/hadoop/hive/ql/parse/FromClauseParser.g
  - ql/src/java/org/apache/hadoop/hive/ql/parse/SelectClauseParser.g


HiveLexer.g里面定义了一些列的关键字，比如SQL语句里面的where关键字在HiveLexer里面用KW_WHERE来表示，然后在FromClauseParser.g的whereClause规则定义里面，匹配到KW_WHERE之后会生成一个节点，这个节点的名称就是TOK_WHERE

```
...
whereClause
@init { gParent.pushMsg("where clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_WHERE searchCondition -> ^(TOK_WHERE searchCondition)
    ;
...
```


### DEMO AST

上面提到的DEMO SQL
```sql
SELECT d.dept_name,
       avg(e.age) AS avg_age
FROM
  (SELECT *
   FROM test.emp
   WHERE age > 31) e,
     test.dept d
WHERE e.dept_id = d.dept_id
GROUP BY d.dept_name
ORDER BY avg_age DESC;
```

生成的AST如下图所示：



![]({{ "/images/hive-sql-compile-ast.png" | absolute_url }})

整体还是比较清晰的，这棵树就是SQL的一个结构化表达方式：
- 根节点tok_query代表整个查询，tok_query由tok_from和tok_insert构成
- tok_insert从字面上看起来有点奇怪，因为语句里面并没有insert字眼，这是在解析的时候由固定的规则产生的，每个查询里面除了from子句的其他子句都会放在tok_insert下面，这里可以看到tok_select、tok_where、tok_groupby和tok_orderby, 这些节点对应的都是语句里面相应的子句。tok_destination跟tok_insert相关，表示会把最后的结果塞入一个临时文件
- tok_from下面有个join节点，join由一个子查询和一个表组成，子查询的结构跟主查询的结构类似。


<br/>

## 元数据验证

有了AST之后就可以按一定的规则对这个树进行遍历，获取所需要的数据库对象，然后连接到Hive Metastore进行验证, 整个流程的入口位于方法SemanticAnalyzer#genResolvedParseTree

<br/>

### 前置特殊情况处理

在正式遍历收集对象之前，会对一些特殊情况进行处理

- 处理Orderby和GroupBy里面的位置别名，在SemanticAnalyzer#processPositionAlias里面处理。关于位置别名, order by后面的默认是启用的，由hive.orderby.position.alias控制，groupby里面默认不启用，可以通过set hive.groupby.position.alias = true启动，这里的处理方式是遍历整个语法树(深度优先)把groupby 和orderby 里面的别名替换成对应的列。
- 处理Create Table语句，在方法SemanticAnalyzer#analyzeCreateTable处理。对于单纯的CREATE TABLE或者CREATE TABLE LIKE语句，遍历AST获取建表信息之后构建DDLWork，然后就返回了，分析阶段完毕。返回之前会往queryState和rootTasks这两个变量变更，分别设置CommandType和任务。如果是CREATE TABLE  AS SELECT 语句，则先遍历AST获取建表信息存放到QueryBlock变量里边，然后继续后面的流程。

- 处理Create View语句，流程在SemanticAnalyzer#analyzeCreateView方法：对create view进行解析处理, 添加DDLWork到rootTasks, 从AST里面获取select语句并返回。



- 预处理insert语句，在SemanticAnalyzer#preProcessForInsert方法。对于insert into的语句，检查into目标表对应的HDFS path是否加密，如果有则调用QB#addEncryptedTargetTablePath添加。


<br/>

### 遍历AST收集QueryBlock信息

在处理完前面这些特殊情况之后会调用SemanticAnalyzer#doPhase1递归遍历整个抽象语法树，针对不同的Token类型收集信息不同的信息，用skipRecursion来控制是否进一步递归。解析结果存储在QB和QBParseInfo这两个对象里面。

QueryBlock在分析流程中扮演一个非常重要的角色，遍历AST过程中收集的重要信息都存放在QueryBlock(QB)里面。一个QueryBlock只针对一个查询块，也就是一个select .. where .., 子查询的信息存放的QB和主查询不是同一个QB，子查询的QB从父查询QB的aliasToSubq获取。QB收集的信息包括：
 - 别名和表名的映射关系，存放于aliasToTabs
 - 别名和子查询的对应关系，aliasToSubq
 - 解析信息，主要各类对应dest目标对应的子树，dest引用自Phase1Ctx，比如dest对应group by语法子树，这些信息都存放于对象QBParseInfo
 - 元数据信息，包含各类别名、名称对应的实体表的信息，存放于对象QBMetaData


<br/>

### 验证元数据

验证元数据在SemanticAnalyzer#getMetaData方法里面完全，主要是从上一步产生的QueryBlock里面提取信息并加以验证，主要涉及到如下信息

1. 表别名，用QB#getTabAliases获取，检查表别名对应的表是否在Hive Metastore里面存在。如果是view，检查view并用重新sql用view定义来替换view名称
2. 子查询别名：用QB#getSubqAliases获取，递归调用getMetadata方法处理子查询
3. 输出目标：用QB里面的QBParseInfo#getClauseNamesForDest方法获取，接着根据目标是数据表还是文件夹分别作处理


验证元数据的时候会用Hive(db)变量去获取Hive数据库对象，这个Hive(db)是在构建BaseSemanticAnalyzer(QueryState)调用BaseSemanticAnalyzer#createHiveDB方法构建，这里面调用了Hive#get方法为每个线程生成一个到metastore服务的连接，这是通过把这个变量放到ThreadLocal达到的。Hive(db)里面包含了一个到Hive Metastore Server的RPC client。

<br/>

## 操作树生成


<br/>

### 优化AST


生成操作树的入口是CalcitePlanner#genOPTree，在正式用QueryBlock生成操作树之前会调用CalcitePlanner#getOptimizedAST优化原来的AST，这里采用apache calcite框架来优化语法树，属于逻辑优化，比如最常见的过滤条件下推和映射下推，predicate push down 和project push down。


以下是优化过后的AST:

![]({{ "/images/hive-sql-compile-ast-optimized.png" | absolute_url }})

从上面跟原来AST对比，可以看到有以下变化：

1. 原先主查询里面的where节点被剪除了
2. Join节点左边的subquery里面，select节点从select星号变成两个具体的列的select expr，where节点里面增加一个not null 条件
3. Join节点右边的表变成子查询，子查询的select节点添加了两个具体的列，where节点有一个not null条件。
4. Join节点增加了等号条件的子节点

从以上的变动可以看出，其他主要还是做了条件下推和映射下推的优化。

<br/>

### 基于QueryBlock生成操作树

在优化了AST之后，会重新调用doPhase1方法来重新产生QueryBlock信息，然后根据QueryBlock里面的信息来生成操作数，入口是方法SemanticAnalyzer#genPlan(QB, boolean)。

操作树可以是hive里面的逻辑计划，但是跟其他SQL引擎的不同点在于，这个逻辑计划是其实是参与最后Task执行的。


调用genPlan生成操作树具体流程如下

1. 处理子查询：对子查询进行调用genPlan递归生成操作，并放到aliasToOpInfo。这里的子查询的概念比一般通常所在sql里面的子查询广，包括通常放在括号里面的一段select from语句以及view定义重写后成子查询. 子查询信息是调用QB#getSubqAliases()方法获取。

2. 调用genTablePlan对表进行处理，生成TableScanOperator。先构建TableScanDesc，然后再用TableScanDesc和schema信息构建TableScanOperator，并把这个TableScanOperation添加到topOps里面，每个表扫描操作都会添加到topOps里面。如果有对应的TableSample，还会在此TableScanOperation的基础上添加一个FilterOperation。

3. 处理Join生成JoinOperator：调用SemanticAnalyzer#genJoinTree生成JoinTree，再用JoinTree生成JoinOperator，流程还是相当复杂，这就不赘述了。

4. 调用genBodyPlan处理剩余的其他子句生成对应的Operator: 
    - 根据where子句生成FilterOperation, 调用SemanticAnalyzer#genFilterPlan
    - 如果有聚合函数的话，则调用org.apache.hadoop.hive.ql.parse.SemanticAnalyzer#genSelectAllDesc生成一个SelectOperator
    - 根据不同的配置分别调用如下的方法生成GroupByOperator
        1. genGroupByPlanMapAggrNoSkew
        2. genGroupByPlanMapAggr2MR
        3. genGroupByPlan2MR
        4. genGroupByPlan1MR
    - 调用SemanticAnalyzer#genPostGroupByBodyPlan对如下子句生成Operater
        - 生成having plan，对应方法SemanticAnalyzer#genHavingPlan
        - 生成windowing plan，对应方法SemanticAnalyzer#genWindowingPlan
        - 生成select子句对应的计划， 对应方法SemanticAnalyzer#genSelectPlan
        - 如果有有clusterby、orderby、distributeBy、sortby子句，则调用SemanticAnalyzer#genReduceSinkPlan生成ReduceSinkOperator

<br/>

### DEMO语句对应的操作树生成


优化过后的AST所生成的操作树如下：


![]({{ "/images/hive-sql-compile-operator-tree.png" | absolute_url }})

按照上述流程，首先会对左边的子查询处理，生成TS[0]-FIL[1]-SEL[2], 然后对右边的子查询处理生成TS[3]-FIL[4]-SEL[5], 然后处理Join生成RS[6]，RS[7]和JOIN[8]，然后调用genBodyPlan生成后续所有的操作。


<br/>

### 操作树逻辑优化

在生成操作树之后会在genPlan里面继续调用Optimizer#optimize优化操作树，这里大量的优化规则在Optimzer初始化的时候被放置到变量Optimizer#transformations里面，优化的过程是逐个遍历这些规则，然后调用它的transform方法对ParseContext进行各种优化，ParseContext里面包含了前面所有解析转换流程所产生的context数据。

被优化过后的操作树如下：



![]({{ "/images/hive-sql-compile-operator-tree-optimized.png" | absolute_url }})

可以看到SEL9和SEL13被剪除，FIL1和FIL3被重新处理，具体就不赘述了。

<br/>

## 生成物理执行任务

Hive当前支持三种执行引擎，分别是mapreduce、tez和spark。在构建物理执行任务的时候，针对不同的执行引擎会生成不同的执行任务，所以在编译任务的过程中会有三种不同的compiler
- TezCompiler
- SparkCompiler
- MapReduceCompiler

整个编译过程就是调用这些compiler的compile方法完成，这里以map reduce为例。


<br/>

### 操作树进一步优化

在生成task之前，还会调用optimizeOperatorPlan对操作树进行进一步的优化，这些优化是专门针对这三个执行引擎特定的一些优化，所以放到这里执行。基本流程是构建规则，然后构建一个图遍历的对象，遍历整个操作树并应用这些规则。


<br/>

### 生成MapReduce任务树


生成MapReduce任务树的入口在MapReduceCompiler#generateTaskTree，基本流程是先构建RegExpRule和对应的NodeProcessor，然后用这些规则和processor构建一个GraphWalker，调用GraphWalker的startWalking方法递归遍历操作树。遍历过程中用RegExpRule对节点进行匹配，匹配到最优processor之后进行处理。输入的操作树是调用ParseContext#getTopOps获取，生成的任务树方在传入的rootTasks变量。



<br/>


#### RuleRegExp匹配模式



RuleRegExp构建的时候需要规则名称和一个正则，这里的正则表达式匹配的是节点，比如TS.*RS表示的是匹配TableScan和ReduceSink，他们之间可以有任意个

匹配有三种方式
1. 没有通配符的匹配模式：直接用传入的字符串去匹配节点名称，节点名称后面会加一个百分号(%)，匹配成功后把传入的字符串长度作为代价值返回。
2. 有通配符的匹配模式：会用正则去匹配当前已经遍历的节点。
3. 选项1或者选项2：传入的字符串里面带有\|的情况，不赘述了。


<br/>


#### 节点处理

生成MapReduce任务树的核心逻辑都在这些NodeProcessor里面，举几个例子：

- GenMROperator: 没匹配到的使用默认的NodeProcessor，GenMROperator, 处理如下，获取context里面的mapCurrCtx，然后从这里面获取前面一个operator的GenMapRedCtx, 然后用前面一个operator的currTask和currAliasId构建一个新的GenMapRedCtx并存放到mapCurrCtx。也就是保存了下状态，没做什么实际的事情。
- GenMRTableScan1:匹配到TableSource, 构建一个dummmy的MapRedWork以及Task

- GenMRRedSink1: 调用GenMapRedUtils#initPlan初始化Plan，里面会构建ReduceWork，MapWork在MapRedWork构建的时候已经被构建。

- GenMRRedSink2：主要调用GenMapRedUtils#splitPlan来切割plan，在这个方法里面创建一个child plan(MapredWork)，然后再创建对应的ChildTask，接着调用GenMapRedUtils#splitTasks，在切割后的两个task之间创建临时文件, 在这里面调用GenMapRedUtils#setTaskPlan设置对childtask对应map进行设置。这里会创建一个中间临时的Operator。

- GenMRFileSink1：把当前Task对应的Plan设置为final（setFinalMapRed）；处理linked file sink desc；处理merge file；设置fetch task


<br/>

### DEMO语句生成的任务树

首先是遍历顺序，对于DEMO语句生成的操作树，先从TableScan[0]开始直接一直遍历到最后的FileSink[16]，然后再从TableScan[3]遍历到ReduceSink[7],到RS7之后会匹配到GenMRRedSink1，传入的nodeOutputs已经全部为true，所以返回false。nodeOutputs是从DefaultGraphWalker.retMap里面获取，这个变量保存节点访问历史。

遍历过程中RegExpRule用规则匹配到的节点如下，没匹配到的会用默认GenMROperator：

- TableScan[0]匹配到GenMRTableScan1
- ReducerSink[6]匹配到GenMRRedSink1
- ReducerSink[11]匹配到GenMRRedSink2
- ReducerSink[14]匹配到GenMRRedSink2
- FileSink[16]匹配到GenMRFileSink1
- TableScan[3]匹配到GenMRTableScan1
- ReducerSink[7]匹配到GenMRRedSink1


可以看到GenMRRedSink2处理了两次，所以总共会有3个有依赖关系的任务

```
Stage-1:MAPRED
- Stage-2:MAPRED
- - Stage-3:MAPRED
```

各个任务的map和reduce所对应的操作如下图


![]({{ "/images/hive-sql-compile-task-tree.png" | absolute_url }})

可以看出原先的操作树被切分到不同的mapreduce任务里面，任务之间的依赖用用临时文件的方式来展现，所以任务之间会多出来一个新的FileSink和TableScan。



<br/>


### 任务树优化

生成操作树之后会继续优化它，入口为MapReduceCompiler#optimizeTaskPlan。

这里面首先会调用MapReduceCompiler#breakTaskTree打散操作数，把所有reducersink的child操作节点设置为null。比如原来mapwork里面的map operator是: TS[22]-RS[14]-SEL[15]-FS[16]，处理完成之后变成 TS[22]-RS[14]



接着就会调用PhysicalOptimizer#optimize进行优化，这里的优化跟流程跟上面的操作树优化非常类似，只不过叫做resolver,上面叫做transformer，默认的resolver有
- CommonJoinResolver
- MapJoinResolver
- NullScanOptimizer
- CrossProductCheck


遍历过程中，TaskGraphWalker访问顺序是从下往上，就是先访问叶子节点，等叶子节点全部完成之后再访问根节点。

<br/>



#### DEMO语句的任务树优化

DEMO语句生成的stage1到3的任务树在优化的时候涉及到了2个resolver，分别是CommonJoinResolver和MapJoinResolver。

在CommonJoinResolver里面，根据表的大小把join转换成map join，最后调用CommonJoinTaskDispatcher#mergeMapJoinTaskIntoItsChildMapRedTask
把当前的task合并到子任务里面的localtask。所以这个阶段结束之后,task变成:
```
Stage-2:MAPRED
- Stage-3:MAPRED
```

接着会进入MapJoinResolver，如果当前task的map work含有MapredLocalWork, 则创建一个MapredLocalTask，并把当前task设置local task的子任务，对于当前例子，就是把Stage-2:MAPRED里面的local拿出来创建一个task，最后生成的任务树

```
Stage-6:MAPREDLOCAL
- Stage-2:MAPRED
- - Stage-3:MAPRED
```

拿这个去跟explain出来的结果对比，会发现完全吻合的上。



<br/>



## 执行阶段

提交MapReduce任务执行，mapper和reducer具体执行的是对应Operator里面的操作，就不赘述了。



<br/>

## 小结

Hive SQL编译和执行流程是hive当中最核心的处理部分，理解这部分流程和代码相当于掌握了Hive的精髓。



<br/>

-----------
注：hive源码基于cloudera的cdh6.3.2版本，对应apache的2.1.1
