---
layout:     post
title:      SparkSQL解析流程详解
date:       2020-04-12
summary:    SparkSQL解析流程代码级别的深入剖析
categories: spark
published:  true
---



Spark在执行SQL语句的时候其实是先把SQL先转换成RDD，最后执行的是RDD。从SQL转换成RDD这个流程是比较复杂的，Spark先把SQL文本转换成逻辑计划，然后会对逻辑计划进行验证与优化，最后再转换成物理执行计划。本文就先讲讲SQL是如何转换成逻辑计划的。

Spark把SQL文本解析转换成逻辑计划这个过程主要借助了ANTLR这个框架，可以进一步可以拆分为两个步骤:
1. SQL文本解析成ParseTree, ParseTree是ANTLR4里面一种树状数据结构
2. 把ParseTree转换成LogicalPlan

<br/>

## ANTLR 简介
ANTLR全称ANother Tool for Language Recognition， 相当于一种语言识别框架。ANTLR框架提供的好处是用户不需要再写代码解析文本，而是直接操作生成好的一颗ParseTree。ANTRL一般的用法是这样的：
1. 先用ANLTR提供的meta language设计一种语言的语法
2. 用ANTLR Tool基于设计好的语法文件产生一系列java类，利用这些java类再加上ANTLR runtime API可以直接把语言文本解析成一颗ParseTree
3. 扩展产生的Java类以编写应用代码


在Spark SQL解析过程中，ANTLR也是这么用的，首先来看看meta language是如何设计SQL语言的。

<br/>

## SQL语法定义SqlBase.g4

Spark的Sql语文件是SqlBase.g4, 位于sql-catalyst项目的src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4

从该文件头注释中可以看到这个文件是基于Presto的SQL语法文件改编而来，也就是说Presto也采用了ANTLR框架来解析SQL语句。


meta-language本身语法并不复杂，结合下面从SqlBase.g4摘取的部分有关SQL语言的定义，语法基本如下
- 小写开头的是规则(rule), 大写开头的是Token；Token可以用类正则的语法来定义
- 规则可以由任意规则和Token组成，比如singleStatement由statement 加上 EOF组成。
- 一个规则可以有多种选择，比如statement可以是query，也可以是USE DATABASE 或者 CREATE DATABASE等等。后面的#statementDefault等注释会为后面的代码生成产生影响。
- 一个规则还可以递归定义



```
singleStatement
    : statement EOF
    ;
statement
    : query                                                            #statementDefault
    | USE db=identifier                                                #use
    | CREATE DATABASE (IF NOT EXISTS)? identifier
        (COMMENT comment=STRING)? locationSpec?
        (WITH DBPROPERTIES tablePropertyList)?                         #createDatabase
    | ALTER DATABASE identifier SET DBPROPERTIES tablePropertyList     #setDatabaseProperties
...
USE: 'USE';
...
```



<br/>

## 使用ANTLR工具产生代码


有了语法文件之后，我们就可以用antlr提供的工具来生成相应的代码。ANTLR为多种编程语言提供了工具，包括Java、Python、C++、JS等等，这里我们使用Java对应的工具，可以用以下命令下载antlr工具并设置alias

```
wget https://www.antlr.org/download/antlr-4.7-complete.jar
cp antlr-4.7-complete.jar  /usr/local/lib/
export CLASSPATH=".:/usr/local/lib/antlr-4.7-complete.jar:$CLASSPATH"
alias antlr4='java -jar /usr/local/lib/antlr-4.7-complete.jar'
```

执行产生代码的命令, 默认情况下visitor相关代码是不会产生的，需要加上-visitor参数:

```
antlr4 -visitor SqlBase.g4
```

产生的文件代码列表如下，SqlBaseLexer和SqlBaseParser是用来把输入的SQL文本解析成ParseTree，SqlBaseListener和SqlBaseBaseListener是基于listener的方式来遍历操作ParseTree，SqlbaseVisitor和SqlBaseBaseVisitor是基于visitor模式的方式来遍历ParseTree。

```
SqlBase.tokens
SqlBaseLexer.tokens
SqlBaseLexer.java
SqlBaseParser.java
SqlBaseListener.java
SqlBaseBaseListener.java
SqlBaseBaseVisitor.java
SqlBaseVisitor.java
```

在工程里面，一般会采用对应的maven插件在build的过程中产生代码:

```
<plugin>
  <groupId>org.antlr</groupId>
  <artifactId>antlr4-maven-plugin</artifactId>
  <version>4.7</version>
  <executions>
    <execution>
      <goals>
        <goal>antlr4</goal>
      </goals>
    </execution>
  </executions>
  <configuration>
    <visitor>true</visitor>
    <sourceDirectory>../catalyst/src/main/antlr4</sourceDirectory>
  </configuration>
</plugin>
```



接下来看看spark如何利用这些生成的代码来解析SQL文本到ParseTree。

<br/>

## 解析SQL文本生成ANTLR ParseTree

通常来说我们会在代码里面使用SparkSession的sql方法来执行SQL语句, 可以看到它调用了SessionState里面的sqlParser赌赢的parsePlan来解析SQL文本

```scala
def sql(sqlText: String): DataFrame = {
  Dataset.ofRows(self, sessionState.sqlParser.parsePlan(sqlText))
}
```

所以先来看下这个sqlParser，也就是对应的SparkSqlParser对应是如何被创建的。



<br/>

**创建SparkSqlParser**

SparkSqlParser是SessionState的一个域，所以先来看看SparkSession里面的sessionState是如何初始化的。


我们可以在一个Spark应用里面拥有多个Session，每个Session里面会共享一部分内容，比如SparkConf里面的配置信息，但每个Session也可以有独特的，和其他session不同的设置内容，这个内容就是存在于SessionState里面。

SessionState是在SparkSession里面根据spark.sql.catalogImplementation参数动态创建的，当前的2.x版本里面有两个选项，分别是in-memory和hive, 对应的初始化类名分别是SessionStateBuilder和HiveSessionStateBuilder。默认是in-memory，当我们在创建SparkSession的时候加了enableHiveSupport选项之后就变成hive。从代码中可以看到HiveSessionStateBuilder扩展了BaseSessionStateBuilder。

```scala
private val HIVE_SESSION_STATE_BUILDER_CLASS_NAME =
  "org.apache.spark.sql.hive.HiveSessionStateBuilder"

private def sessionStateClassName(conf: SparkConf): String = {
  conf.get(CATALOG_IMPLEMENTATION) match {
    case "hive" => HIVE_SESSION_STATE_BUILDER_CLASS_NAME
    case "in-memory" => classOf[SessionStateBuilder].getCanonicalName
  }
}
...
private def instantiateSessionState(
    className: String,
    sparkSession: SparkSession): SessionState = {
  try {
    val clazz = Utils.classForName(className)
    val ctor = clazz.getConstructors.head
    ctor.newInstance(sparkSession, None).asInstanceOf[BaseSessionStateBuilder].build()
  } catch {
    case NonFatal(e) =>
      throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
  }
}
```



sqlParser在BaseSessionStateBuilder被定义为lazy的一个field，一旦被使用的时候会调用SparkSessionExtensions#buildParser方法来组建sqlParser：

```scala
protected lazy val sqlParser: ParserInterface = {
  extensions.buildParser(session, new SparkSqlParser(conf))
}
```

刚开始接触Scala的码农看着buildParser这里面的代码可能会觉得有点绕，这里的ParserBuilder是自定义类型，它是一个值函数，对应的类型是 (SparkSession, ParserInterface) => ParserInterface。
其实buildParser的主要一个目的是让用户可以注入自定义的Parser, 最后生成的Parser是以SparkSqlParser为基础往上堆叠的parser。如果用户没有注入自定义的Parser，那么返回给BaseSessionStateBuilder.sqlParser的就是SparkSqlParser。


```scala
private[this] val parserBuilders = mutable.Buffer.empty[ParserBuilder]

private[sql] def buildParser(
    session: SparkSession,
    initial: ParserInterface): ParserInterface = {
  parserBuilders.foldLeft(initial) { (parser, builder) =>
    builder(session, parser)
  }
}
def injectParser(builder: ParserBuilder): Unit = {
  parserBuilders += builder
}
```









<br/>

**生成ANTLR ParseTree**


从上面我们了解导SessionState里面的sqlParser对应的是SparkSqlParser，它扩展了AbstractSqlParser，SparkSession里面调用的parsePlan其实是AbstractSqlParser#parsePlan。从它的实现里面可以看到它就是调用了parse方法


```scala
override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
  astBuilder.visitSingleStatement(parser.singleStatement()) match {
    case plan: LogicalPlan => plan
    case _ =>
      val position = Origin(None, None)
      throw new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
  }
}
```

parse方法在SparkSqlParser里面有实现，它是个多值函数，可以看到这里面只是对command，也就是SQL文本做了变量替换，然后就调用AbstractSqlParser里面的parse方法

```scala
protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
  super.parse(substitutor.substitute(command))(toResult)
}
```

而SQL解析的主要流程就是在AbstractSqlParser#parse里面, 具体实现代码如下：



```scala
protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
  logDebug(s"Parsing command: $command")

  val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
  lexer.removeErrorListeners()
  lexer.addErrorListener(ParseErrorListener)

  val tokenStream = new CommonTokenStream(lexer)
  val parser = new SqlBaseParser(tokenStream)
  parser.addParseListener(PostProcessor)
  parser.removeErrorListeners()
  parser.addErrorListener(ParseErrorListener)

  try {
    try {
      // first, try parsing with potentially faster SLL mode
      parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
      toResult(parser)
    }
    ...
  }
  ...
}
```


1. 这里首先用SQL文本构建之前生成的SqlBaseLexer，然后重新设了下ErrorListener，使用了自定义的ParseErrorListener, 这个是用来捕捉解析过程中的错误信息。
2. 然后用lexer创建一个CommonTokenStream, 并用tokenStream构建SqlBaseParser(这也是之前产生的代码)，再重置parser里面的errorlistener和parselistenern。到此为止，其实还没开始解析过程，只是做了些初始化的任务。
3. 最后调用toResult,也就是parsePlan里面传入的第二个大括号里面的内容，这里面首先调用来parser.singleStatement()，也就是调用了singleStatement这个规则，生成了一颗ParseTree。生成ParseTree的流程就是这么简单，调用一个ANTLR生成好的Parser里面的一个规则即可。


到此为止ParseTree就解析生成完成，那么有没有办法可以直观地查看这颗树呢？当然可以，使用ANTLR提供的工具可以非常方便地查看针对某一个语句生成的ParseTree。

假设我们的输入语句如下, 这里要注意全部字符都需要大写，因为从上面parse代码里面可以看到创建lexer之前首先会创建UpperCaseCharStream，也就是对输入进行字符大写转换:

```sql
SELECT * FROM T WHERE C = 1
```

生成ParseTree的命令行如下：

```shell
# 产生代码
antlr4 SqlBase.g4
# 编译所有的java文件
javac *.java
# 设置alias
alias grun='java org.antlr.v4.gui.TestRig'
# 针对SqlBase这个grammar里面的singleStatement rule进行测试并展示
grun SqlBase singleStatement -gui
SELECT * FROM T WHERE C = 1
^D
```

最后生成的ParseTree如下:


![]({{ "/images/spark-sql-parse-tree-full.png" | absolute_url }})


如果觉得命令行工具比较麻烦的话，可以用IDE比如Intellij的antlr插件来完成，插件名称是ANTLR v4 grammar plugin, 安装完成之后只需要在grammar文件SqlBase.g4里面选择对应的rule点右键选择Test Rule xx就可以了。



<br/>


## 遍历ANTLR ParseTree生成LogicalPlan 



<br/>

**AstBuilder**

在parsePlan里面上面只讲到调用parser.singleStatement()生成一个ParseTree，那么接下来就会调用astBuilder.visitSingleStatement来遍历这棵ParseTree然后最后生成LogicalPlan。



这里面关键的一个类就是SparkSQLAstBuilder, 这个类扩展了AstBuilder，而它最终实现了SqlBaseBaseVisitor，也就是用ANTLR工具产生的代码。ANTLR为了解耦文本解析和应用逻辑，所以加入了中间的ParseTree数据结构，并且提供了两种方法来遍历这棵树，分别是采用Listener和Visitor，这两种形式分别对应于不同的生成代码。那么Spark里面采用了Visitor的模式，因为它更好地控制遍历细节。

在AstBuilder里面可以看到除了对特别需要处理的规则(RuleContext)节点的visit方法实现之外，还对visitChildren这个方法做了override，默认的visitChidlren会遍历访问所有的子节点，而在这里是没有一个通用的方法来组成返回多个子节点的结果，对于多个子节点，比如Join、Union等结果，都需要定制他们的visit方法。
```scala
override def visitChildren(node: RuleNode): AnyRef = {
  if (node.getChildCount == 1) {
    node.getChild(0).accept(this)
  } else {
    null
  }
}
```



对于当前SQL语句而言，它最核心的实现方法是在AstBuilder#visitQuerySpecification, 针对于上面的简单demo语句 SELECT * FROM T WHERE C = 1，从最开始的visitSingleStatement到visitQuerySpecification, 可以直接通过ParseTree可以看出它的访问路径


![]({{ "/images/spark-sql-parse-tree-top.png" | absolute_url }})



在visitQuerySepcification里面，我们可以看到它把一个SQL语句拆分为2部分处理，分别是from子句和其他部分。这里还是以上面的SELECT * FROM T WHERE C = 1 为例子讲下整个流程.

```scala
override def visitQuerySpecification(
    ctx: QuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
  val from = OneRowRelation().optional(ctx.fromClause) {
    visitFromClause(ctx.fromClause)
  }
  withQuerySpecification(ctx, from)
}
```





<br/>

**from子句**

首先from子句有可能不存在，比如说你可以直接在spark.sql里面执行"select 'a' as c1", 那么从代码里面可以看到这里会产生一个OneRowRelation。

如果from子句存在，那么就调用visitFromClause继续遍历fromClause这棵子树。这下面可能会包含子查询，join等复杂关系，这里就略过了。我们这个例子里面是个表，所以根据语法的设定会产生如下一个访问轨迹：


![]({{ "/images/spark-sql-parse-tree-from.png" | absolute_url }})



并在最后返回一个UnresolvedRelation。


<br/>

**withQuerySpecification** 

一个SQL语句的剩余部分都在withQuerySepcification里面处理，这些剩余的子句相对来说是比较固定的。querySpecification在SqlBase.g4里面的定义如下:

```
querySpecification
    : (((SELECT kind=TRANSFORM '(' namedExpressionSeq ')'
        | kind=MAP namedExpressionSeq
        | kind=REDUCE namedExpressionSeq))
       inRowFormat=rowFormat?
       (RECORDWRITER recordWriter=STRING)?
       USING script=STRING
       (AS (identifierSeq | colTypeList | ('(' (identifierSeq | colTypeList) ')')))?
       outRowFormat=rowFormat?
       (RECORDREADER recordReader=STRING)?
       fromClause?
       (WHERE where=booleanExpression)?)
    | ((kind=SELECT (hints+=hint)* setQuantifier? namedExpressionSeq fromClause?
       | fromClause (kind=SELECT setQuantifier? namedExpressionSeq)?)
       lateralView*
       (WHERE where=booleanExpression)?
       aggregation?
       (HAVING having=booleanExpression)?
       windows?)
    ;
```

我们可以看到它有两种选择:

1. SELECT TRANSFROM\MAP\REDUCE开头的这种查询，说实话在实际工作总还真没用过这种SQL语句，这里就略过了。
2. 一般的select from 查询，但是这里也有两种选择：
    - 正常的select xxx from yyy, 我们一般都用这种。
      ```
      kind=SELECT (hints+=hint)* setQuantifier? namedExpressionSeq fromClause?
      ```
    - from yyy select xxx, 我是看了这个语法文件才知道还可以这么写, 可能这个语法的有一个方便之处在于可以直接执行 "from t"返回一个表的所有列，不过一般也不会这么写，主要是怕影响代码的可读性。另外这种语法看起来是不支持hint的，其他方面跟前面正常的语法没什么不同，解析逻辑是一样的，最后解析出的逻辑计划也是一样的。
      ```
      fromClause (kind=SELECT setQuantifier? namedExpressionSeq)?
      ```


<br/>

很显然我们的样例语句属于第二类的第一种情况，第二类情况的代码实现如下:
```
case SqlBaseParser.SELECT =>
  // Regular select

  // Add lateral views.
  val withLateralView = ctx.lateralView.asScala.foldLeft(relation)(withGenerate)

  // Add where.
  val withFilter = withLateralView.optionalMap(where)(filter)

  // Add aggregation or a project.
  val namedExpressions = expressions.map {
    case e: NamedExpression => e
    case e: Expression => UnresolvedAlias(e)
  }
  val withProject = if (aggregation != null) {
    withAggregation(aggregation, namedExpressions, withFilter)
  } else if (namedExpressions.nonEmpty) {
    Project(namedExpressions, withFilter)
  } else {
    withFilte
  }

  // Having
  val withHaving = withProject.optional(having) {
    // Note that we add a cast to non-predicate expressions. If the expression itself is
    // already boolean, the optimizer will get rid of the unnecessary cast.
    val predicate = expression(having) match {
      case p: Predicate => p
      case e => Cast(e, BooleanType)
    }
    Filter(predicate, withProject)
  }

  // Distinct
  val withDistinct = if (setQuantifier() != null && setQuantifier().DISTINCT() != null) {
    Distinct(withHaving)
  } else {
    withHaving
  }

  // Window
  val withWindow = withDistinct.optionalMap(windows)(withWindows)

  // Hint
  hints.asScala.foldRight(withWindow)(withHints)

```

从代码实现里面可以看到，SQL语句的每个部分都由一个LogicalPlan组成，然后这些plan严格地按一定的顺序组成父子关系，继而堆叠成一个树，下面是按照处理顺序的详细流程：
1. withLateralView: 首先基于from子句返回的relation处理lateralView， 这种语法一般和explode等方法联合使用来对包含数据的列进行拆解，使一行变多行，但其实现在spark可以直接使用explode方法来达到这个目的，不需要再使用这个这种语法。那么我们这个样例也没有这种语法，所以返回的就是from里面的UnresolvedRelation
    ```scala
    val withLateralView = ctx.lateralView.asScala.foldLeft(relation)(withGenerate)
    ```
2. withFilter: 如果有where条件，则会调用filer方法添加一个Filter的logical plan，并把Relation作为它的子节点。我们样例中有WHERE C = 1，所以这里返回的是Filter的逻辑计划
    ```scala
    val withFilter = withLateralView.optionalMap(where)(filter)
    ```
3. withProject: 这里面会检查聚合，如果有聚合则会返回一个Aggregaton逻辑计划；如果没有聚合并且select里面的表达式不为空，构建一个Filter为子节点的Project逻辑计划；否则直接返回Filter。那么例子中是有select *的，所以这里返回Project逻辑计划
    ```scala
    val withProject = if (aggregation != null) {
      withAggregation(aggregation, namedExpressions, withFilter)
    } else if (namedExpressions.nonEmpty) {
      Project(namedExpressions, withFilter)
    } else {
      withFilter
    }
    ```

4. withHaving: 这是在有聚合的基础之上如果有having子句，那么构建再在Aggregation上面构建一个Filter逻辑计划。样例中没有having语句，所以直接返回Project
    ```scala
    val withHaving = withProject.optional(having) {
      val predicate = expression(having) match {
        case p: Predicate => p
        case e => Cast(e, BooleanType)
      }
      Filter(predicate, withProject)
    }
    ```

5. withDistinct: 如果select后面跟上distinct子句，则会在这里加上一个Distinct的逻辑计划。我们这里没有，直接返回Project。
    ```scala
    val withDistinct = if (setQuantifier() != null && setQuantifier().DISTINCT() != null) {
      Distinct(withHaving)
    } else {
      withHaving
    }
    ```
6. withWindow: 在having语句后面还可以跟window相关的函数，比如cluster by 之类的，最后返回一个WithWindowDefinition的逻辑计划。我们样例中没有，直接返回Project逻辑计划。
    ```scala
    val withWindow = withDistinct.optionalMap(windows)(withWindows)
    ```
7. withHints: 最后才处理hints，我们这里没有hints，所以直接返回Proejct
    ```scala
    hints.asScala.foldRight(withWindow)(withHints)
    ```


从上面可以看出来一个SQL语句的解析顺序是from、where、select(aggregation)、having、distinct、windows函数和hints。我们这个样例最后解析出的执行计划如下:
```
'Project [*]
+- 'Filter ('C = 1)
   +- 'UnresolvedRelation `T`
```


至此，逻辑计划就创建出来了。



<br/>

## 小结

可以看到SQL文本到解析生成逻辑计划整个流程就是典型的对ANTLR这个框架的应用



<br/>
<br/>

-------
注：spark代码基于cloudera spark，版本release 2.3.0.cloudera3

