---
layout:     post
title:      RDBMS中的路径访问优化
date:       2020-07-26
summary:    一篇有关关系型数据库中访问路径优化论文的阅读理解笔记
categories: sql
published:  true
---




无意间看到一篇上个世纪70年代的论文，叫Access Path Selection in a Relational Database Management System，主要介绍了SQL在访问路径优化方面的内容，这些优化是基于当时的一个关系型数据库的研究项目System R，这个System R是现代关系型数据库的鼻祖，里面的很多技术都可以在MySQL、Oracle这些数据库中找到踪影。本文主要是对这篇paper里面所涉及到技术的个人理解方面的展示。


在这个SystemR系统里面，SQL语句的处理流程跟现代各类数据库系统类似也分为SQL文本解析、优化、执行代码生成和执行这四个阶段，而这篇论文主要关于第二步优化阶段的访问路径选择问题，可以分为三部分说明：

1. SystemR存储系统介绍
2. 单表访问路径选择
3. 多表关联访问路径选择






<br/>

## SystemR存储系统介绍

因为关于路径访问优化离不开底层的存储系统，所以这里先介绍下System R系统里面的存储系统。

System R下面有个子系统叫RSS(Research Storage System), 它主要负责关系数据的物理存储，这里的关系可以简单的理解成表(table), 关系是以以下规则来存储的：

1. 关系是以tuple的集合来存储的，这里的tuple可以理解成行。tuple由列垂直组成，列的存储在物理上是连续的，也就是通常说的行式存储。
2. tuples存在一个个page里面，page的大小是4k，一个tuple不能跨page。
3. 一个segment由多个page组成，一个segment可以包含多个关系，但是一个关系不能跨segment。
4. 多个关系的tuple可以存在一个page里面，tuple会有一个标记来确认它是属于哪个关系。


访问一个关系里面的tuple的主要方式是扫描(scan)，在给定一个访问路径的前提下，一次扫描返回一个tuple。扫描有OPEN、NEXT和CLOSE三个操作。

扫描有两种类型：

1. segment scan：也就是扫描一个segment的所有page，把其中给定的一个关系相关的所有tuple找出来，因为一个表不会跨segment。
2. index scan: 一个表可以有若干个index，index的存储独立于对应表存储的page。Index采用B-tree结构实现，叶子节点存储的是键值+键值对应tuple的定位符，并且叶子节点所存储的page是链接在一起的，所以index scan可以一直连续在叶子节点扫描而不用跳到上层节点。


在性能方面，segment scan会对一个segement里面的所有page进行扫描，无论是否包含给定表的tuple，但是所有的page只扫描一次；但如果通过一个index扫描一张表的全部数据的时候，tuple所存储的一个page有可能被扫描多次，因为一个page里面包含的多个tuple在index里面对应的键值很可能不是连续的。但是有一种特殊的index叫clustered index，它对应的表的tuple的插入物理顺序基本上和index的键值顺序一致，这种index不管是sgement scan还是index scan，page都只会被扫描一次。







<br/>


## 单表访问路径选择

对于只访问一张表的SQL语句，它的所有可用的访问路径，包括通过全表扫描(segment scan)以及在这个表上面的所有index，比如一张表建了两个index，那么总共可用的访问路径是3个。
优化器主要是根据where里面的条件和统计信息来计算各个访问路径的代价，然后选择代价最小的访问路径。


![]({{ "/images/sql-access-path-single-table.png" | absolute_url }})




下面分别说明统计信息、where条件和代价计算这三个主要部分

<br/>

### 统计信息

在优化阶段，优化器会查找对应表和索引的统计信息，在SystemR里面统计信息具体定义如下

存储的表的统计信息：
1. NCARD(T) - 表的行数量
2. TCARD(T) - segment里面包含该表的page数量
3. P(T) - TCARD(T) / segment非空page总数量

index的统计信息:
1. ICARD(I) - index键值的唯一值数量
2. NINDX(I) - index所包含的page数量


这些统计信息在表和index初始创建和装载的时候会更新，但后续不会在DML操作的时候同时更新，需要用户主动去触发。


<br/>

### Where子句和选择性因子

Where子句里面的条件可以被视为conjunctive normal form，就是由一连串的子条件用AND连接而成，每个子条件叫做布尔因子(boolean factor)。

每个布尔因子都可以计算出一个值，叫选择性因子，全称selectivity factor，用F表示，具体计算方式要根据不同的条件类型，列举几个如下：

- column = value: 如果column上面有index,  F = 1 / ICARD(I), 否则 F = 1 / 10
- column1 = column2: 如果两列都有index，F = 1 / Max(ICARD(column1 index), ICARD(column2 index))


从上面的公式可以看出selectivity factor代表的是大致上这个条件过滤后的数据占整体数据的比例。


假设where子句后面有n个布尔因子，那么整个where子句整体的选择性因子由所有布尔因子的选择性因子相乘得出:

```
F(predicates) = F1 * F2 * F3 * Fn
```


因为一次RSI Call返回一个tuple，那么预测的RSI Call总数等于表行数乘以F(predicates)，记为RSICARD:

```
RSICARD = NCARD(T) * F(predicates)
```


<br/>

### 单表代价计算


优化器根据如下公式来计算每个访问路径的预估代价

```
COST = PAGE FETCHES + W * (RSI CALLS)
```

Page Fetch用来衡量IO的代价，RSI Call用来衡量CPU的代价，W则是可调整的变量，用来权衡CPU和IO。



针对每一种可用的访问路径，可以用公式根据不同的情况来计算Cost：

- 布尔因子包含相当条件的唯一索引列: COST = 1 + 1 + W，这相当于是点查询,最多从一个page返回一个tuple
- 一个或多个布尔因子包含聚集索引：COST = F(predicates) * (NINDX(I) + TCARD) + W * RSICARD， 因为tuple在索引page和segment page的顺序是一致的，所以可以把索引page数量和segment page数量加一起乘以F
- 一个或多个布尔因子包含普通索引：COST = F(predicates) * (NINDX(I) + NCARD) + W * RSICARD，因为普通索引的index page和segment page无关，所以segment page的访问个数换成所有行数，但是如果数据库系统的buffer可以装下这个表所有的segment page，那么它的COSt跟上面的聚集索引是一样的。
- 没有任何布尔因子包含聚集索引：COST = (NINDX(I) + TCARD) + W * RSICARD
- 没有任何布尔因子包含普通索引：COST = (NINDX(I) + NCARD) + W * RSICARD
- segment scan：COST = TCARD/P + W * RSICARD，即扫描segment里面所有非空page。


所有可用访问路径的COST计算出来之后，选择COST最小的那个。 




<br/>


###  Interesting Order

使用索引访问模式或者对tuples进行排序都会产生排序后的结果，这些结果按索引键值或者排序键值排序，如果这个排序键值被定义在group by或者orderby，那么这个就叫做interesting order，因为如果选用这种访问模式会节省掉后续group by 或者 order by里面的排序工作, 所以一般会保留interesting order相关的路径访问，到后面再进行代价比较。

另外多表关联里面的sort-merge join也需要排好序的列，所以也会关注interesting order。



<br/>


## 多表关联访问代价选择

当多张表进行关联的时候，主要是从关联类型的选择和关联顺序的选择这两个维度拼凑出的关联组合里面选择出最优的访问路径, 当然这些是基于每个表的最优访问路径以及对应的interesting order，所以最终


<br/>


### 关联类型

首先看下outer relation和inner relation的定义。当两张表join的时候，会先从一张表里面取一行，然后再拿这一行数据到第二张表里面去作匹配，那么第一张表叫做outer relation，后面这张表叫做inner relation。另外从这样的关系中我们可以得出来join之后数据的排序是按outer relation的顺序来。


在System R系统里面，关联类型分两种：
1. nested loop： 假设join的两张表是A和B，nested loop会扫描A的所有行，针对A中的每一行扫描B中的所有行。假设A和B的行数分别是N和M，那么总操作数是N*M
2. merging scans: 也就是通常说的sort-merge join，先对A和B表按join条件进行排序，然后再进行merge操作，这里还会考虑到A和B是否可以按join条件的排序方式进行访问，比如在A和B上面是否创建了join条件相关的index，那么这时候就可以省去事先排序的工作了。在merging scan join中，会先从outer relation取一条数，然后跟inner relation的第一条进行对比。



多表Join可以被视为一系列的双表Join，比如A、B和C多表join，可被视为A和Bjoin之后的结果再和C作Join，当然Join的顺序也是一个考虑点，因为不能保证A、B、C的join顺序所产生的代价和A、C、B是一样的。在实际运行过程中，后面一个Join不必要等前面的完成之后再运行，只要前面的join产生部分结果之后就可以开始后面的Join，除非第二个Join之前需要排序，那么必须要等第一个完成之后。


<br/>

### 关联顺序


n个表关联顺序有n的阶乘种排列组合，这里首先会针对笛卡尔乘积进行处理，即如果有表没有跟其他任何表有连接条件，则尽量把它放后面处理。

对于两张表A和B的关联顺序，在给定关联类型的情况下，(A,B)和(B,A)是一样的，区别在于前者出来的结果是按A来排序，后者是按B排序。


<br/>

### 搜索树

优化器会构建一颗搜索树来寻找多表关联时的最佳访问计划，步骤如下
1. 对于join里面每个表在interesting ordering和没有interesting ordering的情况下分别找到最佳单表访问方式
2. 列举全部有关联关系的表，然后按不同的关联类型计算出他们的访问代价，对于相同表和相同的排序选出代价最小的。
3. 把第二步的结果作为一张表跟第三张表进行关系，也按不同的关联类型计算出他们的访问代价, 对于相同表和相同的排序选出代价最小的。循环知道最后一张表完成，最后选出代价最小的结果。




<br/>

### 关联代价计算



多表关联可以视为先关联两张表，接着把两张表的结果视为临时表然后跟第三张表关联，关联的结果再跟第四张表关联，循环类推直至最后一张表。也就是说每次关联都是n张表已经关联好的结果和第n+1张表关联，这时候把已经关联好的结果记为outer relation, 第n+1张表记为inner relation。

这里把N记为到现在为止outer relation的行数，N由如下公式所得:

```
N = (到此为止所有已经关联表行数的乘积) * (所有where条件的selectivity factor的乘积)
```

C-outer(path1)记为outer relation的cost，C-inner(path2)记为inner relation的cost

那么nested loop的关联代价为:
```
C-nested-loop-join(pathl,path2)= C-outer(path1) + N * C-inner(path2)
```

sort merge scan join的关联代价为:

```
C-merge(pathl,path2)=C-outer(path1) + N * C-inner(path2)
```

从公式看出来这两个cost是一样的，merge scan的优势在于已经排好序的inner relation可以减少page fetch次数，针对于outer relation的每个tuple，不需要扫描整个inner relation去获取一个匹配。这是论文里面给出的说明，但这还是有点令人费解的，因为基于现在高级语言的对于两个值的对比方法，merge过程根本不需要全部扫描，只能推断那时候可能没办法像现在这样对值进行大于小于的对比，而只能进行等于或者不等于的对比。




<br/>

## 多表关联搜索树例子

论文里面最后给出了一个详细的例子来说明整个多表关联的流程，首先看下样例表和数据：

![]({{ "/images/sql-access-search-tree-sample-data.png" | absolute_url }})

例子里面执行的SQL语句：

```sql
SELECT NAME,TITLE,SAL,DNAME FROM EMP,DEPT,JOB
WHERE TITLE=‘CLERK’
AND LOC=‘DENVER’
AND EMP.DNO=DEPT.DNO AND EMP.JOB=JOB.JOB
```




<br/>

### 三张表的单表访问路径选择

每张表的单表访问路径选择如下：
1. EMP表有3条访问路径，分别是DNO列的index、JOB列index和segment扫描。The interesting orders 分别是 DNO and JOB，DNO列的index提供了DNO的排序，，JOB列的index提供了JOB的排序，而segment scan提供的无序结果。这里假设JOB列index是代价最小的路径，DNO列因为是interesting order所以保留，而segment扫描直接切除掉。


2. DEPT表有两条访问路径，分别是DNO index和segment扫描。假设DNO index代价更新，所以segment scan去除。


3. JOB表有两条访问路径： JOB列index和segment 扫描。假设segment scan 代价更小，所以两者都保留。


最后这三张表里面被保留的单表访问计划要么是代价最小的，要么是interesting order：

![]({{ "/images/sql-access-path-search-tree-single-relation.png" | absolute_url }})



<br/>

### 搜索树

首先分别构建两张表的搜索树，针对两种不同的关联类型。




两张表关联的nested loop搜索树如下，列出了所以可以相关联的两张表关联关系，每条路径代表一种关联关系，最底下列出来用公式计算出的关联关系的代价以及排序，这里Ni表示中间部分结果的行数：


![]({{ "/images/sql-access-path-search-tree-nested-loop.png" | absolute_url }})




前两张表join的sort merge join搜索树如下，可以看到有些关联关系只需要merge，有些则需要排序:


![]({{ "/images/sql-access-path-search-tree-sort-merge.png" | absolute_url }})






最后从以上两棵树里面筛选出最低代价的表+interesting order的关联关系之后创建第三张表的关联搜索：


![]({{ "/images/sql-access-path-search-tree-3-tables.png" | absolute_url }})