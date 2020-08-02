---
layout:     post
title:      B-Tree索引
date:       2020-08-02
summary:    通过一篇论文来了解B-Tree index的结构以及在这上面元素的检索、插入和删除操作。
categories: sql,database
published:  true
---



论文： ORGANIZATION AND MAINTENANCE OF LARGE ORDERED INDICES

<br/>

## 1. 基本介绍



索引由索引元素的集合组成，索引元素由键值x和相关联的信息α组成，x可以唯一确认索引里面的元素，α通常是指向数据块的指针或者若干个记录的集合。


存储的设备有main store和backup store，都是随机访问设备，mainstore会快很，back store会慢点，可以把main store理解为在内存里面，back store在磁盘。index只有少部分在main store里面，大部分在back store。


index存放在固定大小的page里面，main store和back store之间的数据转移是以page为单位。
这些page是一棵树的节点，也就是B-tree的节点。

<br/>

**B-tree结构的优点**

1. 较少的空间使用率
2. 文件变大的时候会请求更多存储，文件缩小的时候会释放存储，不存在随着使用时间的增长而性能恶化的问题。
3. 键值按顺序存储，范围查询效率很高
4. 批量查询、插入和删除效率高



<br/>

## 2. B-trees 定义


B-trees必须符合以下条件(k是自然数)：
1. 从根节点到任意叶子节点拥有相同的长度h，这个h也叫做树的高度
2. 除了根和叶子节点之外的每个节点都拥有至少k+1个子节点。根节点要么是叶子节点，要么至少有两个子节点。
3. 每个节点至多拥有2k+1个子节点



根据上面的定义，当根节点拥有两个子节点，并且其他非叶子节点拥有k+1子节点的时候，这时候B-tree拥有最小的节点数，节点数公式如下:




![]({{ "/images/b-tree-node-min.png" | absolute_url }})


每个节点拥有2k+1个子节点的情况下，这时候B-tree拥有最大的节点数，公式公式如下:


![]({{ "/images/b-tree-node-max.png" | absolute_url }})

显然一棵B-tree index可以拥有的节点范围处于Nmin和Nmax之间。




<br/>


## 3. Page




Btree树的每个节点都是Page，一个Page的数据结构如下:



![]({{ "/images/b-tree-page-structure.png" | absolute_url }})

- p是指向子节点的指针
- x是键值
- α是键值相关的信息





 B-tree的数据结构有如下属性：


1. root page包含1到2k数量的key，除此之外的page，每个page包含k到2k数量的key，
2. 假设非叶子节点的page拥有l个key，那么它拥有l+1个子节点。
3. 在一个Page内部，keys按序列排序。
4. page里面包含的指针所对应的子节点，在顺序也要跟keys的位置相匹配。 
    - 是当一个元素y小于x1的时候，那么y肯定处在p0所指向的子树里面。
    - 当y大于xi并且小于x(i+1)的时候，那么y肯定处于pi所指向的子树里面。
    - 当y大于xl的时候，也就是最后一个元素所在的entry，那么y肯定处于pl所指向的子树。




<br/>

**高度h的范围**

因为root page包含1到2k数量的key，除此之外的page，每个page包含k到2k数量的key，然后再根据B-tree的最小和最大节点数，就可以算出总key数量的最大值和最小值

![]({{ "/images/b-tree-keys-max-min.png" | absolute_url }})


当key总数量一定的时候，可以反推出h的最大和最小值

![]({{ "/images/b-tree-h-max-min.png" | absolute_url }})

这个其实可以在面积一定的情况，一个等腰三角形的高度可以在一定的变化范围内进行选择。

<br/>

## 4. 检索流程

检索一个元素y的程序流程图如下:


![]({{ "/images/b-tree-retrieval.png" | absolute_url }})
可以看到这里消耗是扫描当前page，比较查找值与这个page里面所有key，可以用二分搜索法来实现。






<br/>


## 5. 插入流程

插入元素流程图：
![]({{ "/images/b-tree-insert.png" | absolute_url }})

1. 首先检索元素y,如果找到直接退出.
2. 如果未找到y并且b-tree是空的，则创建root节点，然后放入y
3. 如果未找到y并且b-tree非空，则把s指向最后一个扫描的叶子page。检查该page是否未已满，是的话则启动split page流程，否则在page里面插入y然后结束流程。

<br/>


### Page Split

当一个即将被插入元素的Page处于满状态的时候，即拥有2k个entry的时候，会触发Page Split流程。


PageSplit流程首先把当前插入的entry放到page里面，这时候page拥有2k+1个entry， 接着把
p0,(xl,pl),...,(pk,pk)放到page P里面，把 
pk+l,(xk+2,pk+2),(xk+3,pk+3).....(x2k+l,p2k+l)放到P'里面，这样还剩下xk+1这个处于中间的entry。最后把xk+1插到P的父节点Q里面，用p'指向P'。这样P和P'就称了兄弟。

另外Q里面插入一个元素也可能会导致page split流程，所以这个page split可能一直会触发到root节点。


<br/>


## 6. 删除流程



![]({{ "/images/b-tree-delete.png" | absolute_url }})

删除流程里面有以下三种情况：

1. 先查询键值y，如果没找到则结束

2. 如果找到y，则判断是否在叶子page上面，如果是的话删除y然后视情况执行catenation和underflow流程

3. 如果找到y，但是不在叶子page上面，假设找到的键值为yi, 这时候需要找到大于yi的最小值，方法是从Pi开始沿着P0指针一直到叶子page，这个叶子page的第一个值即是最小值。接着用这个最小值来替换yi，然后删除叶子page上面的第一个值。最后视情况执行catenation和underflow流程

<br/>


### Catenation流程


当两个由相邻指针p和p'指向的相邻兄弟节点P和P'的键值总数量少于或等于2k的时候，可以发生Catenation操作，这个操作于是split的反向操作，操作完成后总体节点数减少一个。

具体过程如下，先把Q里面的p'删除，把P、yj和P'合并。


![]({{ "/images/b-tree-page-catenation.png" | absolute_url }})




Catention操作会删除Q里面的entry，于是可能会触发Q相关的Catenation或者underflow操作，这个动作有可能会一直递进到root节点。


<br/>

### Underflow


Catention操作的触发条件是相邻兄弟节点P和P'的键值总数量少于或等于2k，当他们的键值总数量大于2k的时候就会触发Underflow操作，也就是使P和P'里面的键值重新均匀分配。

Underflow的操作是由Contantion和page split组合而且，先在P和P'上作Contention,然后再在合并好的Page的中间点作split操作。

因为Underflow的节点不会使父节点Q的键值数量发生改变，所以这个操作并不会触发额外的操作。

<br/>

## 7. 操作Cost

记f(min)和f(max)为从磁盘上读取page的最小和最大的数量，记w(min)和w(max)为写入磁盘的最小和最大page数量。

<br/>

### 检索Cost

从检索算法中可以看出
```
f(min) = 1
f(max) = h
w(min) = 0
w(max) = 0
```

<br/>

### 插入Cost

在没有发生page split的情况下Cost最小
```
f(min) = h
w(min) = 1
```


在最坏的情况下，page split一直从叶子节点执行到root，包括产生一个新的root节点，这时候Cost最大(这里的h指的是split发生前的树的高度：

```
f(max) = h
w(max) = 2h + 1
```


<br/>

### 删除Cost

当被删除的entry y处在叶子节点并且没有catenation和underflow的时候，代价最小:

```
f(min) = h
w(min) = 1
```
当y不处在叶子节点并且没有catenation和underflow的时候的代价为:

```
f(min) = h
w(min) = 2
```

在最坏的情况下，在检索路径上面除了root和root的子节点都发生catenation操作，root的子节点发生underflow操作，root自身因为子节点发生underflow操作所以也被修改了，所以代价为:

```
f(max) = 2h - 1  \\除了root节点之外，检索路径上的其他节点都需要读取两次

w(max) = h + 1   \\除了检索路径上的节点，还有一个是root子节点的兄弟节点，因为有underflow操作
```




