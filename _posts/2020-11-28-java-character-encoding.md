---
layout:     post
title:      Java字符编码
date:       2020-11-28
summary:    介绍Unicode、UTF-8、GB2312等字符编码相关概念以及在Java里面如何完成编码转换。
categories: java
published:  true
---




## 字符编码模型

在具体了解Java字符集编码之前先看下字符编码模型，这里直接采用了Unicode的字符编码模型，因为它完善和扩展了现有的或者过去的编码模型。

Unicode字符编码模型总共有4层

1. 抽象字符集：一定数量的字符按一定的形式组成的集合，比如ASCII里面的英文字母、数字和符号，
2. 编码字符集：一种映射，把抽象字符集里面的每个字符映射成一个非负整数，这个非负整数叫做码位(code point)，例如Unicode标准版本4 ISO/IEC 10646:2003， 比如中文常用的GBK
3. 字符编码格式：也是一种映射，将编码字符集的码位映射成码元(code unit)，码元是一定bit长度的整形。比如GBK使用的是EUC-CN这种字符编码格式，一个编码字符集可以对应多个字符编码格式，比如Unicode标准对应UTF-8、UTF-16等
4. 字符编码方法：将码元映射到8位字节序列，有些只是把码元直接映射到字节序列上，比如UTF-8，有些需要在文件头加上标记来指定端序(大小端)，比如UTF-16



## Unicode

Java等许多高级语言都使用了Unicode来代表其文本，所以这里先了解下Unicode。


### Unicode编码空间


Unicode标准属于编码模型里面的第二层，也就是映射抽象字符集到码位。Unicode的编码空间，也就是码位的范围是从U+0000到U+10FFFF，因为从U+D800到U+DFFF之间总2048个值为永久预留码位不提供编码，所以总共有1,112,064（16^4*17 - 1 - 2048 + 1）码位来映射字符。

Unicode的编码空间可以划分为17个平面（plane），每个平面包含2^16（65,536）个码位。17个平面的码位可表示为从U+xx0000到U+xxFFFF，其中xx表示十六进制值从0016到1016，共计17个平面。第一个平面称为基本多语言平面（Basic Multilingual Plane, BMP），或称第零平面（Plane 0），这个平面包含了常用字符，其他平面称为辅助平面（Supplementary Planes），这里包含一些生僻汉字、历史文字脚本、数学符号等。


Unicode版本一直在升级，版本升级会伴随着字符集的扩充，当前版本是13，可以用code chart页面(https://unicode.org/charts/)来查找当前版本字符和码位的映射关系，从这里可以看到

 - ACSII里面包含的字符映射在U+0000到U+007F
 - 一般常用的汉字映射落于U+4E00到U+9FFC之间
 - 一些生僻的汉字处于U+3400和U+4DBF之间，以及辅助平面理的U+20000到U+2EBE0，另外还有一些落于U+30000多




### UTF-8


UTF-8处于字符编码模型里面的第三层和第四层，全称Unicode Transformation Format 8-bit，是针对Unicode的一种可变长度字符编码。
UTF-8把Unicode的码位转换成一定长度的码元，码元的长度是1到4个字节。

UTF-8里面的8表示8个位，即一个字节，并不是说用一个字节来存储码位，而是这个编码过程是面向byte的，即逐个byte处理，这样的好处是编码过程跟计算机的端序无关。


UTF-8从1到4个字节的编码范围和规则如下：

- 1个字节：范围U+0000 到 U+007F，总共8*16=128个ASCII字符。这个字节的最高位固定为0，其余7个位用来存储码位，即0xxxxxxx
- 2个字节：范围U+0080 到 U+07FF，总共8*16*16 - 128 = 1920个字符，覆盖了剩余的拉丁语系字符。第一个字节110开头，第二字节10开头，即110xxxxx 10xxxxxx，剩余的11个bit刚好可以由码位来填充。
- 3个字节：范围U+0800 到 U+FFFF，总共16^4 - 16^2*8 = 63488个字符，覆盖了剩余所有基本多语言板块里面的字符，包含了大多数中文、日文和韩文字符。3个字节为 1110xxxx 10xxxxxx 10xxxxxx，剩余的16个bit由码位来填充。
- 4个字节：范围U+10000 到 U+10FFFF, 覆盖了非常见中文、日本和韩文字符以及历史语言脚本、数学符号和emoji。4个字节为11110xxx 10xxxxxx 10xxxxxx 10xxxxxx，最大值10FFFF由21个bit刚好可以依次填充剩余的bit。


可以看到如果大部分字符都由ACSII组成的时候，使用UTF-8来比编码效率很高，可以节省大量的空间,这也是为什么现在互联网大部分都使用了UTF-8作为其编码方式。


下面以汉字"优"为例来讲下编码过程,

1. 码位转为二进制: U+4F18 ->  0100 1111 0001 1000
2. U+4F18属于U+0800到U+FFFF，所以用3个字节来存储，1110xxxx 10xxxxxx 10xxxxxx
3. 用16bit的二进制码位依次填充得到 1110[0100] 10[111100] 10[011000]
4. 最后得到3个字节11100100 10111100 10011000，对应的16进制为E4BC98




### UTF-16

UTF-16跟UTF-8类似，也是一种可变长字符编码，区别在于UTF-16用16个bit来编码。UTF-16对于BMP(基本多语言平面)和辅助平面（Supplementary Planes）有不同的编码方式：


- 对于BMP(基本多语言平面)，即第0个平面里面的编码，一个16bit即可覆盖所有字符，因为码位和码元都是16bit，所以直接映射在BMP平面内，这也意味着UTF-16和ASCII和UTF-8都不兼容，因为即使最开始的U+0000到U+007F都用16bit来表示。

- 辅助平面里面的字符需要两个16bits来表示，即32bits来表示。从U+D800到U+DFFF之间的码位区段是永久保留不映射到Unicode字符，UTF-16就利用保留下来的0xD800-0xDFFF区块的码位来对辅助平面的字符的码位进行编码，编码步骤如下：
    1. 码位减去 0x10000，得到的值的范围为20比特长的 0...0xFFFFF。
    2. 高位的10比特的值（值的范围为 0...0x3FF）被加上 0xD800 得到第一个码元或称作高位代理（high surrogate），值的范围是 0xD800...0xDBFF。
    3. 低位的10比特的值（值的范围也是 0...0x3FF）被加上 0xDC00 得到第二个码元或称作低位代理（low surrogate），现在值的范围是 0xDC00...0xDFFF。


下面以一个化学符号"🜲"(码位U+1F732)为例来说明下UTF-16对于辅助平面是如何编码的
1. 0x1F732减去0x10000等于0x0F732，转换成20个bits为0000 1111 0111 0011 0010
2. 高位的10位值为0000 1111 01，16进制为0x003D，加上0xD800之后等于0xD83D，所以高位代理是0xD83D
3. 低位的10bit为11 0011 0010，16进制0x0332，加上0xDC00之后等于0xDF32，所以低位代理是0xDF32。




#### 端序的影响



计算机的架构在字节的序列上面会有所区别，也就是所谓的端序，有大端和小端之分，比如Mac上面是大端，而win上面是小端。UTF-16是以16bits为单元来处理的，所以构成这16个bits的两个bytes的顺序需要确定，也就是编码的时候要选择是大端还是小端，所以UTF-16对应以下三种模式
1. UTF-16 with BOM：这种模式会在字节序列的开头加上一个标记(byte order marker)用来表示使用的是大端还是小端，标记为16bit，0xFEFF表示大端，0xFFFE表示小端。比如上面的化学符号"🜲"用Java里面默认的UTF-16编码之后会得到 0xFEFFD83DDF32
2. UTF-16BE：使用大端，比如上面的化学符号"🜲"用Java里面UTF_16BE编码之后会得到 0xD83DDF32，跟上面的一样，只是缺省了BOM
3. UTF-16LE：使用小端，比如上面的化学符号"🜲"用Java里面的UTF_16LE编码之后会得到 0x3DD832DF，可以看到UTF-16是使用16bits作为单元进行处理，当有32bits的时候，前面16个bit的顺序和后面16个bits的顺序不会因为端序而发生变化，变化的是16bit内部前后两个8bit发生了交换。







#### UTF-16 vs UTF-8 

这两种编码形式如何选择取决于数据内容，如果大部分内容是英文或者数字的时候，选择UTF-8更节省存储，因为只需要一个字节来存储一个字符，而这时候UTF-16需要两个字节来存储一个字符，所以采取UTF-8可以至多节省一般存储空间。

当大部分内容是中文的时候，使用UTF-16是一个更好的选择，因为大部分中文UTF-16只需要两个字节来存储，而UTF-8则需要3个字节来存储，使用UTF-16可以至多节省三分之一的空间。


## Java字符编码


### Java和Unicode

Java使用Unicode来代表文本，也就是其文本相关的类，比如char、Character、String等，内部是使用Unicode来表示的，使用的Unicode的版本可以在Character这个类里面看到，比如java1.8就是基于Unicode Standard, version 6.2.0.

在实现层面，Java采用的是UTF-16(BE)编码来表示Unicode，比如可以使用如下char literal来表示汉字"优"

```java
char c = '\u4F18';
System.out.println(c);
```

因为char长度是16bits，但是UTF-16用32bits来表示辅助平面的字符，所以辅助平面的字符用两个char来表示，比如上面的化学符号"🜲"可以用如下char literal来表示

```java
char[] c = new char[]{'\uD83D', '\uDF32'};
System.out.println(c);
```


当然这使用起来跟零平面的字符还是有些区别的，所以Java还提供了基于CodePoint的字符API(https://www.oracle.com/technical-resources/articles/javase/supplementary.html)





### Java字符解码编码API

这里介绍下java nio下面的字符解码编码API，核心类是CharsetEncoder和CharsetDecoder，以下是一个从文件读取并使用UTF-8解码的DEMO

```java
String path = "file/path";
RandomAccessFile raf = new RandomAccessFile(path, "r");
FileChannel fileChannel = raf.getChannel();
try {
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4);
    Charset charset = Charset.forName("utf-8");
    CharsetDecoder charsetDecoder = charset.newDecoder();
    CharBuffer charBuffer = CharBuffer.allocate(10);

    boolean eof = false;

    while (!eof) {
        eof = fileChannel.read(byteBuffer) == -1;
        byteBuffer.flip();
        CoderResult coderResult = charsetDecoder.decode(byteBuffer, charBuffer, eof);
        if (coderResult.isError()) {
            coderResult.throwException();
        }
        byteBuffer.compact();
        charBuffer.flip();
        while (charBuffer.hasRemaining()) {
            System.out.print(charBuffer.get());
        }
        charBuffer.clear();
    }
} finally {
    fileChannel.close();
    raf.close();
}
```

过程很简单，从文件获取一个Channel，然后不断从Channel读取数据到ByteBuffer，并调用CharsetDecoder#decode方法解码并打印。

这里要注意的是字符经过UTF8编码之后可以是1个字节，2个字节，3个字节或者4个字节，在读取的过程当中，bytebuffer可能会从中间截断某个字符，从而导致decode返回一个Malformed input的CodeResult。
解决方法是decode方法的第三个参数传入的是false（文件未读完之前)，指示decode过程在遇到不可解码的时候放弃解码并把数据留在bytebuffer里面，接着调用byteBuffer的compact方法，compact把留下来未处理的数据移动到这个bytebuff的最前面，然后继续从channel读取后续数据。




## GB2312

虽然GB2312现在不怎么使用了，但是在很多老系统里面还是会遇到这个字符集编码，所以还是有必要了解下，特别是GB2312在Java内部的实现。


### GB2312编码空间

GB2312采用区位的形式来表示字符，共有94个区，每个区内有94个字符，这两个编号构建区位码，也就是码位。
完整的区位码查询可以在http://www.cn1.net/zhuanli/QuWeiMa.htm 网站，收录的字符大致情况如下：
- 01-09区：包含特殊字符、英文字符、数字、日文片假名等，要注意的是这里英文数字跟ASCII里面的不一样，平时遇到的全角数字/英文就是这些字符。
- 10-15区：空区
- 16-55区：包含常用汉字3755个，55区最后5个位置空缺
- 56-87区：包含非常用汉字3008个
- 88-94区：空区




### GB2312编码方式

GB2312通常采用EUC-CN来作为其编码方式，以便兼容ASCII，具体是每个字符以两个字节来表示，第一个字节称为高位字节，第二个字节称为低位字节。
- 高位字节等于区号(1-87区，16位表示为0x01 - 0x57)加上0xA0，范围是0xA1到0xF7。
- 低位字节等于位号(1-94位，16进制为0x01 - 0x5E)加上0xA0，范围是0xA1到0xFE


以汉字"优"为例，其位于51区，37位，十六进制表示为0x33和0x25，分别加上0xA0之后为0xD3和0xC5，所以优字编码之后的结果是0xD3C5


### GB2312编码Java实现

Java的扩展字符集定义在sun.nio.cs.ext.ExtendedCharsets里面，GB2312的实现类是sun.nio.cs.ext.EUC_CN，可以看到这个类里面定了字符串数组b2cStr，这个数组在索引161到247定了字符串，这个索引其实映射到区号对应的高位字节，161-247转换成十六进制刚好是0xA1到0xF7。
每个字符串包含了所有位所对应的字符，每个字符都是用utf-16表示的unicode，这里并没有使用低位字符，所以在实际映射中需要为位号加上0xA1。

比如"优"字的高位字节是0xD3，对应十进制是211，位号37，所以可以在b2cStr这个数组里面用如下下标查询：

```java
b2cStr[211].toCharArray()[37-1]
```

所以字符串数组bc2Str其实就是映射过后的GB2312区位表。




## 小结

对于码农来说，字符编码是个绕不开的主题，深入了解是早晚的事。