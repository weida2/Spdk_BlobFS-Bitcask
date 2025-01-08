# SPDK_BlobFuse bitcask

## 实验目的

基于BlobFS，设计一个key-value数据库，支持open，put，get，close操作

## 实验内容

基于BlobFS，设计一个key-value数据库，支持open，put，get，close操作

## 实验过程和步骤

### 实验思路

本次基于BlobFS设计KV数据库。考虑到BlobFS仅支持追加写的特性，本次实验选择使用C++复现Bitcask数据库。

- BlobFS的限制![image-20221222194509364](./README_set/image-20221222194509364.png)

### Bitcask数据库原理

#### 日志型数据存储

所有写操作只追加而不修改老的数据，这样做目的是能保证最大程度的顺序 IO ，压榨出机械硬盘的顺序写性能。

在Bitcask模型中，数据文件以日志型只增不减的写入文件，而文件有一定的大小限制，当文件大小增加到相应的限制时，就会产生一个新的文件，老的文件将只读不写。

在任意时间点，只有一个文件是可写的，在Bitcask模型中称其为active data file，而其他的已经达到限制大小的文件，称为older data file，如下图：

![image-20221222195029020](./README_set/image-20221222195029020.png)

文件中的数据结构非常简单，是一条一条的数据写入操作，每一条数据的结构如下：

![image-20221222194639115](./README_set/image-20221222194639115.png)

#### 基于hash表的索引数据

日志类型的数据文件会让我们的写入操作非常快，而如果在这样的日志型数据上进行key值查找，那将是一件非常低效的事情。于是我们需要使用一些方法来提高查找效率。

例如在Bigtable中，使用bloom-filter算法为每一个数据文件维护一个bloom-filter 的数据块，以此来判定一个值是否在某一个数据文件中。
在Bitcask模型中，除了存储在磁盘上的数据文件，还有另外一块数据，那就是存储在内存中的hash表，hash表的作用是通过key值快速的定位到value的位置。hash表的结构大致如下图所示：

![image-20221222195243972](./README_set/image-20221222195243972.png)

hash表对应的这个结构中包括了三个用于定位数据value的信息，分别是文件id号(file_id)，value值在文件中的位置（value_pos）,value值的大小（value_sz），于是我们通过读取file_id对应文件的value_pos开始的value_sz个字节，就得到了我们需要的value值。整个过程如下图所示：

![image-20221222195226455](./README_set/image-20221222195226455.png)

由于多了一个hash表的存在，我们的写操作就需要多更新一块内容，即这个hash表的对应关系。于是一个写操作就需要进行一次顺序的磁盘写入和一次内存操作。