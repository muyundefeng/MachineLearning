##Kmeans算法流程以及实现
Kmeans是一种基本的聚类算法，算法首先随机选择k个对象，每个对象代表一个簇的平均值或者中心．对于剩余每个对象，根据其到各簇中心的距离，把他们分给距离最小的簇中心，然后重新计算每个簇的平均值．重复这个过程，知道聚类准则函数收敛．一般来说准则函数采用两种方式：第一，计算全局误差函数．第二计算两次中心误差变化．
```
（1） 从 n个数据对象任意选择 k 个对象作为初始聚类中心；
（2） 根据每个聚类对象的均值（中心对象），计算每个对象与这些中心对象的距离；并根据最小距离重新对相应对象进行划分；
（3） 重新计算每个（有变化）聚类的均值（中心对象）；
（4） 计算准则函数，当满足一定条件，如函数收敛时，则算法终止；如果条件不满足则回到步骤（2）。
```
####算法的实现（hadoop方式实现）
源代码参考：http://blog.csdn.net/fansy1990/article/details/8028546
但是需要进行相关的修改，
1.修改hdfs相关的权限，（如果hadoop所处的用户与开发目录的用户是同一个用户，则不需要进行权限修改），如果用户不一致，执行下面hadoop fs命令
`hadoop fs -chmod -R 777 /user/hadoop/`,修改之后就可以在开发者用户之下进行程序的调试．
2.代码修改，①缓存中心向量文件，加入缓存文件的方式使用如下方法：
```
job.addCacheFile(centersFile.toUri());
```
同样在读取文件的时候，使用
```
FileSystem fileSystem = FileSystem.get(caches[0],context.getConfiguration(),"hadoop");
FSDataInputStream inputStream = fileSystem.open(new Path(caches[0]));
```
不要使用`bufferReader`普通java读取文件类获取文件内容
