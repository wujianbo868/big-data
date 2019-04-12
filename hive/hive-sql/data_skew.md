####数据倾斜问题整理

[数据倾斜问题整理](https://www.jianshu.com/p/153e01c1d764)

####两表均是大表,但join key存在严重倾斜的情况

假如发生倾斜的join的两边有一个是小表，那么可以把join改成map jion来处理，比较不好处理的情况是join的两倍都是大表，join key存在严重倾斜的情况。
针对这种情况，可以考虑使用

``` 
set hive.optimize.skewjoin=true;
set hive.skewjoin.key=100000;
``` 

另一种解决途径是进行数据拆分,key值大的单独处理再union处理