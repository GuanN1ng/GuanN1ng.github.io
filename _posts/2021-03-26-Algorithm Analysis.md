---
title:  Algorithm Analysis
date:   2021-03-26 16:52:58
categories: 
- Data Structures
---

算法分析，最重要的是确定一个算法将需要多少时间或空间等资源量的问题，而问题实例的规模n是决定计算成本的主要因素。

### 计算模型 RAM 
Random Access Machine,等价于通用图灵机
* 寄存器按顺序编号，无总数限制；
* 每一基本操作仅需单位常数时间；
则算法的运行时间 ∝(正相关) 算法需要执行的基本操作次数。
于是T(n) = 该算法在RAM中为求解规模为n问题所需的基本操作次数。

### 大O标记法

∃ c > 0，当n ≫ 2后，T(n) <= cf(n),T(n)的增长率小于或等于f(n)的增长率,记为： 
`T(n) = O(f(n)) ` 
T(n)表示算法的执行时间，n标识数据规模的大小，f(n)为算法需执行基本操作次数的总和。
此时 f(n)为T(n)的上界(upper bound);

大O时间复杂度并不具体地标识代码真正的运行时间，而是表示`代码执行时间随数据规模增长的变化趋势`,分析时，可以忽略常量、低阶、系数，只需要记录一个`最大阶的量级`即可。


### 其他记号
* `∃ c > 0，当n ≫ 2后，T(n) >= c × f(n),记为T(n) = Ω(f(n))`,T(n)的增长率大于等于f(n)的增长率，f(n)为T(n)的下界(lower bound);
* `∃ c1>c2>0，当n ≫ 2后，c1×f(n) > T(n) > c2×f(n),记为T(n) = Θ(f(n))`,两个函数基本以相同的速率增长，此时结果尽可能的好

### 复杂度量级
* 常数阶 O(1)  例 2021^2021
* 对数阶 O(logn)
    * 二分查找
    * 欧几里得算法，两个正整数的最大公约数是同时整除二者的最大整数
```
 while( n != 0){
     long rem = m % n;
     m = n;
     n = rem;
 }
 return m;
```

* 线性阶 O(n)
* 平方阶、立方阶、K次方阶
* 指数阶 O(2^n)  `无效算法`



### 复杂度分析
* 最好情况时间复杂度
* 最坏情况时间复杂度


