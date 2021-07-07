---
layout: post
title:  "Java Thread"
date:   2021-06-28 14:15:40
categories: JDK
---

线程(Thread)是**CPU调度和分派的基本单位**，包含在进程之中，是进程中的实际运作单位，**同一进程中的多条线程将共享该进程中的全部系统资源，
但每个线程有各自的调用栈(call stack)、寄存器环境(register context)、自己的线程本地存储(ThreadLocal)**。


#### 线程模型

线程的具体实现分为**用户线程**和**内核线程**，用户线程位于用户空间，由应用函数库实现，线程的建立、同步、销毁均在用户态完成，内核线程则由操作系统实现并支持和管理。
**线程模型是指实现线程的方式**，共有三种：

* 一对一模型，**使用内核线程实现**，一个用户线程对应一个内核线程，内核中的调度器(Thread Scheduler)负责每个线程的调度，**缺点：1.系统调用过程中，需要在
用户态和内核态中来回切换，2.每个内核线程都要消耗一定的内核资源，因此线程数是有限的，大量线程会对系统性能有影响。**

* 多对一模型，**使用用户线程实现**，线程的所有行为都在用户空间内完成，**系统内核对用户线程无感知**。**优势：线程的操作廉价高效；缺点：实现困难。**
    
* 多对多模型，**用户线程与内核线程混合使用**，用户线程与内核线程是多对多(m:n)的映射模型，既可以保证用户线程的操作廉价，支持大规模并发，又可以使用内核提供的线程调度及处理器映射。

Java中的**线程操作方法都是native方法**，Linux HotSpot下每一个Java线程都映射到一个操作系统原生线程实现，采用**1:1模型**。

#### Java Thread

Java线程状态，java.lang.Thread.State枚举中定义了关于线程生命周期的不同状态；

* NEW  线程创建后，未调用过start()方法
* RUNNABLE  调用start()方法线程已就绪，可能在运行，可能在等系统资源调度，如CPU时间片
* BLOCKED  阻塞表示线程在等待获取锁
* WAITING  正在等待其他线程采取某些操作
* TIMED_WAITING  存在超时条件的等待状态
* TERMINATED  线程退出终止


线程状态相关方法：

* 继承自Object类：
    
    * waite()/wait(long timeout)/wait(long timeout, int nanos)  
        
         使当前线程进入WAITING/TIMED_WAITING状态，**进入等待队列**，并**释放所持有的monitor_lock**。当前线程必须持有monitor_lock，否则抛出异常IllegalMonitorStateException。
        
    * notify()/notifyAll()
        
         **持有的monitor_lock的线程调用该方法**，唤醒任意一个/所有在此monitor_lock上WAITING的线程，当前线程释放monitor后，**被唤醒的线程参与竞争monitor**。
    
* Thread类：

    * start() **只有NEW状态的线程可以调用**，即同一个线程只能调用一次，否则抛出IllegalThreadStateException。线程进入RUNNABLE状态；
    
    * join()/join(long millis)/join(long millis, int nanos) **使用wait()实现**，当前线程里调用其它线程t的join方法，当前线程进入WAITING/TIMED_WAITING状态，
    当前线程不会释放已经持有的对象锁。线程t执行完毕或者millis时间到，当前线程进入RUNNABLE状态。
    
    * yield() 提示调度器当前线程**愿放弃**获取的CPU时间片，**调度器可忽略**，让出CPU后，由运行状态变为就绪状态让调度器再次选择线程，**不释放锁资源**。
    
    * sleep(long millis)/sleep(long millis, int nanos)  使当前线程休眠(WAITING)指定时间间隔，**期间不会释放对象monitor**;调用sleep(0)可以释放
    cpu时间以使其他等待线程能够执行，且当前重新回到就绪队列等待调度而非等待队列;
    
    * run()  在调用该方法的线程内执行。如果该线程是使用单独的Runnable运行对象构造的，则调用该Runnable对象的run方法； 否则，此方法不执行任何操作并返回。
    
    
#### ThreadLocal    

ThreadLocal类在多线程环境下为线程提供**保存线程私有信息的机制，且保证各线程间互不干扰**。thread-local在整个线程生命周期内有效，**使线程可以在关联的不同业务模块之间传递信息**，比
如事务ID、Cookie等上下文相关信息。ThreadLocal有一个内部静态类**ThreadLocalMap**，该内部类是**实现线程隔离机制的关键**，ThreadLocalMap的存储结构Entry定义如下：
```
static class Entry extends WeakReference<ThreadLocal<?>> {
    /** The value associated with this ThreadLocal. */
    Object value;
    
    Entry(ThreadLocal<?> k, Object v) {
        super(k);
        value = v;
    }
}
```
由定义可知，Entry的**Key值为ThreadLocal实例对象，且引用为弱引用**，Value为线程本地变量。即**ThreadLocal**实例本身是不存储值，
它只是**为当前线程提供变量对应的Key**。ThreadLocal类中提供了final属性的成员变量**threadLocalHashCode**，保证同线程内hash不会冲突，
且大多数情况下，线程间也不会冲突，生成规则如下：
```
private final int threadLocalHashCode = nextHashCode();
//保存全局静态工厂方法生成的属于自己的唯一的哈希值

private static AtomicInteger nextHashCode =new AtomicInteger(); 
//全局静态变量，初始化一次，因为所有线程共享，所以需要保证线程安全，故设置为AtomicInteger！

private static final int HASH_INCREMENT = 0x61c88647; 
//黄金分割数

private static int nextHashCode() {
    //所有线程共用一个静态工厂，专门生成哈希值
    return nextHashCode.getAndAdd(HASH_INCREMENT); 
}
```

遇到hash冲突时，ThreadLocalMap采用开放地址法解决哈希冲突.
```
for (Entry e = tab[i];
     e != null;
     e = tab[i = nextIndex(i, len)]) {  //遍历Entry[] tab   nextIndex即为下一个位置
    ThreadLocal<?> k = e.get();
    if (k == key) {
        e.value = value;  //相同则覆盖
        return;
    }
    if (k == null) {
        replaceStaleEntry(key, value, i);  //key为null，删除value 
        return;
    }
}
```

当key==null执行删除是为了**优化ThreadLocalMap的内存泄漏问题**，因为**Entry的key是弱引用，但value是强引用，GC时，
则会出现key被回收，value未被回收，当线程长时间存活(线程池)且频繁操作ThreadLocalMap时，则会发生内存泄漏问题**，所以JDK源码中，为了避免用户忘记显示的调用remove()方法
去除不再使用的变量，在getEntry()、set()方法中，添加删除空值的逻辑。


**ThreadLocalMap的实例对象有线程持有**，Thread类中有两个**ThreadLocal.ThreadLocalMap**类型的成员变量，**用于存储线程的本地变量值**。
```
ThreadLocal.ThreadLocalMap threadLocals = null;
ThreadLocal.ThreadLocalMap inheritableThreadLocals = null;  //从父线程中继承的变量  
* 父线程 创建当前线程的线程（ - -|）
```

虽然Thread对象持有ThreadLocalMap对象的引用，但**ThreadLocalMap的操作方法全部为private属性**，所有关于ThreadLocalMap的操作必须通过ThreadLocal实现。
ThreadLocal的get()、set()、remove()也都是基于ThreadLocalMap的操作。
```
例：
public void remove() {
 ThreadLocalMap m = getMap(Thread.currentThread());
 if (m != null)
     m.remove(this);
}
```







