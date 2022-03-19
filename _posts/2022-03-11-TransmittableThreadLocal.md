---
layout: post
title:  TransmittableThreadLocal
date:   2022-03-11 19:12:53
categories: 全链路压测
---

JDK中的java.lang.Thread类有两个Map结构的成员属性：threadLocals和inheritableThreadLocals，用于维护线程的本地变量，定义如下：

```
//ThreadLocal values pertaining to this thread. This map is maintained by the ThreadLocal class. 
ThreadLocal.ThreadLocalMap threadLocals = null;

//InheritableThreadLocal values pertaining to this thread. This map is maintained by the InheritableThreadLocal class.
ThreadLocal.ThreadLocalMap inheritableThreadLocals = null;
```

其中inheritableThreadLocals通过java.lang.**InheritableThreadLocal**进行维护，即ThreadLocalMap中的key为InheritableThreadLocal对象。**用于父子线程间的线程变量传递**，
当，实现如下：

```
private void init(ThreadGroup g, Runnable target, String name, long stackSize, AccessControlContext acc, boolean inheritThreadLocals) {
    //调用new Thread()方法的线程即为新建线程的父线程   
    Thread parent = currentThread();
    //父线程的inheritableThreadLocals属性不为null
    if (inheritThreadLocals && parent.inheritableThreadLocals != null)
        //传递变量
        this.inheritableThreadLocals = ThreadLocal.createInheritedMap(parent.inheritableThreadLocals);
    
    ...//other code
}
```

