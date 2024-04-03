---
layout: post
title:  TransmittableThreadLocal
date:   2022-03-11 19:12:53
categories: APM
---

# 背景

JDK中的java.lang.Thread类有两个Map结构的成员属性：threadLocals和inheritableThreadLocals，用于维护线程的本地变量，定义如下：

```
//ThreadLocal values pertaining to this thread. This map is maintained by the ThreadLocal class. 
ThreadLocal.ThreadLocalMap threadLocals = null;

//InheritableThreadLocal values pertaining to this thread. This map is maintained by the InheritableThreadLocal class.
ThreadLocal.ThreadLocalMap inheritableThreadLocals = null;
```

其中inheritableThreadLocals使用**java.lang.InheritableThreadLocal**进行维护，**用于完成父子线程间的线程变量传递**。当创建线程时，会将父线程的inheritableThreadLocals
复制并赋值给新建线程的inheritableThreadLocals，完成变量传递(**此处为浅拷贝，可自行继承InheritableThreadLocal，重写childValue方法，实现深拷贝**)，实现如下：

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

**父线程的inheritableThreadLocals仅在子线程第一次初始化时才会被复制到子线程中**，对于使用线程池的场景下，由于线程被复用，后续提交任务的线程的inheritableThreadLocals并不会被执行任务的线程所
继承，若任务执行需要依赖inheritableThreadLocals中的对象，则会导致错乱。

# TransmittableThreadLocal

[TransmittableThreadLocal(TTL)](https://github.com/alibaba/transmittable-thread-local )是阿里开源的，用于解决异步执行时线程上下文传递问题的组件，在InheritableThreadLocal基础上，实现了线程复用(线程池)场景下的线程变量传递功能。

## 使用

TTL的使用方式大致可分为两种：编码时直接使用TTL的API和javaagent方式，采用javaagent方式时，无需对项目代码进行修改，TTL Agent会自动增强相关目标类，具体可从`com.alibaba.ttl.threadpool.agent.TtlAgent#premain`方法进行源码阅读，
且类增强的工具类库为Javassist（探针工作逻辑可见[Java Agent机制](https://guann1ng.github.io/apm/2022/03/05/Java-Agent%E6%9C%BA%E5%88%B6/)）。

### 原生API

使用原生API时，首先需在项目中添加依赖，maven依赖如下：

```
<dependency>
    <groupId>com.alibaba</groupId>
    <artifactId>transmittable-thread-local</artifactId>
    <version>2.14.5</version>
</dependency>
```







### Java Agent



## 实现原理











