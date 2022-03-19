---
layout: post 
title:  Java Agent机制
date:   2022-03-05 21:26:20 
categories: 全链路压测
---

JDK 1.5中引入了java.lang.instrument包，[官网文档](https://docs.oracle.com/javase/7/docs/api/java/lang/instrument/package-summary.html#package_description) 对其的描述为
`Provides services that allow Java programming language agents to instrument programs running on the JVM. The mechanism for instrumentation is modification of the byte-codes of methods`，
即允许使用Java语言编写agent去监测或协助已在JVM中运行的程序，而实现的机制是通过对方法的字节码进行修改和增强。这样的特性实际上提供了一种虚拟机级别支持的AOP方式，使得开发者无需对原有应用做任何修改，
就可以实现类的动态修改和增强。

# Java Agent

Java Agent一般以jar包的形式存在，主要包含两部分内容：实现代码和配置文件，


Manifest、Agent Class和ClassFileTransformer。


```
Manifest-Version: 1.0
Archiver-Version: Plexus Archiver
Created-By: Apache Maven
Built-By: 
Build-Jdk: 1.8.0_281
Agent-Class: 
Can-Redefine-Classes: true
Can-Retransform-Classes: true
Premain-Class: 
```

# 启动方式

从命令行启动和使用Attach机制启动。

# Instrumentation


