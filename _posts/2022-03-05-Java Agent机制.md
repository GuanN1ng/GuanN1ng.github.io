---
layout: post 
title:  Java Agent机制
date:   2022-03-05 21:26:20 
categories: APM
---

# JDK API

JDK 1.5后引入了java.lang.instrument包，[官网文档](https://docs.oracle.com/javase/7/docs/api/java/lang/instrument/package-summary.html#package_description) 对其的描述为
`Provides services that allow Java programming language agents to instrument programs running on the JVM. The mechanism for instrumentation is modification of the byte-codes of methods`，
即允许使用Java语言编写agent去监测或协助已在JVM中运行的程序，而实现的机制是通过对方法的字节码进行修改和增强。这样的特性实际上提供了一种虚拟机级别支持的AOP方式，使得开发者无需对原有应用做任何修改，
就可以实现类的动态修改和增强。

## Instrumentation

Instrumentation是Java提供的基于JVMTI的接口，**JVMTI**(JVM Tool Interfac)是JVM暴露出来的一些**基于事件驱动**的供用户扩展的接口集合，通过Instrumentation中的相关API，
用户能够查看并操作Java的类定义及添加jar文件至指定ClassLoader（BootstrapClassLoader或SystemClassLoader）的classpath下。主要API列表如下：

```java
    /**
     * Registers the supplied transformer.
     * The transformer is called when classes are loaded, when they are redefined. 
     * and if canRetransform is true, when they are retransformed. ClassFileTransformer defines the order of transform calls.
     * @since 1.6
     */
    void addTransformer(ClassFileTransformer transformer, boolean canRetransform);
    
    /**
     * Same as addTransformer(transformer, false).
     */
    void addTransformer(ClassFileTransformer transformer);

    /**
     * Unregisters the supplied transformer
     */
    boolean removeTransformer(ClassFileTransformer transformer);

    /**
     * Can-Retransform-Classes manifest attribute is set to true in the agent JAR file(MANIFEST.MF)
     * JVM supports retransformation capability.
     */
    boolean isRetransformClassesSupported();
    
    /**
     * instrumentation for already loaded classes,Instances of the retransformed class are not affected.
     * The retransformation may change method bodies, the constant pool and attributes (unless explicitly prohibited). 
     * The retransformation must not add, remove or rename fields or methods, change the signatures of methods, or change inheritance
     */
    void retransformClasses(Class<?>... classes) throws UnmodifiableClassException;

    /**
     * Can-Redefine-Classes manifest attribute is set to true in the agent JAR file(MANIFEST.MF)
     * JVM supports redefinition capability.
     */
    boolean isRedefineClassesSupported();
    
    /**
     * replace the definition of a class without reference to the existing class file bytes,Instances of the retransformed class are not affected.
     * The retransformation may change method bodies, the constant pool and attributes (unless explicitly prohibited). 
     * The retransformation must not add, remove or rename fields or methods, change the signatures of methods, or change inheritance
     */
    void redefineClasses(ClassDefinition... definitions) throws  ClassNotFoundException, UnmodifiableClassException;

    /**
     * Specifies a JAR file with instrumentation classes to be defined by the bootstrap class loader.
     */
    void appendToBootstrapClassLoaderSearch(JarFile jarfile);
    /**
     * Specifies a JAR file with instrumentation classes to be defined by the system class loader.
     * see ClassLoader.getSystemClassLoader()
     */
    void appendToSystemClassLoaderSearch(JarFile jarfile);
```

Instrumentation中提供了两种对类定义进行修改的方式，retransformClasses和redefineClasses：

* retransformClasses：调用retransformClasses方法后，JVM会回调已注册的ClassFileTransformer在已加载的类的字节码文件上完成修改；
* redefineClasses：直接使用提供的字节码文件替换掉已存在的class文件。

基于上述，更推荐使用retransformClasses方法，避免在多个JavaAgent同时工作时，导致对字节码的增强丢失。

## ClassFileTransformer

ClassFileTransformer是一个接口类，只有一个方法transform， 通过Instrumentation#addTransformer注册ClassFileTransformer的实现类后，
后续JVM运行中**当发生类加载、类重定义(redefined)、类转换(retransformed)的行为后，JVM会回调所有已注册的ClassFileTransformer的transform方法**，完成类修改动作。

```
    byte[]
    transform(  ClassLoader         loader,
                String              className,
                Class<?>            classBeingRedefined,
                ProtectionDomain    protectionDomain,
                byte[]              classfileBuffer)
        throws IllegalClassFormatException;
```

# Java Agent

Java Agent一般以jar包的形式存在，主要包含两部分内容：实现代码和配置文件。实现代码主要包含JavaAgent的启动入口类、用户实现的ClassFileTransformer以及部分业务代码。
配置文件是指位于Jar包META-INF目录下的MANIFEST.MF文件。

## 启动类及启动方式

Java Agent被允许有两种启动类：premain及agentmain，








## MANIFEST.MF

MANIFEST.MF文件中，和探针相关的配置如下：

* Premain-Class：
* Agentmain-Class：
* Can-Redefine-Classes：
* Can-Retransform-Classes：

示例文件如下：

```
Manifest-Version: 1.0
Premain-Class: xxxx
Agentmain-Class: xxxx
Archiver-Version: Plexus Archiver
Built-By: admin
Can-Redefine-Classes: true
Can-Retransform-Classes: true
Created-By: Apache Maven 3.8.4
Build-Jdk: 1.8.0_301
```

如使用maven打包，可在pom.xml中添加如下内容，自动生成MANIFEST.MF文件。

```
<transformers>
    <transformer
            implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
        <manifestEntries>
            <Premain-Class>${PremainClass}</Premain-Class>
            <Agentmain-Class>${AgentmainClass}</Agentmain-Class>
            <Can-Redefine-Classes>true</Can-Redefine-Classes>
            <Can-Retransform-Classes>true</Can-Retransform-Classes>
        </manifestEntries>
    </transformer>
</transformers>

```


## 字节码修改类库


