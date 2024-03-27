---
layout: post 
title:  Java Agent
date:   2022-03-05 21:26:20 
categories: APM
---

# JDK API

JDK 1.5后引入了java.lang.instrument包，[官网文档](https://docs.oracle.com/javase/7/docs/api/java/lang/instrument/package-summary.html#package_description) 对其的描述为
`Provides services that allow Java programming language agents to instrument programs running on the JVM. The mechanism for instrumentation is modification of the byte-codes of methods`，
即允许使用Java语言编写agent去监测或协助已在JVM中运行的程序，而实现的机制是通过对方法的字节码进行修改和增强。这样的特性实际上提供了一种虚拟机级别支持的AOP方式，使得开发者无需对原有应用做任何修改，
就可以实现类的动态修改和增强。

## Instrumentation

Instrumentation是Java提供的基于JVMTI的接口，**JVMTI**(JVM Tool Interfac)是JVM暴露出来的一些**基于事件驱动**的供用户扩展的接口集合，Instrumentation中的API主要包含三部分功能：
* 添加或移除ClassFileTransFormer；
* 对已完成加载的类进行retransform或redefine；
* 添加jar文件至指定ClassLoader（BootstrapClassLoader或SystemClassLoader）的classpath。
主要API列表如下：

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

Java Agent是指依赖Instrumentation机制实现的一个独立的jar包，主要包含两部分内容：实现代码和配置文件。实现代码主要包含JavaAgent的启动入口类、用户实现的ClassFileTransformer以及部分业务代码。
配置文件是指位于Jar包META-INF目录下的MANIFEST.MF文件。

## 启动方法及启动方式

Java Agent的启动类一般需声明两个方法：premain 和 agentmain，两种方法分别对应着探针的两种启动方式，通过命令行加载（-javaagent） 和 通过JAVA API动态加载。

### premain

当Java Agent是一个可执行的JAR文件，并**通过-javaagent参数被添加到目标应用的启动参数中时**，JVM会在执行目标应用jar文件的main方法执行之前调用探针的premain方法，执行完探针逻辑。premain的声明
形式如下：

```java

public static void premain(String agentArgs, Instrumentation inst) {
    // 在 premain 方法中执行 Java Agent 的逻辑
    System.out.println("Java Agent premain method called");
}
```

### agentmain 

若目标应用已启动，则可通过在其他Java进程中调用固定的JAVA API为指定Java进程动态的添加（attach）探针。此时，JVM则会调用对应探针的agentmain方法来加载探针。agentmain的声明
形式如下：

```java
public static void agentmain(String agentArgs, Instrumentation inst) {
    // 在 agentmain 方法中执行 Java Agent 的逻辑
    System.out.println("Java Agent agentmain method called");
}
```

动态Attach探针的代码如下：
```java
public static void main(String[] args) throws Exception {
    String pid = "1234"; // 目标 Java 进程的 PID
    String agentPath = "/path/to/your/agent.jar"; // Java Agent JAR 文件的路径
    VirtualMachine vm = VirtualMachine.attach(pid);
    vm.loadAgent(agentPath, "argument to agentmain method");
    vm.detach();
}
```

## MANIFEST.MF

Java Agent的jar包内需在 META-INF目录下创建MANIFEST.MF文件，其中需声明探针相关的配置，示例文件如下：

```
Manifest-Version: 1.0
Premain-Class: xxxx
Agentmain-Class: xxxx
Can-Redefine-Classes: true
Can-Retransform-Classes: true
Created-By: Apache Maven 3.8.4
Build-Jdk: 1.8.0_301
```

* Premain-Class：premain方法所在的Class全路径
* Agentmain-Class：agentmain方法所在的Class全路径，可与premain方法声明在同一个类中
* Can-Redefine-Classes：声明Java Agent是否具有重新定义类（Redefine Classes）的能力
* Can-Retransform-Classes：声明Java Agent是否具有重新转换类（Retransform Classes）的能力

如使用maven打包，可在pom.xml中添加如下内容，自动生成MANIFEST.MF文件。

```
<configuration>
    ...
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
    ...
</configuration>

```

## ClassFileTransformer


常见的Java字节码级别操作的库有ASM、Byte Buddy和 Javassist，ASM提供了最底层的字节码操作能力，而Byte Buddy和Javassist则提供了更高级别的抽象和更方便的API。




