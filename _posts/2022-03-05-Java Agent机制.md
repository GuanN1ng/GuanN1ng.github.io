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

Instrumentation是Java提供的基于JVMTI的接口，**JVMTI**(JVM Tool Interfac)是JVM暴露出来的一些**基于事件驱动**的供用户扩展的接口集合，Instrumentation中的API主要包含三部分功能：
* 添加或移除ClassFileTransFormer;
  * addTransformer方法有一个重载方法，带有一个布尔类型的参数`canRetransform`。这个参数的含义是指是否对已加载的类调用该transformer进行重新转换;
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

ClassFileTransformer是一个接口类，有一个默认方法transform， 通过Instrumentation#addTransformer注册ClassFileTransformer的实现类后，
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

## 启动方式

Java Agent的启动类一般需声明两个方法：premain 和 agentmain，两种方法分别对应着探针的两种启动方式，通过命令行加载（-javaagent） 和 通过JAVA API动态加载。premain和agentmain方法最重要的功能是通过
方法入参Instrumentation的addTransformer方法完成用户自定义的ClassFileTransformer的注册。

### premain

当Java Agent是一个可执行的JAR文件，并**通过-javaagent参数被添加到目标应用的启动参数中时**，JVM会在执行目标应用jar文件的main方法执行之前调用探针的premain方法，执行完探针逻辑。premain的声明
形式如下：

```java

public static void premain(String agentArgs, Instrumentation inst) {
    // 在 premain 方法中执行 Java Agent 的逻辑
    System.out.println("Java Agent premain method called");
    inst.addTransformer(/* 用户自定义classTransformer*/,true);
}
```

### agentmain 

若目标应用已启动，则可通过在其他Java进程中调用固定的JAVA API为指定Java进程动态的添加（attach）探针。此时，JVM则会调用对应探针的agentmain方法来加载探针。agentmain的声明
形式如下：

```java
public static void agentmain(String agentArgs, Instrumentation inst) {
    // 在 agentmain 方法中执行 Java Agent 的逻辑
    System.out.println("Java Agent agentmain method called");
    inst.addTransformer(/* 用户自定义classTransformer*/,false)
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

用户通过实现ClassFileTransformer接口，重写transform方法完成对class字节码的修改。transform方法声明如下：
```
    byte[]  //修改后的class字节码
    transform(  
                //当前class的classloader，若class由BootstrapClassLoader加载，则为null
                ClassLoader         loader,
                //class的全限定名，分隔符为"/"   例 java/util/List            
                String              className,
                //若本次调用是由class redefine触发的,则为重定义的类，否则为 null
                Class<?>            classBeingRedefined,
                //类的保护域
                ProtectionDomain    protectionDomain,
                //当前class的字节码
                byte[]              classfileBuffer)
        throws IllegalClassFormatException;
```

debug视图如下：

![transform invoke](https://raw.githubusercontent.com/GuanN1ng/GuanN1ng.github.io/main/_posts/image/class_redefined.png)

常见的Java字节码级别操作的库有ASM、Byte Buddy和 Javassist，ASM提供了最底层的字节码操作能力，而Byte Buddy和Javassist则提供了更高级别的抽象和更方便的API。
有如下一段简单的代码：
```
package com.example.demo.config;

public class AgentDemo {
    
    public void demo(){
        System.out.println("biz code");
    }
}
```
下面分别使用ASM、Byte Buddy和 Javassist实现在demo方法中添加一行start和end的输出，修改后的class逻辑应如下：
```
package com.example.demo.config;

public class AgentDemo {
    
    public void demo(){
        System.out.println("start");
        System.out.println("biz code");
        System.out.println("end");
    }
}
```

### ASM

ASM类库中的核心类主要有四个：
* ClassReader：读取并解析ClassFile内容，针对遇到的每个字段、方法和字节码指令调用给定ClassVisitor的相应访问方法

```
#构造方法
public ClassReader(byte[] classFile)
public ClassReader(final String className)

#注册ClassVisitor，后续ClassReader会调用ClassVisitor的相关方法
public void accept(final ClassVisitor classVisitor, final int parsingOptions)
```

* ClassVisitor：抽象类，定义了一系列访问类数据的方法，由ClassReader调用，用户可继承ClassVisitor覆写其方法逻辑，以实现指定业务，如对方法进行修改，则需覆写visitMethod方法。

```
# 方法调用顺序
visit [ visitSource ] [ visitModule ][ visitNestHost ][ visitOuterClass ] ( visitAnnotation | visitTypeAnnotation | visitAttribute )* ( visitNestMember | [ * visitPermittedSubclass ] | visitInnerClass | visitRecordComponent | visitField | visitMethod )* visitEnd
```

* MethodVisitor：封装了一系列用于生成或修改class方法的API，通过ClassVisitor#visitMethod方法获取，如visitInsn，访问方法的一条指令
* ClassWriter：以二进制形式生成编译后的类，调用toByteArray方法来获取

其余还有FieldVisitor、ModuleVisitor、AnnotationVisitor等，可自行了解。

ASM maven依赖如下：
```
<dependency>
    <groupId>org.ow2.asm</groupId>
    <artifactId>asm</artifactId>
    <version>9.3</version>
</dependency>
```

代码功能实现：

```
//实现ClassFileTransformer 
//修改 com.example.demo.config.AgentDemo#demo方法
public class AsmClassTransformer implements ClassFileTransformer {
    
    @Override
    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
        String classReference = className.replace("/", ".");
        if(!"com.example.demo.config.AgentDemo".equals(classReference)){
            //非目标类，返回
            return classfileBuffer;
        }
        //构建ClassReader   ClassWriter
        ClassReader classReader = new ClassReader(classfileBuffer);
        ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        //注册ClassVisitor
        classReader.accept(new AsmMethodVisitor(Opcodes.ASM8,classWriter), ClassReader.EXPAND_FRAMES);
        return classWriter.toByteArray();
    }
    
    public class AsmMethodVisitor extends ClassVisitor {

        public AsmMethodVisitor(int api, ClassVisitor classVisitor) {
            super(api, classVisitor);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
            //获取mv
            MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);
            if(!"demo".equals(name)){
                //非目标方法，返回
                return mv;
            }
            //修改方法体
            return new MethodVisitor(this.api, mv) {
                //visitCode方法 进入方法时被调用
                @Override
                public void visitCode() {
                    mv.visitFieldInsn(Opcodes.GETSTATIC,"java/lang/System","out","Ljava/io/PrintStream;");
                    mv.visitLdcInsn("start");
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,"java/io/PrintStream","println","(Ljava/lang/String;)V");
                    super.visitCode();
                }

                @Override
                public void visitInsn(int opcode) {
                    //方法返回前修改
                    if(opcode != Opcodes.ARETURN && opcode != Opcodes.RETURN ) {
                        return;
                    }
                    mv.visitFieldInsn(Opcodes.GETSTATIC,"java/lang/System","out","Ljava/io/PrintStream;");
                    mv.visitLdcInsn("end");
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,"java/io/PrintStream","println","(Ljava/lang/String;)V");
                    super.visitInsn(opcode);
                }
            };
        }
    }
}
```

### Javassist

Javassist是一个轻量级的Java字节码操作库，相比于ASM，主要优势在于其简单易用的API，但Javassist相对较慢的性能可能是其在某些场景下的劣势。核心代码介绍如下：
* ClassPool：Javassist中用于存储和管理CtClass对象的容器。它提供了查找、创建、修改CtClass对象的方法；
* CtClass：CtClass对象代表了一个Java类。通过类池（ClassPool）可获取CtClass对象，CtClass提供了一批访问class信息的API，如获取指定方法getDeclaredMethod；
* CtMethod：标识Java类中的一个方法，通过`CtClass.getDeclaredMethod(methodName)`获取，提供了访问及修改方法的API；
* CtField：标识Java类中的一个属性，通过`CtClass.getDeclaredField(fieldName)`获取，提供了访问及修改属性的API。

maven依赖为：

```
<dependency>
    <groupId>javassist</groupId>
    <artifactId>javassist</artifactId>
    <version>3.12.1.GA</version>
</dependency>
```

代码功能实现如下：

```
public class JavasisstClassTransformer implements ClassFileTransformer {

    private static final ClassPool  classPool = ClassPool.getDefault();

    @Override
    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
        String classReference = className.replace("/", ".");
        if (!"com.example.demo.config.AgentDemo".equals(classReference)) {
            //非目标类，返回
            return classfileBuffer;
        }
        try {
            //添加当前classLoader的classPath到ClassPool中，防止NotFoundException
            classPool.appendClassPath(new LoaderClassPath(loader));
            //获取目标类
            CtClass ctClass = classPool.get(classReference);
            //获取目标方法
            CtMethod targetMethod = ctClass.getDeclaredMethod("demo");
            //插入自定义代码
            targetMethod.insertBefore("System.out.println(\"start\");");
            targetMethod.insertAfter("System.out.println(\"end\");");
            byte[] bytecode = ctClass.toBytecode();
            //Removes this CtClass object from the ClassPool. After this method is called, any method cannot be called on the removed CtClass object
            //解除class冻结
            ctClass.detach();
            return bytecode;
        } catch (Exception e) {
            return classfileBuffer;
        }
    }
}
```


### Byte Buddy

Byte Buddy提供了两种层面的类增强方式：
* 方法代理，即为每个目标方法增强都会生成一个新的代理类，通过代理类实现对目标方法增强，skywalking即使用此种增强；
* 修改Class字节码，直接修改原有Class的字节码，不会生成新的代理类，OpenTelemetry使用此种方式。

maven依赖

```
<dependency>
    <groupId>net.bytebuddy</groupId>
    <artifactId>byte-buddy</artifactId>
    <version>1.14.13</version>
</dependency>
```

#### 方法代理

1、创建一个拦截器，用于增强目标方法。

```
public class ByteBuddyInterceptor {
    
    @RuntimeType
    public static Object intercept(@This Object target,    // 当前拦截的目标对象{this}
                                   @AllArguments Object[] allArguments,   // 方法入参
                                   @SuperCall Callable<?> superCall,      // 代理对象
                                   @Origin Method method                  // 当前拦截的目标方法
    ) throws Throwable {
        //目标方法前置逻辑
        System.out.println("end");
        //调用目标方法
        Object result = superCall.call();
        //目标方法后置处理
        System.out.println("end");
        return result;
    }
}
```


2、使用Byte Buddy API进行增强

```
public static void premain(String agentArgs, Instrumentation inst) {

  new AgentBuilder.Default()
      // 增强的类
      .type(ElementMatchers.named("com.example.demo.config.AgentDemo"))
      // 增强的类需 增强的方法实现
      .transform(new AgentBuilder.Transformer() {
          public DynamicType.Builder<?> transform(DynamicType.Builder<?> builder, TypeDescription typeDescription,
                                                  ClassLoader classLoader, JavaModule module, ProtectionDomain protectionDomain) {
              // 增强方法demo
              return builder.method(ElementMatchers.named("demo"))
                      // 设置拦截器
                      .intercept(MethodDelegation.to(ByteBuddyInterceptor.class));
          }
      })
      // 监听类加载
      .installOn(inst);
}
```

执行arthas sc命令后，可以看到，生成了新的代理类；
```
[arthas@38800]$ sc *AgentDemo*
com.example.demo.config.AgentDemo
com.example.demo.config.AgentDemo$auxiliary$EaV0UxCt
```

AgentDemo的反编译代码如下，demo方法内部被修改为通过预定义的拦截器去调用代理类的call方法。

![byte buddy 方法代理]()


#### 字节码修改

