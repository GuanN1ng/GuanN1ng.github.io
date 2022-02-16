---
layout: post 
title:  Kafka Broker网络处理模型
date:   2022-12-20 12:32:02 
categories: Kafka
---

前面的几篇内容中仅分析了Kafka不同类型请求处理的具体实现，如ProduceRequest、FetchRequest、LeaderAndIsrRequest等等，这些请求均是通过KafkaApis对象负责处理，KafkaApis根据
不同的ApiKeys调用对应的handle方法。本篇内容将**从网络模型上分析Kafka如何监听请求、请求如何交给KafkaApis对象，以及处理结果如何响应给客户端或其他Broker**。


# Kafka网络处理流程概述

KafkaServer启动时会完成网络请求处理相关对象的初始化，包括SocketServer、KafkaApis以及KafkaRequestHandlerPool，源码如下：


```
  override def startup(): Unit = {
    ... //other code 
    
    socketServer = new SocketServer(config, metrics, time, credentialProvider, apiVersionManager)
    socketServer.startup(startProcessingRequests = false)
    
    //KafkaApis初始化
    dataPlaneRequestProcessor = createKafkaApis(socketServer.dataPlaneRequestChannel)
    
    dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
      config.numIoThreads, s"${SocketServer.DataPlaneMetricPrefix}RequestHandlerAvgIdlePercent", SocketServer.DataPlaneThreadPrefix)
   ... //other code 
 }
```





# SocketServer初始化

SocketServer启动时，核心功能是调用createDataPlaneAcceptorsAndProcessors()方法完成Acceptor和Processor的创建，源码如下：

```
  def startup(startProcessingRequests: Boolean = true,
              controlPlaneListener: Option[EndPoint] = config.controlPlaneListener,
              dataPlaneListeners: Seq[EndPoint] = config.dataPlaneListeners): Unit = {
    this.synchronized {
      //负责controler相关请求处理，此处暂不关心 
      createControlPlaneAcceptorAndProcessor(controlPlaneListener)
      //负责其他数据请求处理
      createDataPlaneAcceptorsAndProcessors(config.numNetworkThreads, dataPlaneListeners)
      if (startProcessingRequests) {
        this.startProcessingRequests()
      }
    }
```

createDataPlaneAcceptorsAndProcessors()方法中遍历Broker需监听的端口(配置项`listeners`)，为每个监听端口完成Acceptor和Processor的创建：

* 调用createAcceptor()方法创建Acceptor；
* 调用addDataPlaneProcessors()方法为Acceptor创建Processor;

```
  private def createDataPlaneAcceptorsAndProcessors(dataProcessorsPerListener: Int,
                                                    endpoints: Seq[EndPoint]): Unit = {
    //遍历所有的监听端口                                                
    endpoints.foreach { endpoint =>
      connectionQuotas.addListener(config, endpoint.listenerName)
      //创建Acceptor
      val dataPlaneAcceptor = createAcceptor(endpoint, DataPlaneMetricPrefix)
      //创建Processor
      addDataPlaneProcessors(dataPlaneAcceptor, endpoint, dataProcessorsPerListener)
      dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
      info(s"Created data-plane acceptor and processors for endpoint : ${endpoint.listenerName}")
    }
  }
```

## Acceptor初始化

Acceptor类定义及重要属性如下：

```
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              nodeId: Int,
                              connectionQuotas: ConnectionQuotas,
                              metricPrefix: String,
                              time: Time,
                              logPrefix: String = "") extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  this.logIdent = logPrefix
  //底层 java nio selector
  private val nioSelector = NSelector.open()
  //打开ServerSocketChanel 监听TCP链接请求
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)
  //Processor列表
  private val processors = new ArrayBuffer[Processor]()
  //连接数超限待关闭
  private[network] val throttledSockets = new mutable.PriorityQueue[DelayedCloseSocket]()
  
}  
```

Acceptor继承自AbstractServerThread(继承自Runnable)，初始化时，会调用openServerSocket()方法打开ServerSocketChanel，开始监听TCP链接请求。

## Processor初始化

addDataPlaneProcessors()方法会按照配置项`num.network.threads`(默认为3)创建Processor列表，并设置到Acceptor中。

```
  private def addDataPlaneProcessors(acceptor: Acceptor, endpoint: EndPoint, newProcessorsPerListener: Int): Unit = {
    val listenerName = endpoint.listenerName
    val securityProtocol = endpoint.securityProtocol
    val listenerProcessors = new ArrayBuffer[Processor]()
    val isPrivilegedListener = controlPlaneRequestChannelOpt.isEmpty && config.interBrokerListenerName == listenerName
    //newProcessorsPerListener的值为num.network.threads，默认为3
    for (_ <- 0 until newProcessorsPerListener) {
      //创建Processor
      val processor = newProcessor(nextProcessorId, dataPlaneRequestChannel, connectionQuotas, listenerName, securityProtocol, memoryPool, isPrivilegedListener)
      listenerProcessors += processor
      dataPlaneRequestChannel.addProcessor(processor)
      nextProcessorId += 1
    }
    listenerProcessors.foreach(p => dataPlaneProcessors.put(p.id, p))
    //将Processors列表设置到Acceptor中
    acceptor.addProcessors(listenerProcessors, DataPlaneThreadPrefix)
  }
```

Processor同样继承自AbstractServerThread，其内部维护了负责处理的SocketChannel列表，类型为ArrayBlockingQueue阻塞队列，大小为20。类定义如下：

```
private[kafka] class Processor(...) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {
  //负责处理的SocketChannel列表
  private val newConnections = new ArrayBlockingQueue[SocketChannel](connectionQueueSize)
  
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  //响应队列
  private val responseQueue = new LinkedBlockingDeque[RequestChannel.Response]()
  //底层java nio selector
  private val selector = createSelector(...)
  
  ...// other code
}
```

# Acceptor工作流程

Acceptor线程启动后，会首先注册OP_ACCEPT事件，监听是否有新的连接，然后调用acceptNewConnections()方法对新的连接进行处理。run()方法源码如下：
```
  def run(): Unit = {
    //注册 OP_ACCEPT 事件，监听是否有新的连接
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    //线程启动完成标志
    startupComplete()
    try {
      while (isRunning) {
        try {
          //处理新的连接
          acceptNewConnections()
          //关闭单IP创建连接速率过快的请求 connection_creation_rate
          closeThrottledConnections()
        }
        catch {
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      ... //关闭处理 Closing server socket, selector, and any throttled sockets
    }
  }
```


## acceptNewConnections

acceptNewConnections()方法主要完成两项工作：

* 1、遍历就绪的连接事件，并调用accept()方法建立SocketChannel(可能失败,单ip连接数上限max.connections.per.ip,默认值Integer.MAX_VALUE)；
* 2、将SocketChannel以轮询的方式的方式指派给某一个Processor；

```
  private def acceptNewConnections(): Unit = {
    val ready = nioSelector.select(500)
    if (ready > 0) {
      //遍历连接就绪事件
      val keys = nioSelector.selectedKeys()
      val iter = keys.iterator()
      while (iter.hasNext && isRunning) {
        try {
          val key = iter.next
          iter.remove()

          if (key.isAcceptable) {
            //创建SocketChannel，可能失败 单ip连接数上限 max.connections.per.ip  默认值Integer.MAX_VALUE
            accept(key).foreach { socketChannel =>
              var retriesLeft = synchronized(processors.length)
              var processor: Processor = null
              //轮询分配给每个Processor，直到分配成功
              do {
                retriesLeft -= 1
                processor = synchronized {
                  currentProcessorIndex = currentProcessorIndex % processors.length
                  processors(currentProcessorIndex)
                }
                currentProcessorIndex += 1
                //retriesLeft == 0， 是否阻塞 ，循环至最后一个processor，阻塞等待，直至添加至newConnections队列成功
              } while (!assignNewConnection(socketChannel, processor, retriesLeft == 0))
            }
          } else
            throw new IllegalStateException("Unrecognized key state for acceptor thread.")
        } catch {
          case e: Throwable => error("Error while accepting connection", e)
        }
      }
    }
  }
```


# Processor工作流程

Acceptor调用assignNewConnection()方法将新建立的连接SocketChannel添加入Processor#newConnections队列中后，Processor开始为newConnections队列中
的SocketChannel处理相应的读写事件。run()方法源码如下：

```
  override def run(): Unit = {
    startupComplete()
    try {
      while (isRunning) {
        try {
          // 遍历取出newConnections队列中的channel，并为其注册OP_READ
          configureNewConnections()
          // register any new responses for writing
          processNewResponses()
          poll()
          processCompletedReceives()
          processCompletedSends()
          processDisconnected()
          closeExcessConnections()
        } catch {
          case e: Throwable => processException("Processor got uncaught exception.", e)
        }
      }
    } finally {
      debug(s"Closing selector - processor $id")
      CoreUtils.swallow(closeAll(), this, Level.ERROR)
      shutdownComplete()
    }
  }
```


# KafkaRequestHandlerPool