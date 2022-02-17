---
layout: post 
title:  Kafka Broker网络处理模型
date:   2022-12-20 12:32:02 
categories: Kafka
---

前面的几篇内容中仅分析了Kafka不同类型请求处理的具体实现，如ProduceRequest、FetchRequest、LeaderAndIsrRequest等等，这些请求均是通过KafkaApis对象负责处理，KafkaApis根据
不同的ApiKeys调用对应的handle方法。本篇内容将**从网络模型上分析Kafka如何监听请求、请求如何交给KafkaApis对象，以及处理结果如何响应给客户端或其他Broker**。


# KafkaSever

KafkaServer启动时会完成网络请求处理相关对象的初始化，主要包括三个核心对象：SocketServer、KafkaApis以及KafkaRequestHandlerPool，源码如下：


```
  override def startup(): Unit = {
    ... //other code 
    
    socketServer = new SocketServer(config, metrics, time, credentialProvider, apiVersionManager)
    socketServer.startup(startProcessingRequests = false)
    
    dataPlaneRequestProcessor = createKafkaApis(socketServer.dataPlaneRequestChannel)
    
    dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
      config.numIoThreads, s"${SocketServer.DataPlaneMetricPrefix}RequestHandlerAvgIdlePercent", SocketServer.DataPlaneThreadPrefix)
   ... //other code 
 }
```

下面开始分别分析SocketServer、KafkaApis以及KafkaRequestHandlerPool三部分的作用及源码。

# SocketServer

SocketServer类定义如如下：

```
class SocketServer(val config: KafkaConfig,
                   val metrics: Metrics,
                   val time: Time,
                   val credentialProvider: CredentialProvider,
                   val apiVersionManager: ApiVersionManager)
  extends Logging with KafkaMetricsGroup with BrokerReconfigurable {
  
  //queued.max.requests，request 队列中允许的最多请求数，默认是500
  private val maxQueuedRequests = config.queuedMaxRequests
  private val nodeId = config.brokerId
  // Processor列表
  private val dataPlaneProcessors = new ConcurrentHashMap[Int, Processor]()
  //Acceptor 列表
  private[network] val dataPlaneAcceptors = new ConcurrentHashMap[EndPoint, Acceptor]()
  //负责维护RequestQueue
  val dataPlaneRequestChannel = new RequestChannel(maxQueuedRequests, DataPlaneMetricPrefix, time, apiVersionManager.newRequestMetrics)
```

可以看到，SocketServer对象负责维护当前Broker节点内的Acceptor、Processor以及RequestChannel对象，其中RequestChannel对象中初始化了一个requestQueue，负责缓存请求数据，类型为ArrayBlockingQueue，
大小为配置项`queued.max.requests`的值，默认为500。

```
class RequestChannel(val queueSize: Int ...) extends KafkaMetricsGroup {
  import RequestChannel._
  //待处理的请求队列
  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)
  private val processors = new ConcurrentHashMap[Int, Processor]()
```

## startup

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

Processor同样继承自AbstractServerThread，定义如下：

```
private[kafka] class Processor(...) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {
  //负责处理的SocketChannel列表
  private val newConnections = new ArrayBlockingQueue[SocketChannel](connectionQueueSize)
  
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  //响应队列 无界LinkedBlockingDeque
  private val responseQueue = new LinkedBlockingDeque[RequestChannel.Response]()
  //底层java nio selector
  private val selector = createSelector(...)
  
  ...// other code
}
```
其中重要属性如下：

* newConnections队列负责维护待注册处理的SocketChannel列表，类型为ArrayBlockingQueue阻塞队列，大小为20。
* responseQueue负责维护待发送的响应；
* selector底层为java.nio.channels.Selector，负责处理该Processor管理的SocketChannel上的事件；

## Acceptor工作流程

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


### acceptNewConnections

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


## Processor工作流程

Processor负责处理SocketChannel相应的读写事件。run()方法源码如下：

```
  override def run(): Unit = {
    startupComplete()
    try {
      while (isRunning) {
        try {
          // 遍历取出newConnections队列中的channel，并为其注册OP_READ
          configureNewConnections()
          // 处理responseQueue中的请求
          processNewResponses()
          //监听所有的socketChannel事件
          poll()
          //将API请求放入请求队列中
          processCompletedReceives()
          //处理已经完成的发送
          processCompletedSends()
          //处理断开的连接
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

run()方法的处理流程如下：

* configureNewConnections()：Acceptor调用assignNewConnection()方法将新建立的连接SocketChannel添加入Processor#newConnections队列中后，Processor依次从newConnections队列中
取出SocketChannel，并注册OP_READ事件；
* processNewResponses()：从该Processor对应的responseQueue中取出一个response，进行发送；
* poll()： 遍历注册的SocketChannel，查看是否有事件准备就绪；
* processCompletedReceives()：将接收到请求添加到的RequestChannel对象的requestQueue中；
* processCompletedSends()：处理已经完成的响应发送；
* processDisconnected()：处理断开的SocketChannel；
* closeExcessConnections()：当前Broker的Socket连接数超过配置项`max.connections`(默认Integer.MAX_VALUE)，且Broker只开放了一个监听端口，出于保护Broker的原因，选择一个优先级最低的
SocketChannel进行关闭。

可以看到**Processor并不负责请求的具体业务处理，其核心工作内容主要是两步**：

* 将收到的请求放入RequestChannel对象的requestQueue中；
* 从该Processor对应的responseQueue中取出一个response，进行发送。

下面主要分析processCompletedReceives()方法和processNewResponses()两个方法的实现。


### processCompletedReceives

processCompletedReceives()方法的主要作用是处理接收到请求，将其封装为kafka.network.RequestChannel.Request对象，并将其放入到RequestChannel对象的requestQueue中，其实现如下：

```
  private def processCompletedReceives(): Unit = {
    selector.completedReceives.forEach { receive =>
      try {
        openOrClosingChannel(receive.source) match {
          case Some(channel) =>
            //构建请求
            val header = parseRequestHeader(receive.payload)
            val nowNanos = time.nanoseconds()
            val connectionId = receive.source
            val context = new RequestContext()

            val req = new RequestChannel.Request(processor = id, context = context,
              startTimeNanos = nowNanos, memoryPool, receive.payload, requestChannel.metrics, None)
            //将请求添加到 requestQueue中，如果队列满了，将会阻塞
            requestChannel.sendRequest(req)
            //移除OP_READ事件
            selector.mute(connectionId)
      ...//省略其他代码
  }
```

### processNewResponses

processNewResponses()方法的主要作用是遍历responseQueue中的响应数据，若类型为SendResponse，则调用sendResponse()方法发送该response。源码如下：

```
  private def processNewResponses(): Unit = {
    var currentResponse: RequestChannel.Response = null
    //从responseQueue中依次取出响应
    while ({currentResponse = dequeueResponse(); currentResponse != null}) {
      val channelId = currentResponse.request.context.connectionId
      try {
        currentResponse match {
          case response: SendResponse =>
            //发送响应
            sendResponse(response, response.responseSend)
          ...//
  
  }

  protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send): Unit = {
    val connectionId = response.request.context.connectionId
    if (channel(connectionId).isEmpty) {
      //channel已被关闭 连接被远程关闭或selector因空闲时间过长而关闭
      response.request.updateRequestMetrics(0L, response)
    }
    //发送响应
    if (openOrClosingChannel(connectionId).isDefined) {
      selector.send(new NetworkSend(connectionId, responseSend))
      //记录正在发送的响应，发送完成后调用processCompletedSends()清除
      inflightResponses += (connectionId -> response)
    }
  }
```

# KafkaRequestHandlerPool

KafkaRequestHandlerPool的作用是完成指定数量请求处理线程的创建及启动，类定义如下：

```
class KafkaRequestHandlerPool(val brokerId: Int,
                              val requestChannel: RequestChannel, //请求队列
                              numThreads: Int, //线程数
                              ...) extends Logging with KafkaMetricsGroup {

  private val threadPoolSize: AtomicInteger = new AtomicInteger(numThreads)
    
  val runnables = new mutable.ArrayBuffer[KafkaRequestHandler](numThreads)
  for (i <- 0 until numThreads) {
    //handler创建
    createHandler(i)
  }

  def createHandler(id: Int): Unit = synchronized {
    //所有KafkaRequestHandler共享同一个requestChannel，即请求队列
    runnables += new KafkaRequestHandler(id, brokerId, aggregateIdleMeter, threadPoolSize, requestChannel, apis, time)
    //启动线程
    KafkaThread.daemon(logAndThreadNamePrefix + "-kafka-request-handler-" + id, runnables(id)).start()
  }
```

KafkaRequestHandlerPool初始化时，会按照numThreads的值创建相应数量的KafkaRequestHandler，numThreads的值可通过配置项`num.io.threads`进行配置，默认值为8。创建KafkaRequestHandler
对象时，入参均是同一个RequestChannel对象，即SocketServer初始化时创建的RequestChannel。


## KafkaRequestHandler

KafkaRequestHandler的run()方法实现如下：

```
  def run(): Unit = {
    while (!stopped) {
      val req = requestChannel.receiveRequest(300)
      req match {
        case RequestChannel.ShutdownRequest =>
          //关机请求
          completeShutdown()
          return
        case request: RequestChannel.Request =>
          try {
            //调用KafkaApis处理请求
            apis.handle(request, requestLocal)
          } catch {
            case e: FatalExitError =>
              completeShutdown()
              Exit.exit(e.statusCode)
            case e: Throwable => error("Exception when handling request", e)
          } finally {
            request.releaseBuffer()
          }
        case null => // continue
      }
    }
    completeShutdown()
  }
```

KafkaRequestHandler的run()方法的功能是从RequestChannel的请求队列中取出请求，并调用KafkaApis#handle()方法处理请求。

# KafkaApis#handle

KafkaApis#handle()方法会根据请求内容中的ApiKeys选择指定的方法进行处理。如下：

```
  override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    try {
      request.header.apiKey match {
        case ApiKeys.PRODUCE => handleProduceRequest(request, requestLocal)
        case ApiKeys.FETCH => handleFetchRequest(request)
        case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
        ... //other ApiKeys
        case _ => throw new IllegalStateException(s"No handler for request api key ${request.header.apiKey}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable =>
        requestHelper.handleError(request, e)
    } finally {
      replicaManager.tryCompleteActions()
      // The local completion time may be set while processing the request. Only record it if it's unset.
      if (request.apiLocalCompleteTimeNanos < 0)
        request.apiLocalCompleteTimeNanos = time.nanoseconds
    }
  }
```

handleXXXRequest()方法的处理结果会调用RequestChannel#sendResponse()方法返回响应,RequestChannel#sendResponse()方法中会获取请求对应的Processor，然后
将响应放入Processor的响应队列中(responseQueue)中，由Processor进行发送。

```
  requestChannel.sendResponse(request, new ProduceResponse(mergedResponseStatus.asJava, maxThrottleTimeMs), None)

  //RequestChannel#sendResponse()
  private[network] def sendResponse(response: RequestChannel.Response): Unit = {
    ...// log & other code

    val processor = processors.get(response.processor)
    // The processor may be null if it was shutdown. In this case, the connections
    // are closed, so the response is dropped.
    if (processor != null) {
      processor.enqueueResponse(response)
    }
  }
```

# 总结

Kafka的网络处理模型图例如下：

![Kafka网络模型](https://raw.githubusercontent.com/GuanN1ng/diagrams/main/com.guann1n9.diagrams/kakfa/kafka%20%E7%BD%91%E7%BB%9C%E6%A8%A1%E5%9E%8B.png)
