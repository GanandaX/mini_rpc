# mini_rpc

```txt
基于 mini_muduo 实现的第一版 C++ RPC。
构建前，请将 mini_rpc 和 mini_muduo 克隆到同一父目录下，并确保 mini_muduo 的本地目录名为 mini_muduo。
```

## Scope

- 服务端注册 service 和 method。
- 客户端通过 service、method 和字符串 payload 发起异步调用。
- 服务端返回成功 payload 或失败 errorMessage。

本版本不包含：

- 复杂参数与复杂序列化；
- 服务发现、负载均衡、注册中心；

## Build and run

```bash
cmake -S . -B build
cmake --build build

./build/rpc_echo_example
```

rpc_echo_example 会在同一进程中启动 Echo 服务端和客户端，输出响应 payload 后优雅退出。

***

rpc_echo_server_example 会启动Echo服务端，其可以通过后接端口来指定端口号或不输入等待系统自动分配。server中包含三个方法，分别为echo、async_echo 和 async_echo_2。在server启动后会打印ip和端口。在调用rpc_echo_client_example时需在后面指定server的ip和端口。client在执行完后自动退出，但server会常驻。

```bash
./build/rpc_echo_server_example 
server: 127.0.0.1:41545            
pid: 308728
```  

```txt
第一个终端
实际运行时会显示当前状态下的ip和端口 
```  

<br>

```bash
./build/rpc_echo_client_example 127.0.0.1 41545
```

```txt
第二个终端
实际运行时要用服务端打印的端口
```

<br>

`指定端口`

```bash
./build/rpc_echo_server_example 1122
server: 127.0.0.1:1122
pid: 311062
```  

```txt
第一个终端 
```

<br>

```bash
./build/rpc_echo_client_example 127.0.0.1 1122
```

```txt
第二个终端
```

## 基本用法

### 服务端

``` C++
// 为server设置工作线程数
// setWorkerThreadNum只能在首次 start() 前、base loop 中设置一次，数量必须大于零。stop() 不在 base loop 等待业务任务
// 停止完成回调会等 TCP 停止、线程池已接收的任务执行完并退出后触发。这里等待的是 handler 任务返回，不包含 handler 自行安排的外部异步工作
// 当注册了异步方法且设置了工作线程后，服务端的异步处理模块都会将其交给工作线程池处理。未配置业务线程池时，异步 handler 在连接所属的 IO loop 执行
// 已经完成的请求不会再收到失败回调，允许重发的请求还会按重发配置处理断线
RpcServer server(&loop, serverFd);
server.setWorkerThreadNum(4);

```

<br>

``` C++
// 设置业务线程池的最大排队任务数，必须大于零
// 默认不限制，只统计排队任务；在 setWorkerThreadNum() 之后、首次 start() 前由 base loop 设置；
// 队列满返回 RESOURCE_EXHAUSTED 和 "task queue full";
RpcServer server(&loop, serverFd);
server.setWorkerThreadNum(1);
server.setMaxWorkerQueue(1);

```

<br>

``` C++
// 设置消息收发的最大字节数(整个rpc消息的所有字节数,不包括LengthHeaderCodec 附加的 4 字节长度头),可配置长度[1, 64MiB]
// 如果配置得小到连这个失败响应也放不下，服务端会关闭连接。
// 未调用 setMaxMessageSize() 时，生效上限为 LengthHeaderCodec::kMaxMessageSize，即 64 MiB。
// 当收到大于指定长度的消息时断开连接
// 当发送大于指定长度的消息时返回"rpc message too large"
// start() 前、base EventLoop 线程调用
setMaxMessageSize(size_t);
```

<br>

``` C++
// 两者默认不限制，只能在 server.start() 前、RpcServer base EventLoop 线程调用；
// 达到任一上限时，服务端返回失败响应："server request limit reached"
// 只统计已登记 cancellation token 的异步请求；正常 reply、收到 RpcCancel、连接关闭都会释放名额。

// 全局活动异步请求上限；
setMaxActiveRequests(size_t)： 
// 单条 TCP 连接的活动异步请求上限；
setMaxActiveRequestsPerConnection(size_t)：
```

<br>

``` C++
// 注册异步回调函数
// 异步回调函数签名 void(const std::string &, RpcCancellationToken, RpcReply) 
// RpcReply = std::function<void(RpcResult)>;
// 异步 handler 可以稍后调用 reply；若一直不调用，客户端请求将继续等待，直到超时或连接关闭。客户端超时会通知服务端取消，但 handler 是否提前结束由 handler 自己根据 token.isCanceled() 决定
// 客户端发送取消帧后 server 只取消活动异步请求；handler 可用 token.isCanceled() 查询；服务端将异步请求标记为已取消后，后续调用 reply 不发送响应。
RpcServer server(&loop, serverFd);

bool registered = server.registerAsyncMethod(
      "service", "method",
      [](const std::string &payload, RpcCancellationToken cancellation, RpcServer::RpcReply reply) {
        ...
      });

server.start();
```

<br>

``` C++
RpcServer server(&loop, serverFd);

bool registered = server.registerMethod(
    "EchoService", "Echo",
    [](const std::string& payload) -> RpcServer::RpcResult {
      return {ResponseResult::kSuccess, payload, ""};
    });

server.start();
```

<br>

``` C++
RpcServer server(&loop, serverFd);
// registerMethod、registerAsyncMethod 和 unregisterMethod
// 只能在 server.start() 前、RpcServer 的 base EventLoop 线程调用。

bool unregistered =
    server.unregisterMethod("EchoService", "Echo");
```

<br>

### 客户端

``` C++
// 设置消息收发的最大字节数(整个rpc消息的所有字节数,不包括LengthHeaderCodec 附加的 4 字节长度头),可配置长度[1, 64MiB]。
// 未调用 setMaxMessageSize() 时，生效上限为 LengthHeaderCodec::kMaxMessageSize，即 64 MiB。
// 当收到大于指定长度的消息时客户端关闭连接，随后按请求的重发配置处理
// 当发送大于指定长度的消息时调用回调函数并附上报错信息："rpc message too large"
// 首次 connect() 前、所属 EventLoop 线程调用
setMaxMessageSize(size_t);
```

<br>

```txt
 重连后自动重发请求
 Tcp的自动重连和RpcClient的重连后自动重发请求是两独立的开关, 默认只有一次重发机会
 响应超时是指一个请求发送至server端后指定时常内没收到响应则超时，等待重连超时是指连接在非客户端主动断开后指定时常内没重新连接则超时。
 意外断线后取消原响应定时器，开始等待重连；重发时重新计算完整的响应超时。单次 TCP 重连失败不重置等待期限。
 响应超时："rpc request timeout"。等待重连超时："rpc reconnect timeout"。
 默认是tcp不自动重连且重连后不会重发Rpc请求，默认重连时常为1s。若需开启重连后自动重发要开启Tcp的自动重连，且在call时配置重连后自动重发并配置重连超时时间；
 还可使用全局配置setAllowReconnect和setDefaultReconnectTimeout但必须在首次 connect() 前、client 所属 EventLoop 线程中。
 可以使用call中特别指定当前call是否之开启重连后自动重发和重连超时时常。
 四／五参数 call() 使用默认重发配置, 带 bool 的版本覆盖本次开关。
 reconnectTimeout 省略或传 0 表示使用默认等待时长，不是无限等待。
 setDefaultResendLimit() 只能在首次 connect() 前、客户端所属 EventLoop 线程调用。
 实际重发或一轮等待重连超时，各消耗一次机会；等待超时后仍有机会就继续等待，耗尽才回调 "rpc reconnect timeout"
 提供了重发次数的配置，可以通过setDefaultResendLimit和在call中去指定对应的从发次数，按就近原则去获取重发次数。
 在call中指定resendLimit为0，将关闭重连后重发。
 重发可能会重复执行业务，服务端目前不提供去重保证
```

``` C++
    // 通过call配置自动重发，重连后重发次数为默认的1次，开启重连后重发，重发超时时间为1s
    RpcClient client(&loop, serverAddr);
    client.enableRetry();
    client.call(
        "Service", "Method", "Payload",
        [&](const RpcResponse &response) {
            ...
        },
    std::chrono::milliseconds(1000), true,
    std::chrono::milliseconds(1000));


    // 通过全局配置自动重发，重连后重发次数为默认的1次，开启重连后重发，重发超时时间为1s
    RpcClient client(&loop, serverAddr);
    client.enableRetry();
    client.setAllowReconnect(true);
    client.setDefaultReconnectTimeout(std::chrono::milliseconds(1000));
    client.call(
        "Service", "Method", "Payload",
        [&](const RpcResponse &response) {
            ...
        });

    // 通过call配置自动重发，默认次数为 5，本次指定 2，使用本次配置，开启重连后重发，重发超时时间为500ms
    RpcClient client(&loop, serverAddr);
    client.enableRetry();
    client.setDefaultResendLimit(5);
    client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
                ...
            },
            std::chrono::milliseconds(1000), true, std::chrono::milliseconds(500),
            2);

    // 通过call配置自动重发，默认次数为 5，本次指定 2，使用本次配置，开启重连后重发，重发超时时间沿用客户端默认值
    RpcClient client(&loop, serverAddr);
    client.enableRetry();
    client.setDefaultResendLimit(5);

    RpcCallOptions options;
    options.allowResend = true;
    options.resendLimit = 2;
    client.call("EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
                ...
            }, options);   



    // 通过call配置自动重发，设置重连后重发次数为0次，即关闭重发，即使全局配置重发次数为3，此次request也不开启重发
    RpcClient client(&loop, serverAddr);
    client.enableRetry();
    client.setDefaultResendLimit(3);
    client.call(
    "EchoService", "Echo", "hello world",
        [&](const RpcResponse &response) {
            ...
        },
        std::chrono::milliseconds(1000), true, std::chrono::milliseconds(500),
        0);
```

<br>

``` C++
RpcClient client(&loop, serverAddr);

client.call(
    "EchoService", "Echo", "hello",
    [](const RpcResponse& response) {
      // 根据 response.getResponseResult()
      // 读取 payload 或 errorMessage
    });
```

<br>

``` C++
// 这里两个call，主要的区别在最后是否带一个过期时间。四参数版本使用默认超时，默认值初始为 0ms，表示永不超时；但使用带过期时间的，则过期时间以调用时的为准，当调用时传递的为0则表示该调用永不过期。
client.call("EchoService", "Echo", "hello world",
    [&](const RpcResponse &response) {
    // 根据 response.getResponseResult()
    // 读取 payload 或 errorMessage
    });

client.call(
    "EchoService", "Echo", "request from thread 2",
    [&](const RpcResponse &response) {
    // 根据 response.getResponseResult()
    // 读取 payload 或 errorMessage
    },
    std::chrono::milliseconds(0));
```

<br>

``` C++
// 该方法用于取消对应请求的调用，若请求仍在等待响应，调用其原 callback 并返回失败响应；找不到 requestId 时静默返回。本地回调 "rpc request canceled"，请求在等待响应或等待重连时都可以取消；只有连接仍可用时才发送 Cancel 帧
uint64_t requestId = client.call("EchoService", "Echo", "hello world",
    [&](const RpcResponse &response) {
    // 根据 response.getResponseResult()
    // 读取 payload 或 errorMessage
    });
client.cancel(requestId);
```

``` C++
// 该方法用于设置默认过期时间,只能在客户端所属 EventLoop 调用,只能在首次 connect() 前调用。
client.setDefaultTimeout(std::chrono::milliseconds(100));
```

<br>

``` C++
// 该方法用于当前RpcClient同时最大的请求承载量，若当前Client的pending 请求数达到上限时直接返回调用错误的回调。默认不限制RpcClient的同时最大请求量。只能在客户端所属 EventLoop 调用,只能在首次 connect() 前调用。
client.setMaxPendingRequests(1);
```

<br>

### 通过 `CalculatorClient::add` 交互

``` C++
// 在连接成功后调用 add()；封装负责参数编码和结果解码，回调通过 status 判断成功或失败，成功读取 value，失败读取 errorMessage。
// CalculatorClient 通过 AddResult.errorCode 暴露错误码
// add() 返回的 requestId 可用于 cancel()。
CalculatorClient client(&loop, serverAddr);
client.add(1, 2, [&](AddResult result) {
    ...
});
```

<br/>

```C++
// 不传指定超时时间则沿用默认值、显式为 0 则不超时
CalculatorClient client(&loop, serverAddr);
client.setDefaultTimeout(std::chrono::milliseconds(500));

client.add(1, 2, [&](AddResult result) {
        ...
      });

client.add(1, 2, [&](AddResult result) {
    ...
    },
    std::chrono::milliseconds(0));
```

## 协议

每条 RPC 消息先由 LengthHeaderCodec 分帧；它只处理消息边界，
不理解 RPC 请求或响应的字段含义。

|Message|Fields|
|:---:|:---:|
|Request|version, messageType, requestId, service, method, payload|
|Response|version, messageType, requestId, result, errorCode, payload, errorMessage|
|Cancel|version, messageType,requestId|

- `version` 位于每条消息的开头，类型为 `uint16_t`；当前支持的版本为 `2`,暂不兼容v1报文。
- `requestId` 由客户端生成，服务端原样返回。
- 客户端用 `requestId` 匹配 pending callback。
- `service`、`method`、`payload`、`errorMessage` 都是二进制安全字符串。
- 成功响应的 `errorMessage` 为空；失败响应的 `payload` 为空。
- errorCode 类型为 uint16_t；成功时必须为 NO_ERROR，失败时必须为已定义的非零错误码。
- handler 返回的结果若包含非法错误码或状态与错误码不一致，服务端返回 kUnsuccess + INTERNAL_ERROR，错误文字为 "invalid handler result"。
- `messageType` 位于 version 之后，类型为 `uint8_t`，取值为 `Request`、`Response` 或 `Cancel`。
- 收到不支持的 `version`、非法 `messageType`，或不符合端点方向的消息类型时，连接会被关闭。
- 服务端只接收 `Request` 和 `Cancel`；客户端只接收 `Response`。

## 对象所属关系和线程

- `EventLoop` 由调用方拥有；
- `RpcServer` 拥有 `TcpServer` 和服务端 codec；
- `RpcClient` 拥有 `TcpClient` 和客户端 codec；
- 同步 handler 始终在连接所属 IO loop 执行；异步 handler 根据是否配置业务线程池选择执行线程；
- client 的连接回调和 RPC callback 在 client EventLoop 执行；
- `call`支持跨线程调用，但对应的回调函数仍在 `RpcClient` 所在的 `EventLoop` 中执行。
- `cancel`支持跨线程调用，但对应的回调函数仍在 `RpcClient` 所在的 `EventLoop` 中执行。

## 请求生命周期

- `timeout == 0ms` 表示永不超时；
- 超时 callback 收到失败响应；
- 迟到响应会被忽略；
- 真正未知的 response requestId 会关闭连接；
- 意外断线时，允许重发且还有剩余机会的请求保留等待重连，其余请求收到 "connect closed"；主动 disconnect() 不保留请求等待重发。
- 仅调用 enableRetry() 只开启 TCP 自动重连；RPC 重发需要另外配置。
- 默认超时为永不超时
- 当超过pending上限后直接调用回调函数并提示请求失败
- 取消请求后直接调用回调函数并提示请求失败，若请求在取消前已发送给服务端在一定时间内客户端收到响应后直接忽略
- 异步方法的reply可以从任意线程调用，最终发送回连接所属的EventLoop
- 同一个reply只有第一次调用有效
- 异步方法和同步方法不能注册相同的service/method
- 请求超时时，客户端先发送 Cancel 帧；随后 callback 收到 "rpc request timeout" 失败响应。
- 服务端收到 Cancel 帧后，会将对应活动异步请求的 RpcCancellationToken 标记为已取消；之后的 reply 不发送响应。
- 调用 `stop()` 后，服务端拒绝已有连接上的新请求，并给已登记的请求留出回复时间。
- 已登记的请求全部结束后，服务端立即开始优雅关闭连接。宽限期到期时仍有请求未完成，服务端会取消未完成的异步请求，并强制关闭剩余连接。
- TCP 服务停止完成后，若配置了业务线程池，还需等所有 worker 退出。满足这些条件后，`RpcServer` 在 base loop 中调用 `stopCompleteCallback`。
- 注册异步方法时，可以用第四个参数单独指定大于零的超时时长。该值优先于服务端全局配置；省略时沿用全局配置。
- 服务端默认不限制异步请求的处理时间。可以在 start() 前调用 setAsyncHandlerTimeout() 设置大于零的超时时长。从服务端登记请求时开始计时，业务线程池中的排队时间也算在内。
- 异步请求超时时，服务端将 token 标记为已取消，向客户端返回 RpcErrorCode::REQUEST_TIMEOUT，并忽略该请求之后的 reply。如果此时正处于停止宽限期，且这是最后一个已登记请求，服务端会提前开始关闭连接。


## 当前限制

- 复杂序列化
- 服务发现
