#include "RpcServer.h"
#include <assert.h>
#include <atomic>
#include <map>
#include <mutex>
#include <optional>

bool RpcCancellationToken::isCanceled() const { return canceled_->load(); }

RpcCancellationToken::RpcCancellationToken(
    std::shared_ptr<std::atomic<bool>> canceled) {
  canceled_ = std::move(canceled);
}

enum class RequestKind { kSync, kAsync };

enum class RegistrationStatus {
  kRegistered,
  kNotAccepting,
  kDuplicateRequestId,
  kGlobalLimitReached,
  kConnectionLimitReached
};

enum class ExpirationResult {
  // 记录已移除，或同一 ID 已属于另一次请求
  kIgnored,
  // 本次请求成功超时
  kExpired,
  // 本次请求成功超时，且停止中的登记表刚好清空
  kExpiredAndDrained
};

// 一次登记对应一个对象。
// 同步请求的 cancellation 为 std::nullopt。
struct RequestRecord {
  RequestKind kind;
  std::optional<RpcCancellationToken> cancellation;
};

// 通过这个指针不能修改记录中的字段。
// token 的取消状态仍由 registry 通过其内部原子变量修改。
using RequestRecordPtr = std::shared_ptr<const RequestRecord>;

struct RegistrationResult {
  RegistrationStatus status;
  RequestRecordPtr record; // 登记失败时为空
};

class ServerRequestRegistry
    : public std::enable_shared_from_this<ServerRequestRegistry> {
  friend class RpcServer;

public:
  explicit ServerRequestRegistry(
      std::shared_ptr<std::function<void()>> drainedFunc);

  RegistrationResult tryRegister(const TcpConnectionPtr &conn,
                                 uint64_t requestId, RequestKind kind);

  // 是否仍然是表中有效的那次登记。
  bool contains(const TcpConnectionPtr &conn, uint64_t requestId,
                const RequestRecordPtr &expected);

  // 返回 true 表示：
  // 本次完成使登记表在停止接收后变空，需要通知停止流程。
  // 它不是“删除成功”的返回值。
  bool finish(const TcpConnectionPtr &conn, uint64_t requestId,
              const RequestRecordPtr &expected);

  // 返回 true 表示：本次取消使登记表在停止接收后变空。
  // 不表示“是否取消成功”。
  bool cancel(const TcpConnectionPtr &conn, uint64_t requestId);

  // 停止接受新登记。
  // 返回 true 表示此时已经没有任何请求需要等待。
  bool stopAccepting();

  // 连接彻底断开后，移除该连接的全部请求。
  // 返回 true 表示本次清理使登记表在停止接收后变空。
  bool clearConnection(const TcpConnectionPtr &conn);

  // 停止期限到达后，取消所有剩余异步请求并清空全部记录。
  // 同时确保不再接受新登记。
  void cancelAll();

  ExpirationResult expire(const TcpConnectionPtr &conn, uint64_t requestId,
                          const RequestRecordPtr &expectedRecord);

  void setAsyncHandlerTimeout(std::chrono::milliseconds timeout);

  void setMaxMessageSize(size_t maxMessageSize);

private:
  struct RequestEntry {
    RequestRecordPtr record;
    std::optional<TimerId> timerId;
  };

  struct ConnectionRequests {
    std::unordered_map<uint64_t, RequestEntry> entrys;
    size_t activeAsyncRequests = 0;
  };

  std::mutex mutex_;
  bool acceptingRequests_ = true;

  std::optional<std::chrono::milliseconds> asyncTimeout;
  std::optional<size_t> maxMessageSize_;

  // 保留现有“活动异步请求上限”的含义。
  size_t activeAsyncRequests_ = 0;
  std::optional<size_t> maxActiveAsyncRequests_;
  std::optional<size_t> maxActiveAsyncRequestsPerConnection_;

  std::unordered_map<TcpConnectionPtr, ConnectionRequests>
      requestsByConnection_;

  std::shared_ptr<std::function<void()>> drainedFunc_;
};

namespace {
void sendFramedData(const TcpConnectionPtr &conn, uint64_t requestId,
                    ResponseResult responseResult, RpcErrorCode errorCode,
                    std::string payload, std::string errorMessage,
                    size_t maxMessageSize) {
  size_t outputMessageLen =
      RpcResponse::OutputMessageLen(payload.length(), errorMessage.length());
  if (outputMessageLen > maxMessageSize) {
    if (maxMessageSize <
        RpcResponse::OutputMessageLen(0, strlen("rpc message too large"))) {
      conn->forceClose();
      return;
    }

    RpcResponse response(requestId, ResponseResult::kUnsuccess,
                         RpcErrorCode::MESSAGE_TOO_LARGE, "",
                         "rpc message too large");
    std::string outputMessage;
    errorMessage.clear();
    bool res = response.encode(outputMessage, errorMessage);
    if (!res || !errorMessage.empty()) {
      conn->forceClose();
      return;
    }

    if (conn->connected()) {
      LengthHeaderCodec codec;
      codec.send(conn, outputMessage.data(), outputMessage.length());
    }
    return;
  }

  RpcResponse response(requestId, responseResult, errorCode, payload,
                       errorMessage);
  std::string outputMessage;
  errorMessage.clear();
  bool res = response.encode(outputMessage, errorMessage);
  if (!res || !errorMessage.empty()) {
    conn->forceClose();
    return;
  }

  if (conn->connected()) {
    LengthHeaderCodec codec;
    codec.send(conn, outputMessage.data(), outputMessage.length());
  }
}

bool completeRequestInLoop(
    const std::shared_ptr<ServerRequestRegistry> &serverRequestRegistry,
    const RequestRecordPtr &record, const TcpConnectionPtr &conn,
    uint64_t requestId, ResponseResult responseResult, RpcErrorCode errorCode,
    std::string payload, std::string errorMessage, size_t maxMessageSize) {
  assert(conn->loop()->isInLoopThread());

  bool contain = serverRequestRegistry->contains(conn, requestId, record);
  if (!contain) {
    return false;
  }

  sendFramedData(conn, requestId, responseResult, errorCode, std::move(payload),
                 std::move(errorMessage), maxMessageSize);

  return serverRequestRegistry->finish(conn, requestId, record);
}
} // namespace

ServerRequestRegistry::ServerRequestRegistry(
    std::shared_ptr<std::function<void()>> drainedFunc)
    : drainedFunc_(drainedFunc) {}

RegistrationResult
ServerRequestRegistry::tryRegister(const TcpConnectionPtr &conn,
                                   uint64_t requestId, RequestKind kind) {
  assert(conn);
  assert(kind == RequestKind::kSync || kind == RequestKind::kAsync);

  std::lock_guard<std::mutex> lock(mutex_);

  auto connIt = requestsByConnection_.find(conn);

  // 优先识别尚未结束的重复请求。
  if (connIt != requestsByConnection_.end() &&
      connIt->second.entrys.find(requestId) != connIt->second.entrys.end()) {
    return {RegistrationStatus::kDuplicateRequestId, nullptr};
  }

  if (!acceptingRequests_) {
    return {RegistrationStatus::kNotAccepting, nullptr};
  }

  // 现有上限只约束异步请求。
  if (kind == RequestKind::kAsync) {
    if (maxActiveAsyncRequests_.has_value() &&
        activeAsyncRequests_ >= *maxActiveAsyncRequests_) {
      return {RegistrationStatus::kGlobalLimitReached, nullptr};
    }

    const size_t connectionAsyncRequests =
        connIt == requestsByConnection_.end()
            ? 0
            : connIt->second.activeAsyncRequests;

    if (maxActiveAsyncRequestsPerConnection_.has_value() &&
        connectionAsyncRequests >= *maxActiveAsyncRequestsPerConnection_) {
      return {RegistrationStatus::kConnectionLimitReached, nullptr};
    }
  }

  std::optional<RpcCancellationToken> cancellation;
  std::optional<TimerId> timerId;

  if (kind == RequestKind::kAsync) {
    cancellation =
        RpcCancellationToken(std::make_shared<std::atomic<bool>>(false));
  }

  auto record = std::make_shared<const RequestRecord>(
      RequestRecord{kind, std::move(cancellation)});

  if (kind == RequestKind::kAsync && asyncTimeout.has_value()) {
    std::weak_ptr<TcpConnection> weakConnPtr = conn;
    std::weak_ptr<ServerRequestRegistry> weakServerRegistryPtr =
        shared_from_this();
    std::weak_ptr<const RequestRecord> weakRecordPtr = record;

    timerId = conn->loop()->runAfter(
        asyncTimeout.value(),
        [weakConnPtr, weakServerRegistryPtr, weakRecordPtr, requestId,
         maxMessageSize = maxMessageSize_, sendResponse = sendFramedData,
         drainedFunc = drainedFunc_]() {
          auto conn = weakConnPtr.lock();
          auto serverRegistry = weakServerRegistryPtr.lock();
          auto record = weakRecordPtr.lock();

          if (!conn || !serverRegistry || !record) {
            return;
          }

          ExpirationResult result =
              serverRegistry->expire(conn, requestId, record);
          if (result == ExpirationResult::kIgnored) {
            return;
          }

          sendResponse(
              conn, requestId, ResponseResult::kUnsuccess,
              RpcErrorCode::REQUEST_TIMEOUT, "", "handler running timeout",
              maxMessageSize.value_or(LengthHeaderCodec::kMaxMessageSize));

          if (result == ExpirationResult::kExpiredAndDrained) {
            (*drainedFunc)();
          }
        });
  }

  if (connIt == requestsByConnection_.end()) {
    // 先在局部准备好记录，再把连接项放入总表。
    ConnectionRequests connectionRequests;
    RequestEntry entry{record, timerId};
    connectionRequests.entrys.emplace(requestId, RequestEntry{record, timerId});

    connIt = requestsByConnection_.emplace(conn, std::move(connectionRequests))
                 .first;
  } else {
    connIt->second.entrys.emplace(requestId, RequestEntry{record, timerId});
  }

  // 插入成功后才更新计数。
  if (kind == RequestKind::kAsync) {
    ++connIt->second.activeAsyncRequests;
    ++activeAsyncRequests_;
  }

  return {RegistrationStatus::kRegistered, std::move(record)};
}

bool ServerRequestRegistry::contains(const TcpConnectionPtr &conn,
                                     uint64_t requestId,
                                     const RequestRecordPtr &expected) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto connIt = requestsByConnection_.find(conn);
  if (connIt == requestsByConnection_.end()) {
    return false;
  }

  const auto &records = connIt->second.entrys;
  auto requestIt = records.find(requestId);

  return requestIt != records.end() && requestIt->second.record == expected;
}

bool ServerRequestRegistry::finish(const TcpConnectionPtr &conn,
                                   uint64_t requestId,
                                   const RequestRecordPtr &expected) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto connIt = requestsByConnection_.find(conn);
  if (connIt == requestsByConnection_.end()) {
    return false;
  }

  auto &connectionRequests = connIt->second;
  auto requestIt = connectionRequests.entrys.find(requestId);

  if (requestIt == connectionRequests.entrys.end()) {
    return false;
  }

  // ID 相同还不够，必须是当初登记的同一份记录。
  if (requestIt->second.record != expected) {
    return false;
  }

  auto entry = requestIt->second;
  if (entry.record->kind == RequestKind::kAsync) {
    --connectionRequests.activeAsyncRequests;
    --activeAsyncRequests_;

    if (entry.timerId.has_value()) {
      conn->loop()->cancel(entry.timerId.value());
    }
  }

  connectionRequests.entrys.erase(requestIt);

  if (connectionRequests.entrys.empty()) {
    requestsByConnection_.erase(connIt);
  }

  return !acceptingRequests_ && requestsByConnection_.empty();
}

bool ServerRequestRegistry::cancel(const TcpConnectionPtr &conn,
                                   uint64_t requestId) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto connIt = requestsByConnection_.find(conn);
  if (connIt == requestsByConnection_.end()) {
    return false;
  }

  auto &connectionRequests = connIt->second;
  auto requestIt = connectionRequests.entrys.find(requestId);

  if (requestIt == connectionRequests.entrys.end()) {
    return false;
  }

  const auto &record = requestIt->second.record;

  // 当前只支持取消异步请求。
  if (record->kind != RequestKind::kAsync) {
    return false;
  }

  auto entry = requestIt->second;
  assert(entry.record->cancellation.has_value());

  // 先通知业务任务：该请求已经被取消。
  record->cancellation->canceled_->store(true);

  --connectionRequests.activeAsyncRequests;
  --activeAsyncRequests_;

  if (entry.timerId.has_value()) {
    conn->loop()->cancel(entry.timerId.value());
  }

  connectionRequests.entrys.erase(requestIt);

  if (connectionRequests.entrys.empty()) {
    requestsByConnection_.erase(connIt);
  }

  return !acceptingRequests_ && requestsByConnection_.empty();
}

bool ServerRequestRegistry::stopAccepting() {
  std::lock_guard<std::mutex> lock(mutex_);
  acceptingRequests_ = false;

  return requestsByConnection_.empty();
}

bool ServerRequestRegistry::clearConnection(const TcpConnectionPtr &conn) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto connIt = requestsByConnection_.find(conn);
  if (connIt == requestsByConnection_.end()) {
    return false;
  }

  const auto &connectionRequests = connIt->second;
  for (const auto &entry : connectionRequests.entrys) {
    const auto &record = entry.second.record;

    if (record->cancellation.has_value()) {
      record->cancellation->canceled_->store(true);
    }

    if (entry.second.timerId.has_value()) {
      conn->loop()->cancel(entry.second.timerId.value());
    }
  }

  assert(activeAsyncRequests_ >= connectionRequests.activeAsyncRequests);

  activeAsyncRequests_ -= connectionRequests.activeAsyncRequests;
  requestsByConnection_.erase(connIt);

  return !acceptingRequests_ && requestsByConnection_.empty();
}

void ServerRequestRegistry::cancelAll() {
  std::lock_guard<std::mutex> lock(mutex_);

  acceptingRequests_ = false;

  for (const auto &connectionEntry : requestsByConnection_) {
    const auto &connectionRequests = connectionEntry.second;
    for (const auto &entry : connectionRequests.entrys) {
      const auto &record = entry.second.record;

      if (record->cancellation.has_value()) {
        record->cancellation->canceled_->store(true);
      }

      if (entry.second.timerId.has_value()) {
        connectionEntry.first->loop()->cancel(entry.second.timerId.value());
      }
    }
  }

  requestsByConnection_.clear();
  activeAsyncRequests_ = 0;
}

ExpirationResult
ServerRequestRegistry::expire(const TcpConnectionPtr &conn, uint64_t requestId,
                              const RequestRecordPtr &expectedRecord) {
  std::lock_guard<std::mutex> locker(mutex_);

  auto connItr = requestsByConnection_.find(conn);
  if (connItr == requestsByConnection_.end()) {
    return ExpirationResult::kIgnored;
  }

  auto &entrys = connItr->second.entrys;
  auto recordItr = entrys.find(requestId);
  if (recordItr == entrys.end()) {
    return ExpirationResult::kIgnored;
  }

  auto record = recordItr->second.record;
  if (record->kind == RequestKind::kSync) {
    return ExpirationResult::kIgnored;
  }

  if (record != expectedRecord) {
    return ExpirationResult::kIgnored;
  }

  record->cancellation->canceled_->store(true);
  entrys.erase(recordItr);
  --connItr->second.activeAsyncRequests;
  --activeAsyncRequests_;

  if (entrys.empty()) {
    requestsByConnection_.erase(connItr);
  }

  if (!acceptingRequests_ && requestsByConnection_.empty()) {
    return ExpirationResult::kExpiredAndDrained;
  }

  return ExpirationResult::kExpired;
}

void ServerRequestRegistry::setAsyncHandlerTimeout(
    std::chrono::milliseconds timeout) {
  asyncTimeout = timeout;
}

void ServerRequestRegistry::setMaxMessageSize(size_t maxMessageSize) {
  maxMessageSize_ = maxMessageSize;
}

class ReplyDispatcher {
public:
  explicit ReplyDispatcher();

  bool tryQueue(const TcpConnectionPtr &conn, EventLoop::Functor functor);

  bool tryQueue(EventLoop *loop, EventLoop::Functor functor);

  void close();

private:
  std::mutex mutex_;
  bool running_;
};

ReplyDispatcher::ReplyDispatcher() : running_(true) {}

bool ReplyDispatcher::tryQueue(const TcpConnectionPtr &conn,
                               EventLoop::Functor functor) {
  std::lock_guard<std::mutex> locker(mutex_);

  if (!running_) {
    return false;
  }

  conn->loop()->queueInLoop(std::move(functor));
  return true;
}

bool ReplyDispatcher::tryQueue(EventLoop *loop, EventLoop::Functor functor) {
  std::lock_guard<std::mutex> locker(mutex_);

  if (!running_) {
    return false;
  }

  loop->queueInLoop(std::move(functor));
  return true;
}

void ReplyDispatcher::close() {
  std::lock_guard<std::mutex> locker(mutex_);
  running_ = false;
}

RpcServer::RpcServer(EventLoop *loop, int listenFd, int threadNum)
    : loop_(loop), status_(Status::kNotStarted), tcpServerStopped_(false),
      taskPoolStopped_(false) {
  assert(loop != nullptr);
  assert(listenFd >= 0);
  assert(threadNum >= 0);
  loop_->assertInLoopThread();

  maxMessageSize_ = std::make_shared<std::optional<size_t>>();
  server_ = std::make_unique<TcpServer>(loop, listenFd, threadNum);
  replyDispatcher_ = std::make_shared<ReplyDispatcher>();

  drainedFunctor_ =
      std::make_shared<std::function<void()>>([this] { onRequestsDrained(); });

  std::weak_ptr<ReplyDispatcher> weakReplyDispatcherPtr = replyDispatcher_;
  std::weak_ptr<std::function<void()>> drainedFunctorPtr = drainedFunctor_;
  std::shared_ptr<std::function<void()>> drainedFunc =
      std::make_shared<std::function<void()>>(
          [weakReplyDispatcherPtr, baseLoop = loop_, drainedFunctorPtr]() {
            auto replyDispatcher = weakReplyDispatcherPtr.lock();
            if (!replyDispatcher) {
              return;
            }

            replyDispatcher->tryQueue(baseLoop, [drainedFunctorPtr]() {
              auto drainedFunctor = drainedFunctorPtr.lock();
              if (!drainedFunctor) {
                return;
              }

              (*drainedFunctor)();
            });
          });

  serverRequestRegistry_ = std::make_shared<ServerRequestRegistry>(drainedFunc);

  lengthHeaderCodec_.setMessageCallback(std::bind(&RpcServer::onRpcMessage,
                                                  this, std::placeholders::_1,
                                                  std::placeholders::_2));
  server_->setMessageCallback(
      std::bind(&LengthHeaderCodec::onMessage, &lengthHeaderCodec_,
                std::placeholders::_1, std::placeholders::_2));

  server_->setConnectionCallback(
      [this, serverRequestRegistry =
                 serverRequestRegistry_](const TcpConnectionPtr &conn) {
        if (!conn->connected()) {
          bool isDrained = serverRequestRegistry->clearConnection(conn);
          if (isDrained) {
            loop_->queueInLoop([this]() { onRequestsDrained(); });
          }
        }
      });

  server_->setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server_->setStopCompleteCallback([this]() { onTcpServerStopped(); });
}

void RpcServer::start() {
  loop_->assertInLoopThread();
  if (status_ != Status::kNotStarted) {
    return;
  }

  if (taskPool_) {
    taskPool_->start();
  }

  status_ = Status::kRunning;
  server_->start();
}

void RpcServer::stop(std::chrono::milliseconds gracePeriod) {
  loop_->assertInLoopThread();

  if (status_ != Status::kRunning) {
    return;
  }
  status_ = Status::kDraining;

  stopDeadline_ = std::chrono::steady_clock::now() + gracePeriod;

  server_->stopAccepting();
  bool res = serverRequestRegistry_->stopAccepting();
  if (res || gracePeriod == std::chrono::milliseconds(0)) {
    beginClosing();
    return;
  }

  stopTaskId = loop_->runAfter(gracePeriod, [&]() {
    stopTaskId.reset();
    beginClosing();
  });
}

void RpcServer::setStopCompleteCallback(StopCompleteCallback cb) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);

  stopCompleteCallback_ = std::move(cb);
}

bool RpcServer::registerMethod(const std::string &service,
                               const std::string &method, RpcHandler handler) {

  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);

  if (service.empty() || method.empty() || !handler) {
    return false;
  }

  if (isRegistered(service, method)) {
    return false;
  }

  auto serviceItr = syncHandlers_.find(service);
  if (serviceItr == syncHandlers_.end()) {
    std::unordered_map<std::string, RpcHandler> methodMap;
    methodMap[method] = std::move(handler);
    syncHandlers_[service] = std::move(methodMap);
    return true;
  }

  auto handlerItr = serviceItr->second.find(method);
  if (handlerItr == serviceItr->second.end()) {
    (serviceItr->second)[method] = std::move(handler);
    return true;
  }

  return false;
}

bool RpcServer::registerAsyncMethod(const std::string &service,
                                    const std::string &method,
                                    RpcAsyncHandler asyncHandler) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);

  if (service.empty() || method.empty() || !asyncHandler) {
    return false;
  }

  if (isRegistered(service, method)) {
    return false;
  }

  auto serviceItr = asyncHandlers_.find(service);
  if (serviceItr == asyncHandlers_.end()) {
    std::unordered_map<std::string, RpcAsyncHandler> methodMap;
    methodMap[method] = std::move(asyncHandler);
    asyncHandlers_[service] = std::move(methodMap);
    return true;
  }

  auto handlerItr = serviceItr->second.find(method);
  if (handlerItr == serviceItr->second.end()) {
    (serviceItr->second)[method] = std::move(asyncHandler);
    return true;
  }

  return false;
}

bool RpcServer::unregisterMethod(const std::string &service,
                                 const std::string &method) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);

  if (service.empty() || method.empty()) {
    return false;
  }

  if (isRegisteredInSync(service, method)) {
    auto serviceItr = syncHandlers_.find(service);
    if (serviceItr == syncHandlers_.end()) {
      return false;
    }

    auto handlerItr = serviceItr->second.find(method);
    if (handlerItr == serviceItr->second.end()) {
      return false;
    }

    serviceItr->second.erase(method);

    if (serviceItr->second.empty()) {
      syncHandlers_.erase(serviceItr);
    }

    return true;
  }

  if (isRegisteredInAsync(service, method)) {
    auto serviceItr = asyncHandlers_.find(service);
    if (serviceItr == asyncHandlers_.end()) {
      return false;
    }

    auto handlerItr = serviceItr->second.find(method);
    if (handlerItr == serviceItr->second.end()) {
      return false;
    }

    serviceItr->second.erase(method);

    if (serviceItr->second.empty()) {
      asyncHandlers_.erase(serviceItr);
    }
    return true;
  }

  return false;
}

void RpcServer::setMaxActiveRequests(size_t maxActiveRequests) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);
  assert(maxActiveRequests > 0);

  serverRequestRegistry_->maxActiveAsyncRequests_ = maxActiveRequests;
}

void RpcServer::setMaxActiveRequestsPerConnection(size_t maxActiveRequests) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);
  assert(maxActiveRequests > 0);

  serverRequestRegistry_->maxActiveAsyncRequestsPerConnection_ =
      maxActiveRequests;
}

void RpcServer::setMaxMessageSize(size_t maxMessageSize) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);
  assert(maxMessageSize > 0);
  assert(maxMessageSize <= LengthHeaderCodec::kMaxMessageSize);

  *maxMessageSize_ = maxMessageSize;

  serverRequestRegistry_->setMaxMessageSize(maxMessageSize);
}

void RpcServer::setWorkerThreadNum(uint8_t workThreadNum) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);
  assert(workThreadNum > 0);
  assert(!taskPool_);

  taskPool_ = std::make_unique<TaskThreadPool>(workThreadNum);
}

void RpcServer::setMaxWorkerQueue(size_t maxWorkerQueue) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);
  assert(maxWorkerQueue > 0);
  assert(taskPool_);

  taskPool_->setMaxQueueTasks(maxWorkerQueue);
}

void RpcServer::setAsyncHandlerTimeout(std::chrono::milliseconds timeout) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);
  assert(timeout > std::chrono::milliseconds(0));

  serverRequestRegistry_->setAsyncHandlerTimeout(timeout);
}

void RpcServer::onRpcMessage(const TcpConnectionPtr &conn,
                             const std::string &msg) {
  if (msg.empty()) {
    conn->forceClose();
    return;
  }

  size_t effectiveMaxMessageSize =
      maxMessageSize_->value_or(LengthHeaderCodec::kMaxMessageSize);

  if (msg.length() > effectiveMaxMessageSize) {
    conn->forceClose();
    return;
  }

  RpcMessageType messageType;
  bool decodeRes = decodeHead(msg, messageType);
  if (!decodeRes) {
    conn->forceClose();
    return;
  }

  if (messageType == RpcMessageType::kCancel) {
    RpcCancel cancel;
    std::string errorMessage;

    bool res = cancel.decode(msg, errorMessage);
    if (!res || !errorMessage.empty()) {
      conn->forceClose();
      return;
    }

    cancelAsync(conn, cancel.getRequestId());
    return;
  }

  if (messageType != RpcMessageType::kRequest) {
    conn->forceClose();
    return;
  }

  RpcRequest request;
  std::string errorMessage;
  bool res = request.decode(msg, errorMessage);
  if (!res || !errorMessage.empty()) {
    conn->forceClose();
    return;
  }

  if (isRegisteredInSync(request.getService(), request.getMethod())) {
    invokeSyncHandler(conn, request);
    return;
  }

  if (isRegisteredInAsync(request.getService(), request.getMethod())) {
    invokeAsyncHandler(conn, request);
    return;
  }

  sendResponse(conn, request.getRequestId(), ResponseResult::kUnsuccess,
               RpcErrorCode::METHOD_NOT_FOUND, "",
               "service or method not exist");
  return;
}

void RpcServer::sendResponse(const TcpConnectionPtr &conn, uint64_t requestId,
                             ResponseResult responseResult,
                             RpcErrorCode errorCode, std::string payload,
                             std::string errorMessage) {
  size_t outputMessageLen =
      RpcResponse::OutputMessageLen(payload.length(), errorMessage.length());
  size_t effectiveMaxMessageSize =
      maxMessageSize_->value_or(LengthHeaderCodec::kMaxMessageSize);
  if (outputMessageLen > effectiveMaxMessageSize) {
    if (effectiveMaxMessageSize <
        RpcResponse::OutputMessageLen(0, strlen("rpc message too large"))) {
      conn->forceClose();
      return;
    }

    RpcResponse response(requestId, ResponseResult::kUnsuccess,
                         RpcErrorCode::MESSAGE_TOO_LARGE, "",
                         "rpc message too large");
    std::string outputMessage;
    errorMessage.clear();
    bool res = response.encode(outputMessage, errorMessage);
    if (!res || !errorMessage.empty()) {
      conn->forceClose();
      return;
    }

    if (conn->connected()) {
      lengthHeaderCodec_.send(conn, outputMessage.data(),
                              outputMessage.length());
    }
    return;
  }

  RpcResponse response(requestId, responseResult, errorCode, payload,
                       errorMessage);
  std::string outputMessage;
  errorMessage.clear();
  bool res = response.encode(outputMessage, errorMessage);
  if (!res || !errorMessage.empty()) {
    conn->forceClose();
    return;
  }

  if (conn->connected()) {
    lengthHeaderCodec_.send(conn, outputMessage.data(), outputMessage.length());
  }
}

bool RpcServer::isRegistered(const std::string &service,
                             const std::string &method) {
  return isRegisteredInSync(service, method) ||
         isRegisteredInAsync(service, method);
}

bool RpcServer::isRegisteredInSync(const std::string &service,
                                   const std::string &method) {
  auto syncServiceItr = syncHandlers_.find(service);

  if (syncServiceItr == syncHandlers_.end()) {
    return false;
  }

  auto handlerItr = syncServiceItr->second.find(method);
  if (handlerItr != syncServiceItr->second.end()) {
    return true;
  }
  return false;
}

bool RpcServer::isRegisteredInAsync(const std::string &service,
                                    const std::string &method) {
  auto asyncServiceItr = asyncHandlers_.find(service);
  if (asyncServiceItr == asyncHandlers_.end()) {
    return false;
  }

  auto handlerItr = asyncServiceItr->second.find(method);
  if (handlerItr != asyncServiceItr->second.end()) {
    return true;
  }
  return false;
}

void RpcServer::invokeSyncHandler(const TcpConnectionPtr &conn,
                                  RpcRequest request) {
  auto serviceItr = syncHandlers_.find(request.getService());
  auto handler = serviceItr->second.find(request.getMethod())->second;

  RegistrationResult result = serverRequestRegistry_->tryRegister(
      conn, request.getRequestId(), RequestKind::kSync);

  if (result.status != RegistrationStatus::kRegistered) {
    RpcErrorCode errorCode = RpcErrorCode::INTERNAL_ERROR;
    std::string errorMessage;
    if (result.status == RegistrationStatus::kNotAccepting) {
      errorMessage = "server stopping";
    } else if (result.status == RegistrationStatus::kDuplicateRequestId) {
      conn->forceClose();
      return;
    } else if (result.status == RegistrationStatus::kGlobalLimitReached) {
      errorCode = RpcErrorCode::RESOURCE_EXHAUSTED;
      errorMessage = "global limit reached";
    } else if (result.status == RegistrationStatus::kConnectionLimitReached) {
      errorCode = RpcErrorCode::RESOURCE_EXHAUSTED;
      errorMessage = "connection limit reached";
    }

    sendResponse(conn, request.getRequestId(), ResponseResult::kUnsuccess,
                 errorCode, "", errorMessage);

    return;
  }

  RpcResult rpcResult;
  try {
    rpcResult = handler(request.getPayload());
  } catch (...) {
    // sendResponse(conn, request.getRequestId(), ResponseResult::kUnsuccess,
    //              RpcErrorCode::INTERNAL_ERROR, "", "handler exception");

    bool isDrained = completeRequestInLoop(
        serverRequestRegistry_, result.record, conn, request.getRequestId(),
        ResponseResult::kUnsuccess, RpcErrorCode::INTERNAL_ERROR, "",
        "handler exception",
        maxMessageSize_->value_or(LengthHeaderCodec::kMaxMessageSize));

    if (isDrained) {
      loop_->queueInLoop([this]() { onRequestsDrained(); });
    }
    return;
  }

  if ((rpcResult.result == ResponseResult::kUnsuccess &&
       !rpcResult.payload.empty()) ||
      (rpcResult.result == ResponseResult::kSuccess &&
       !rpcResult.errorMessage.empty()) ||
      (rpcResult.result != ResponseResult::kSuccess &&
       rpcResult.result != ResponseResult::kUnsuccess) ||
      rpcResult.errorCode >= RpcErrorCode::ERROR_CODE_CNT ||
      (rpcResult.result == ResponseResult::kSuccess &&
       rpcResult.errorCode != RpcErrorCode::NO_ERROR) ||
      (rpcResult.result == ResponseResult::kUnsuccess &&
       rpcResult.errorCode == RpcErrorCode::NO_ERROR)) {
    // sendResponse(conn, request.getRequestId(), ResponseResult::kUnsuccess,
    //              RpcErrorCode::INTERNAL_ERROR, "", "invalid handler result");

    bool isDrained = completeRequestInLoop(
        serverRequestRegistry_, result.record, conn, request.getRequestId(),
        ResponseResult::kUnsuccess, RpcErrorCode::INTERNAL_ERROR, "",
        "invalid handler result",
        maxMessageSize_->value_or(LengthHeaderCodec::kMaxMessageSize));

    if (isDrained) {
      loop_->queueInLoop([this]() { onRequestsDrained(); });
    }
    return;
  }

  bool isDrained = completeRequestInLoop(
      serverRequestRegistry_, result.record, conn, request.getRequestId(),
      rpcResult.result, rpcResult.errorCode, std::move(rpcResult.payload),
      std::move(rpcResult.errorMessage),
      maxMessageSize_->value_or(LengthHeaderCodec::kMaxMessageSize));

  if (isDrained) {
    loop_->queueInLoop([this]() { onRequestsDrained(); });
  }
}

void RpcServer::invokeAsyncHandler(const TcpConnectionPtr &conn,
                                   RpcRequest request) {
  auto serviceItr = asyncHandlers_.find(request.getService());
  auto handler = serviceItr->second.find(request.getMethod())->second;

  std::weak_ptr<TcpConnection> connWeakPtr = conn;
  uint64_t requestId = request.getRequestId();

  RegistrationResult result = serverRequestRegistry_->tryRegister(
      conn, request.getRequestId(), RequestKind::kAsync);

  if (result.status != RegistrationStatus::kRegistered) {
    RpcErrorCode errorCode = RpcErrorCode::INTERNAL_ERROR;
    std::string errorMessage;
    if (result.status == RegistrationStatus::kNotAccepting) {
      errorMessage = "server stopping";
    } else if (result.status == RegistrationStatus::kDuplicateRequestId) {
      conn->forceClose();
      return;
    } else if (result.status == RegistrationStatus::kGlobalLimitReached) {
      errorCode = RpcErrorCode::RESOURCE_EXHAUSTED;
      errorMessage = "server request limit reached";
    } else if (result.status == RegistrationStatus::kConnectionLimitReached) {
      errorCode = RpcErrorCode::RESOURCE_EXHAUSTED;
      errorMessage = "server request limit reached";
    }

    sendResponse(conn, request.getRequestId(), ResponseResult::kUnsuccess,
                 errorCode, "", errorMessage);
    return;
  }

  std::weak_ptr<ServerRequestRegistry> weakServerRequestRegistryPtr =
      serverRequestRegistry_;
  std::weak_ptr<ReplyDispatcher> weakReplyDispatcherPtr = replyDispatcher_;
  std::weak_ptr<std::optional<size_t>> weakMaxMessageSizePtr = maxMessageSize_;
  std::weak_ptr<std::function<void()>> drainedFunctorPtr = drainedFunctor_;

  std::shared_ptr<std::atomic<bool>> repliedFlag =
      std::make_shared<std::atomic<bool>>(false);
  std::function<void(RpcResult)> reply =
      [baseLoop = loop_, weakServerRequestRegistryPtr, record = result.record,
       connWeakPtr, requestId, repliedFlag, weakMaxMessageSizePtr,
       weakReplyDispatcherPtr, drainedFunctorPtr](RpcResult rpcResult) -> void {
    auto replyDispatcher = weakReplyDispatcherPtr.lock();
    if (!replyDispatcher) {
      return;
    }

    if (repliedFlag->exchange(true)) {
      return;
    }

    auto conn = connWeakPtr.lock();
    if (!conn) {
      return;
    }

    if ((rpcResult.result == ResponseResult::kUnsuccess &&
         !rpcResult.payload.empty()) ||
        (rpcResult.result == ResponseResult::kSuccess &&
         !rpcResult.errorMessage.empty()) ||
        (rpcResult.result != ResponseResult::kSuccess &&
         rpcResult.result != ResponseResult::kUnsuccess) ||
        (rpcResult.result == ResponseResult::kSuccess &&
         rpcResult.errorCode != RpcErrorCode::NO_ERROR) ||
        (rpcResult.result == ResponseResult::kUnsuccess &&
         rpcResult.errorCode == RpcErrorCode::NO_ERROR) ||
        (rpcResult.errorCode >= RpcErrorCode::ERROR_CODE_CNT)) {
      rpcResult = {ResponseResult::kUnsuccess, RpcErrorCode::INTERNAL_ERROR, "",
                   "invalid handler result"};
    }

    replyDispatcher->tryQueue(
        conn, [baseLoop, weakServerRequestRegistryPtr, record, conn, requestId,
               rpcResult = std::move(rpcResult), weakMaxMessageSizePtr,
               drainedFunctorPtr, weakReplyDispatcherPtr]() mutable {
          auto serverRequestRegistry = weakServerRequestRegistryPtr.lock();
          if (!serverRequestRegistry) {
            return;
          }

          auto maxMessageSize = weakMaxMessageSizePtr.lock();
          if (!maxMessageSize) {
            return;
          }

          size_t effectiveMaxMessageSize =
              maxMessageSize->value_or(LengthHeaderCodec::kMaxMessageSize);

          bool isDrained = completeRequestInLoop(
              serverRequestRegistry, record, conn, requestId, rpcResult.result,
              rpcResult.errorCode, std::move(rpcResult.payload),
              std::move(rpcResult.errorMessage), effectiveMaxMessageSize);
          if (isDrained) {
            auto replyDispatcher = weakReplyDispatcherPtr.lock();
            if (!replyDispatcher) {
              return;
            }

            replyDispatcher->tryQueue(baseLoop, [drainedFunctorPtr]() {
              auto drainedFunctor = drainedFunctorPtr.lock();
              if (!drainedFunctor) {
                return;
              }

              (*drainedFunctor)();
            });
          }
        });
  };

  auto functor = [request, handler, reply,
                  token = result.record->cancellation.value()]() {
    try {
      if (token.isCanceled()) {
        return;
      }
      handler(request.getPayload(), token, reply);
    } catch (...) {
      reply(RpcServer::RpcResult{ResponseResult::kUnsuccess,
                                 RpcErrorCode::INTERNAL_ERROR, "",
                                 "handler exception"});
      return;
    }
  };

  if (taskPool_) {
    TaskSubmitResult res = taskPool_->addTask(functor);

    if (res == TaskSubmitResult::kAccepted) {
      return;
    }

    RpcErrorCode errorCode;
    std::string errorMessage;

    if (res == TaskSubmitResult::kNotRunning) {
      errorCode = RpcErrorCode::INTERNAL_ERROR;
      errorMessage = "task pool not running";
    } else if (res == TaskSubmitResult::kQueueFull) {
      errorCode = RpcErrorCode::RESOURCE_EXHAUSTED;
      errorMessage = "task queue full";
    } else if (res == TaskSubmitResult::kEmptyTask) {
      errorCode = RpcErrorCode::INTERNAL_ERROR;
      errorMessage = "empty task";
    }

    reply(RpcResult{ResponseResult::kUnsuccess, errorCode, "", errorMessage});
    return;
  }

  conn->loop()->runInLoop(functor);
}

void RpcServer::cancelAsync(const TcpConnectionPtr &conn, uint64_t requestId) {
  bool isDrained = serverRequestRegistry_->cancel(conn, requestId);

  if (isDrained) {
    loop_->queueInLoop([this]() { onRequestsDrained(); });
  }
}

void RpcServer::onTcpServerStopped() {
  loop_->assertInLoopThread();
  tcpServerStopped_ = true;
  tryCompleteStop();
}

void RpcServer::onTaskPoolStopped() {
  loop_->assertInLoopThread();
  taskPoolStopped_ = true;
  tryCompleteStop();
}

void RpcServer::tryCompleteStop() {
  loop_->assertInLoopThread();
  if (status_ != Status::kStopping) {
    return;
  }

  if (tcpServerStopped_ && taskPoolStopped_) {
    if (taskPoolStopThread_.joinable()) {
      taskPoolStopThread_.join();
    }

    status_ = Status::kStopped;
    drainedFunctor_.reset();
    if (stopCompleteCallback_) {
      stopCompleteCallback_();
    }
  }
}

void RpcServer::onRequestsDrained() {
  assert(loop_->isInLoopThread());

  if (status_ == Status::kDraining) {
    beginClosing();
  }
}

void RpcServer::beginClosing() {
  assert(loop_->isInLoopThread());

  if (status_ != Status::kDraining) {
    return;
  }

  status_ = Status::kStopping;

  if (stopTaskId.has_value()) {
    loop_->cancel(stopTaskId.value());
    stopTaskId.reset();
  }

  replyDispatcher_->close();

  const auto now = std::chrono::steady_clock::now();
  std::chrono::milliseconds gracePeriod{0};
  if (now < stopDeadline_.value()) {
    gracePeriod = std::chrono::duration_cast<std::chrono::milliseconds>(
        stopDeadline_.value() - now);
  } else {
    serverRequestRegistry_->cancelAll();
  }

  if (taskPool_) {
    taskPoolStopThread_ = std::thread([&]() {
      taskPool_->stop();
      loop_->queueInLoop([&]() { onTaskPoolStopped(); });
    });
  } else {
    onTaskPoolStopped();
  }

  server_->stop(gracePeriod);
}