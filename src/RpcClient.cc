#include "RpcClient.h"

#include <assert.h>
#include <map>
#include <vector>

RpcClient::RpcClient(EventLoop *loop, const InetAddress &serverAddr)
    : hasConnectAttempted_(false), maxPendingRequests_(std::nullopt),
      defaultTimeout_(std::chrono::milliseconds(0)), loop_(checkedLoop(loop)),
      status_(Status::kDisconnected), tcpClient_(loop_, serverAddr),
      clearIgnoredResponseRequestsTimerId_(std::nullopt),
      defaultReconnectTimeout_(std::chrono::milliseconds(1000)),
      defaultResendLimit_(1), defaultAllowResend_(false),
      manualDisconnection_(false) {
  loop_->assertInLoopThread();

  codec_.setMessageCallback(std::bind(&RpcClient::onRpcMessage, this,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  tcpClient_.setConnectionCallback(
      std::bind(&RpcClient::onConnection, this, std::placeholders::_1));
  tcpClient_.setConnectionErrorCallback(
      std::bind(&RpcClient::onConnectionError, this, std::placeholders::_1));
  tcpClient_.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage,
                                          &codec_, std::placeholders::_1,
                                          std::placeholders::_2));
  tcpClient_.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  clearIgnoredResponseRequestsTimerId_ = loop_->runEvery(
      std::chrono::milliseconds(1000),
      std::bind(&RpcClient::clearExpiredIgnoredResponseRequests, this));
}

RpcClient::~RpcClient() {
  loop_->assertInLoopThread();

  clearPendingRequests();
  clearIgnoredResponseRequests();

  if (clearIgnoredResponseRequestsTimerId_.has_value()) {
    loop_->cancel(clearIgnoredResponseRequestsTimerId_.value());
  }
}

void RpcClient::setConnectionCallback(ConnectionCallback cb) {
  loop_->assertInLoopThread();
  assert(hasConnectAttempted_ == false);

  connectionCallback_ = std::move(cb);
}

void RpcClient::setConnectionErrorCallback(ConnectionErrorCallback cb) {
  loop_->assertInLoopThread();
  assert(hasConnectAttempted_ == false);

  connectionErrorCallback_ = std::move(cb);
}

void RpcClient::connect() {
  loop_->assertInLoopThread();
  if (status_ != Status::kDisconnected) {
    return;
  }

  status_ = Status::kConnecting;
  hasConnectAttempted_ = true;
  tcpClient_.connect();
}

void RpcClient::disconnect() {
  loop_->assertInLoopThread();

  manualDisconnection_ = true;
  tcpClient_.disconnect();

  if (status_ != Status::kConnected && status_ != Status::kConnecting) {
    failAllPendingRequests(RpcErrorCode::CONNECTION_CLOSED, "connect closed");
  } else if (status_ == Status::kConnected) {
    status_ = Status::kDisconnecting;
  } else if (status_ == Status::kConnecting) {
    status_ = Status::kDisconnected;
    failAllPendingRequests(RpcErrorCode::CONNECTION_CLOSED, "connect closed");
  }
}

uint64_t RpcClient::call(const std::string &service, const std::string &method,
                         const std::string &payload, RpcCallback cb) {
  return call(service, method, payload, cb, defaultTimeout_);
}

uint64_t RpcClient::call(const std::string &service, const std::string &method,
                         const std::string &payload, RpcCallback cb,
                         std::chrono::milliseconds timeout) {
  return call(service, method, payload, cb, timeout, defaultAllowResend_);
}

uint64_t RpcClient::call(const std::string &service, const std::string &method,
                         const std::string &payload, RpcCallback cb,
                         std::chrono::milliseconds timeout, bool allowResend,
                         std::chrono::milliseconds reconnectTimeout,
                         std::optional<uint8_t> resendLimit) {
  assert(cb != nullptr);
  assert(timeout >= std::chrono::milliseconds{0});
  assert(reconnectTimeout >= std::chrono::milliseconds(0));

  uint64_t requestId = nextRequestId_.fetch_add(1);

  if (loop_->isInLoopThread()) {
    callInLoop(requestId, service, method, payload, std::move(cb), timeout,
               allowResend, reconnectTimeout, resendLimit);
  } else {
    loop_->queueInLoop([this, requestId, service, method, payload,
                        callback = std::move(cb), timeout, allowResend,
                        reconnectTimeout, resendLimit]() mutable {
      callInLoop(requestId, service, method, payload, std::move(callback),
                 timeout, allowResend, reconnectTimeout, resendLimit);
    });
  }
  return requestId;
}

uint64_t RpcClient::call(const std::string &service, const std::string &method,
                         const std::string &payload, RpcCallback cb,
                         RpcCallOptions callOptions) {
  return call(service, method, payload, std::move(cb),
              callOptions.timeout.value_or(defaultTimeout_),
              callOptions.allowResend.value_or(defaultAllowResend_),
              callOptions.reconnectTimeout.value_or(defaultReconnectTimeout_),
              callOptions.resendLimit.value_or(defaultResendLimit_));
}

void RpcClient::enableRetry() {
  loop_->assertInLoopThread();
  tcpClient_.enableRetry();
}

void RpcClient::disableRetry() {
  loop_->assertInLoopThread();
  tcpClient_.disableRetry();
}

void RpcClient::setMaxPendingRequests(size_t maxPendingRequests) {
  assert(maxPendingRequests > 0);
  loop_->assertInLoopThread();
  assert(!hasConnectAttempted_);

  maxPendingRequests_ = maxPendingRequests;
}

void RpcClient::setDefaultTimeout(std::chrono::milliseconds defaultTimeout) {
  assert(defaultTimeout >= std::chrono::milliseconds(0));
  loop_->assertInLoopThread();
  assert(!hasConnectAttempted_);

  defaultTimeout_ = defaultTimeout;
}

void RpcClient::setDefaultReconnectTimeout(
    std::chrono::milliseconds defaultTimeout) {
  assert(defaultTimeout > std::chrono::milliseconds(0));
  loop_->assertInLoopThread();
  assert(!hasConnectAttempted_);

  defaultReconnectTimeout_ = defaultTimeout;
}

void RpcClient::setDefaultResendLimit(uint8_t resendLimit) {
  loop_->assertInLoopThread();
  assert(!hasConnectAttempted_);
  defaultResendLimit_ = resendLimit;
}

void RpcClient::cancel(uint64_t requestId) {
  if (loop_->isInLoopThread()) {
    cancelInLoop(requestId);
  } else {
    loop_->queueInLoop([this, requestId]() { cancelInLoop(requestId); });
  }
}

EventLoop *RpcClient::checkedLoop(EventLoop *loop) {
  assert(loop != nullptr);
  return loop;
}

void RpcClient::setMaxMessageSize(size_t maxMessageSize) {
  loop_->assertInLoopThread();
  assert(!hasConnectAttempted_);
  assert(maxMessageSize > 0);
  assert(maxMessageSize <= LengthHeaderCodec::kMaxMessageSize);

  maxMessageSize_ = maxMessageSize;
}

void RpcClient::setAllowReconnect(bool allow) {
  loop_->assertInLoopThread();
  assert(!hasConnectAttempted_);

  defaultAllowResend_ = allow;
}

void RpcClient::onConnection(const TcpConnectionPtr &conn) {
  loop_->assertInLoopThread();

  if (conn->connected()) {
    conn_ = conn;
    status_ = Status::kConnected;
    manualDisconnection_ = false;

    if (connectionCallback_) {
      connectionCallback_(conn);
    }

    if (conn == conn_) {
      checkReconnect();
    }
  } else {
    status_ = Status::kDisconnected;
    conn_.reset();

    if (manualDisconnection_) {
      failAllPendingRequests(RpcErrorCode::CONNECTION_CLOSED, "connect closed");
    } else {
      failAllDisreconnectRequests(RpcErrorCode::CONNECTION_CLOSED,
                                  "connect closed");
    }
    clearIgnoredResponseRequests();

    if (connectionCallback_) {
      connectionCallback_(conn);
    }
  }
}

void RpcClient::onRpcMessage(const TcpConnectionPtr &conn,
                             const std::string &msg) {
  loop_->assertInLoopThread();

  if (msg.empty()) {
    conn->forceClose();
    return;
  }

  size_t effectiveMaxMessageSize =
      maxMessageSize_.value_or(LengthHeaderCodec::kMaxMessageSize);

  if (msg.length() > effectiveMaxMessageSize) {
    conn->forceClose();
    return;
  }

  RpcMessageType messageType;
  if (!decodeHead(msg, messageType) ||
      messageType != RpcMessageType::kResponse) {
    conn->forceClose();
    return;
  }

  RpcResponse response;
  std::string errorMsg;
  bool res = response.decode(msg, errorMsg);
  if (!res || !errorMsg.empty()) {
    conn->forceClose();
    return;
  }

  auto findRes = pendingRequests_.find(response.getRequestId());
  if (findRes == pendingRequests_.end()) {
    auto timeoutFindRes =
        ignoredResponseRequests_.find(response.getRequestId());
    if (timeoutFindRes != ignoredResponseRequests_.end()) {
      ignoredResponseRequests_.erase(timeoutFindRes);
      return;
    }
    conn->forceClose();
    return;
  }

  if (findRes->second.timerId.has_value()) {
    loop_->cancel(findRes->second.timerId.value());
  }

  RpcCallback cb = std::move(findRes->second.callback);
  pendingRequests_.erase(findRes->first);
  cb(response);
}

void RpcClient::onConnectionError(int errorCode) {
  loop_->assertInLoopThread();

  conn_.reset();
  status_ = Status::kDisconnected;
  failAllDisreconnectRequests(RpcErrorCode::CONNECTION_UNAVAILABLE,
                              "connection error");
  clearIgnoredResponseRequests();
  if (connectionErrorCallback_) {
    connectionErrorCallback_(errorCode);
  }
}

void RpcClient::failAllPendingRequests(RpcErrorCode errorCode,
                                       std::string errorMessage) {
  loop_->assertInLoopThread();

  std::unordered_map<uint64_t, PendingRequest> temp;
  temp.swap(pendingRequests_);

  for (auto &item : temp) {
    if (item.second.timerId.has_value()) {
      loop_->cancel(item.second.timerId.value());
    }
  }

  for (auto &item : temp) {
    RpcResponse response(item.first, ResponseResult::kUnsuccess, errorCode, "",
                         errorMessage);
    (item.second.callback)(response);
  }
}

void RpcClient::failAllDisreconnectRequests(RpcErrorCode errorCode,
                                            std::string errorMessage) {
  loop_->assertInLoopThread();

  std::vector<uint64_t> waitingToChangePhase;
  std::vector<uint64_t> waitingToCallFailCallback;

  for (auto &item : pendingRequests_) {
    if (item.second.phase == RequestPhase::kWaitingResponse) {
      if (item.second.allowReconnect) {
        if (item.second.remainingResends) {
          waitingToChangePhase.push_back(item.first);
        } else {
          waitingToCallFailCallback.push_back(item.first);
        }
      } else if (!item.second.allowReconnect) {
        waitingToCallFailCallback.push_back(item.first);
      }
    }
  }

  for (auto item : waitingToChangePhase) {
    auto it = pendingRequests_.find(item);
    if (it == pendingRequests_.end()) {
      continue;
    }

    it->second.phase = RequestPhase::kWaitingReconnect;
    if (it->second.timerId.has_value()) {
      loop_->cancel(it->second.timerId.value());
      it->second.timerId.reset();
    }

    TimerId timerId = loop_->runAfter(
        it->second.reconnectTimeout,
        std::bind(&RpcClient::onReconnectTimeout, this, it->first));
    it->second.timerId = timerId;
  }

  std::map<uint64_t, RpcCallback> callbacks;
  for (auto item : waitingToCallFailCallback) {
    auto it = pendingRequests_.find(item);
    if (it == pendingRequests_.end()) {
      continue;
    }

    if (it->second.timerId.has_value()) {
      loop_->cancel(it->second.timerId.value());
      it->second.timerId.reset();
    }

    callbacks.insert({it->first, std::move(it->second.callback)});
    pendingRequests_.erase(it);
  }

  for (auto &item : callbacks) {
    RpcResponse response(item.first, ResponseResult::kUnsuccess, errorCode, "",
                         errorMessage);
    item.second(response);
  }
}

void RpcClient::onRequestTimeout(uint64_t requestId) {
  loop_->assertInLoopThread();

  auto findRes = pendingRequests_.find(requestId);
  if (findRes == pendingRequests_.end()) {
    return;
  }

  if (findRes->second.phase != RequestPhase::kWaitingResponse) {
    return;
  }

  PendingRequest pendingRequest = std::move(findRes->second);
  pendingRequests_.erase(findRes);

  auto oneMinuteLater =
      std::chrono::steady_clock::now() + std::chrono::minutes(1);
  ignoredResponseRequests_[requestId] = oneMinuteLater;

  sendCancel(requestId);

  RpcResponse response(requestId, ResponseResult::kUnsuccess,
                       RpcErrorCode::REQUEST_TIMEOUT, "",
                       "rpc request timeout");
  (pendingRequest.callback)(response);
}

void RpcClient::onReconnectTimeout(uint64_t requestId) {
  loop_->assertInLoopThread();

  auto findRes = pendingRequests_.find(requestId);
  if (findRes == pendingRequests_.end()) {
    return;
  }

  if (findRes->second.phase != RequestPhase::kWaitingReconnect) {
    return;
  }

  findRes->second.remainingResends--;

  if (!findRes->second.remainingResends) {
    PendingRequest pendingRequest = std::move(findRes->second);
    pendingRequests_.erase(findRes);

    RpcResponse response(requestId, ResponseResult::kUnsuccess,
                         RpcErrorCode::RECONNECT_TIMEOUT, "",
                         "rpc reconnect timeout");
    (pendingRequest.callback)(response);
    return;
  }

  TimerId timerId = loop_->runAfter(
      findRes->second.reconnectTimeout,
      std::bind(&RpcClient::onReconnectTimeout, this, requestId));
  pendingRequests_[requestId].timerId = timerId;
}

void RpcClient::callInLoop(uint64_t requestId, const std::string &service,
                           const std::string &method,
                           const std::string &payload, RpcCallback cb,
                           std::chrono::milliseconds timeout,
                           bool allowReconnect,
                           std::chrono::milliseconds reconnectTimeout,
                           std::optional<uint8_t> resendLimit) {
  loop_->assertInLoopThread();
  assert(timeout >= std::chrono::milliseconds{0});
  assert(cb != nullptr);

  RpcErrorCode errorCode = RpcErrorCode::NO_ERROR;
  std::string errorMessage;

  do {
    if ((status_ == Status::kConnected) && (conn_ != nullptr) &&
        (conn_->connected())) {
      size_t outputMessageLen = RpcRequest::OutputMessageLen(
          service.length(), method.length(), payload.length());
      if ((!maxMessageSize_.has_value() &&
           outputMessageLen > LengthHeaderCodec::kMaxMessageSize) ||
          (maxMessageSize_.has_value() &&
           outputMessageLen > maxMessageSize_.value())) {
        RpcResponse response(requestId, ResponseResult::kUnsuccess,
                             RpcErrorCode::MESSAGE_TOO_LARGE, "",
                             "rpc message too large");
        cb(response);
        return;
      }

      if (maxPendingRequests_.has_value() &&
          pendingRequests_.size() >= maxPendingRequests_) {
        RpcResponse response(requestId, ResponseResult::kUnsuccess,
                             RpcErrorCode::RESOURCE_EXHAUSTED, "",
                             "too many pending requests");
        cb(response);
        return;
      }

      RpcRequest request(requestId, service, method, payload);
      std::string output;
      std::string errorMsg;
      bool res = request.encode(output, errorMsg);
      if (!res || !errorMsg.empty()) {
        errorCode = RpcErrorCode::INTERNAL_ERROR;
        errorMessage = "encode error";
        break;
      }

      uint8_t remainingResends = resendLimit.value_or(defaultResendLimit_);
      if (allowReconnect && reconnectTimeout == std::chrono::milliseconds(0)) {
        reconnectTimeout = defaultReconnectTimeout_;
      }

      const bool canResend = allowReconnect && remainingResends > 0;

      pendingRequests_[requestId] = {canResend ? service : "",
                                     canResend ? method : "",
                                     canResend ? payload : "",
                                     std::move(cb),
                                     timeout,
                                     canResend,
                                     RequestPhase::kWaitingResponse,
                                     remainingResends,
                                     reconnectTimeout,
                                     std::nullopt};
      if (timeout > std::chrono::milliseconds(0)) {
        TimerId timerId = loop_->runAfter(
            timeout, std::bind(&RpcClient::onRequestTimeout, this, requestId));
        pendingRequests_[requestId].timerId = timerId;
      }

      codec_.send(conn_, output.data(), output.length());
      return;
    }
  } while (false);

  if (errorCode == RpcErrorCode::NO_ERROR) {
    errorCode = RpcErrorCode::CONNECTION_UNAVAILABLE;
    errorMessage = "connection not connected";
  }

  RpcResponse response(requestId, ResponseResult::kUnsuccess, errorCode, "",
                       errorMessage);
  cb(response);
}

void RpcClient::clearExpiredIgnoredResponseRequests() {
  loop_->assertInLoopThread();

  auto now = std::chrono::steady_clock::now();

  for (auto it = ignoredResponseRequests_.begin();
       it != ignoredResponseRequests_.end();) {
    if (it->second <= now) {
      it = ignoredResponseRequests_.erase(it);
    } else {
      ++it;
    }
  }
}

void RpcClient::clearPendingRequests() {
  loop_->assertInLoopThread();

  std::unordered_map<uint64_t, PendingRequest> temp;
  temp.swap(pendingRequests_);

  for (auto &item : temp) {
    if (item.second.timerId.has_value()) {
      loop_->cancel(item.second.timerId.value());
    }
  }
}

void RpcClient::clearIgnoredResponseRequests() {
  loop_->assertInLoopThread();
  ignoredResponseRequests_.clear();
}

void RpcClient::cancelInLoop(uint64_t requestId) {
  loop_->assertInLoopThread();

  auto findRes = pendingRequests_.find(requestId);
  if (findRes == pendingRequests_.end()) {
    return;
  }

  auto callback = std::move(findRes->second.callback);
  if (findRes->second.timerId.has_value()) {
    loop_->cancel(findRes->second.timerId.value());
  }
  pendingRequests_.erase(findRes);

  auto oneMinuteLater =
      std::chrono::steady_clock::now() + std::chrono::minutes(1);
  ignoredResponseRequests_[requestId] = oneMinuteLater;

  sendCancel(requestId);

  RpcResponse response{requestId, ResponseResult::kUnsuccess,
                       RpcErrorCode::REQUEST_CANCELED, "",
                       "rpc request canceled"};
  callback(response);
}

void RpcClient::sendCancel(uint64_t requestId) {
  loop_->assertInLoopThread();

  if (!conn_ || !conn_->connected()) {
    return;
  }

  RpcCancel cancel(requestId);
  std::string output;
  std::string errorMsg;
  bool res = cancel.encode(output, errorMsg);
  if (!res || !errorMsg.empty()) {
    return;
  }

  codec_.send(conn_, output.data(), output.length());
}

void RpcClient::checkReconnect() {
  loop_->assertInLoopThread();

  std::vector<uint64_t> waitingToResendRequest;
  for (auto &item : pendingRequests_) {
    if (item.second.phase != RequestPhase::kWaitingReconnect ||
        !item.second.allowReconnect || item.second.remainingResends == 0) {
      continue;
    }

    waitingToResendRequest.push_back(item.first);
  }

  auto conn = conn_;
  for (auto item : waitingToResendRequest) {
    if (!conn || conn != conn_ || status_ != Status::kConnected ||
        !conn->connected()) {
      return;
    }

    auto it = pendingRequests_.find(item);
    if (it == pendingRequests_.end() ||
        it->second.phase != RequestPhase::kWaitingReconnect) {
      continue;
    }

    if (it->second.timerId.has_value()) {
      loop_->cancel(it->second.timerId.value());
      it->second.timerId.reset();
    }

    --it->second.remainingResends;
    it->second.phase = RequestPhase::kWaitingResponse;

    RpcRequest request(it->first, it->second.serviceName, it->second.methodName,
                       it->second.payload);
    std::string output;
    std::string errorMsg;
    bool res = request.encode(output, errorMsg);
    if (!res || !errorMsg.empty()) {
      auto callback = std::move(it->second.callback);
      RpcResponse response(it->first, ResponseResult::kUnsuccess,
                           RpcErrorCode::INTERNAL_ERROR, "", errorMsg);
      pendingRequests_.erase(it->first);
      callback(response);
      continue;
    }

    if (it->second.requestTimeout > std::chrono::milliseconds(0)) {
      TimerId timerId = loop_->runAfter(
          it->second.requestTimeout,
          std::bind(&RpcClient::onRequestTimeout, this, it->first));
      it->second.timerId = timerId;
    }

    codec_.send(conn, output.data(), output.length());
  }
}