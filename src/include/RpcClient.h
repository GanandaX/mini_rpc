#ifndef MINI_RPC_RPC_CLIENT_H
#define MINI_RPC_RPC_CLIENT_H

#include "EventLoop.h"
#include "InetAddress.h"
#include "LengthHeaderCodec.h"
#include "RpcMessage.h"
#include "TcpClient.h"
#include "TcpConnection.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>

using RpcCallback = std::function<void(const RpcResponse &)>;

struct RpcCallOptions {
  std::optional<std::chrono::milliseconds> timeout;
  std::optional<bool> allowResend;
  std::optional<std::chrono::milliseconds> reconnectTimeout;
  std::optional<uint8_t> resendLimit;
};

class RpcClient {
public:
  explicit RpcClient(EventLoop *loop, const InetAddress &serverAddr);
  ~RpcClient();

  void setConnectionCallback(ConnectionCallback cb);
  void setConnectionErrorCallback(ConnectionErrorCallback cb);

  void connect();
  void disconnect();

  uint64_t call(const std::string &service, const std::string &method,
                const std::string &payload, RpcCallback cb);

  uint64_t call(const std::string &service, const std::string &method,
                const std::string &payload, RpcCallback cb,
                std::chrono::milliseconds timeout);

  /**
   * 当resendLimit赋值为0时表示关闭重连后重发
   */
  uint64_t call(
      const std::string &service, const std::string &method,
      const std::string &payload, RpcCallback cb,
      std::chrono::milliseconds timeout, bool allowReconnect,
      std::chrono::milliseconds reconnectTimeout = std::chrono::milliseconds(0),
      std::optional<uint8_t> resendLimit = std::nullopt);

  uint64_t call(const std::string &service, const std::string &method,
                const std::string &payload, RpcCallback cb,
                RpcCallOptions callOptions);

  void enableRetry();
  void disableRetry();

  void setMaxPendingRequests(size_t maxPendingRequests);

  /**
   * RpcClient 默认超时为 0ms，表示不启用超时；调用 setDefaultTimeout 可在首次
   * connect() 前修改它。
   */
  void setDefaultTimeout(std::chrono::milliseconds defaultTimeout);

  /**
   *  设置默认重连超时时间
   */
  void setDefaultReconnectTimeout(std::chrono::milliseconds defaultTimeout);

  /**
   * resendLimit默认值为1，允许设置为0表示关闭重连后重发。
   * 必须在首次 connect() 前，在所属 EventLoop 线程中设置
   */
  void setDefaultResendLimit(uint8_t resendLimit);

  void cancel(uint64_t requestId);

  void setMaxMessageSize(size_t maxMessageSize);

  void setAllowReconnect(bool allow);

private:
  static EventLoop *checkedLoop(EventLoop *loop);
  void onConnection(const TcpConnectionPtr &conn);
  void onRpcMessage(const TcpConnectionPtr &conn, const std::string &msg);
  void onConnectionError(int errorCode);
  void failAllPendingRequests(RpcErrorCode errorCode, std::string errorMessage);
  void failAllDisreconnectRequests(RpcErrorCode errorCode,
                                   std::string errorMessage);
  void onRequestTimeout(uint64_t requestId);
  void onReconnectTimeout(uint64_t requestId);

  void callInLoop(uint64_t requestId, const std::string &service,
                  const std::string &method, const std::string &payload,
                  RpcCallback cb, std::chrono::milliseconds timeout,
                  bool allowReconnect,
                  std::chrono::milliseconds reconnectTimeout,
                  std::optional<uint8_t> resendLimit);

  void clearExpiredIgnoredResponseRequests();

  void clearPendingRequests();
  void clearIgnoredResponseRequests();

  void cancelInLoop(uint64_t requestId);

  void sendCancel(uint64_t requestId);

  void checkReconnect();

private:
  enum class Status { kDisconnected, kConnecting, kDisconnecting, kConnected };

  enum class RequestPhase {
    kWaitingResponse, // 等待响应
    kWaitingReconnect // 等待重连
  };

  struct PendingRequest {
    std::string serviceName;
    std::string methodName;
    std::string payload;
    RpcCallback callback;
    std::chrono::milliseconds requestTimeout;

    bool allowReconnect;
    RequestPhase phase;
    uint8_t remainingResends;
    std::chrono::milliseconds reconnectTimeout;

    std::optional<TimerId> timerId;
  };

  bool hasConnectAttempted_;
  std::atomic<uint64_t> nextRequestId_ = 1;
  std::optional<size_t> maxPendingRequests_;
  std::chrono::milliseconds defaultTimeout_;
  std::optional<size_t> maxMessageSize_;
  EventLoop *loop_;
  Status status_;
  LengthHeaderCodec codec_;
  TcpClient tcpClient_;
  TcpConnectionPtr conn_;
  ConnectionCallback connectionCallback_;
  ConnectionErrorCallback connectionErrorCallback_;
  std::unordered_map<uint64_t, PendingRequest> pendingRequests_;
  std::optional<TimerId> clearIgnoredResponseRequestsTimerId_;
  std::unordered_map<uint64_t, std::chrono::steady_clock::time_point>
      ignoredResponseRequests_;

  std::chrono::milliseconds defaultReconnectTimeout_;
  uint8_t defaultResendLimit_;
  bool defaultAllowResend_;
  bool manualDisconnection_;
};

#endif