#ifndef MINI_RPC_RPC_SERVER_H
#define MINI_RPC_RPC_SERVER_H

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <unordered_map>
#include <utility>

#include "EventLoop.h"
#include "LengthHeaderCodec.h"
#include "RpcMessage.h"
#include "TaskThreadPool.h"
#include "TcpServer.h"

class ServerRequestRegistry;
class ReplyDispatcher;

class RpcCancellationToken {
public:
  bool isCanceled() const;

private:
  explicit RpcCancellationToken(std::shared_ptr<std::atomic<bool>> canceled);

  std::shared_ptr<std::atomic<bool>> canceled_;

  friend class RpcServer;
  friend class ServerRequestRegistry;
};

class RpcServer {
public:
  struct RpcResult {
    RpcResult()
        : RpcResult(ResponseResult::kUnsuccess, RpcErrorCode::INTERNAL_ERROR,
                    "", "") {}

    RpcResult(ResponseResult resultValue, std::string payloadValue,
              std::string errorMessageValue)
        : RpcResult(resultValue, RpcErrorCode::NO_ERROR,
                    std::move(payloadValue), std::move(errorMessageValue)) {
      if (resultValue == ResponseResult::kSuccess) {
        errorCode = RpcErrorCode::NO_ERROR;
      } else if (resultValue == ResponseResult::kUnsuccess) {
        errorCode = RpcErrorCode::BUSINESS_ERROR;
      }
    }

    RpcResult(ResponseResult resultValue, RpcErrorCode errorCodeValue,
              std::string payloadValue, std::string errorMessageValue)
        : result(resultValue), errorCode(errorCodeValue),
          payload(std::move(payloadValue)),
          errorMessage(std::move(errorMessageValue)) {}

    ResponseResult result;
    RpcErrorCode errorCode;
    std::string payload;
    std::string errorMessage;
  };

public:
  using StopCompleteCallback = std::function<void()>;
  using RpcHandler = std::function<RpcResult(const std::string &)>;
  using RpcReply = std::function<void(RpcResult)>;
  using RpcAsyncHandler =
      std::function<void(const std::string &payload,
                         RpcCancellationToken cancellation, RpcReply reply)>;

  explicit RpcServer(EventLoop *loop, int listenFd, int threadNum = 0);

  void start();
  void stop(std::chrono::milliseconds gracePeriod);

  void setStopCompleteCallback(StopCompleteCallback cb);

  bool registerMethod(const std::string &service, const std::string &method,
                      RpcHandler handler);

  bool registerAsyncMethod(const std::string &service,
                           const std::string &method,
                           RpcAsyncHandler asyncHandler);

  bool unregisterMethod(const std::string &service, const std::string &method);

  void setMaxActiveRequests(size_t maxActiveRequests);

  void setMaxActiveRequestsPerConnection(size_t maxActiveRequests);

  void setMaxMessageSize(size_t maxMessageSize);

  void setWorkerThreadNum(uint8_t workThreadNum);

  void setMaxWorkerQueue(size_t maxWorkerQueue);

private:
  void onRpcMessage(const TcpConnectionPtr &, const std::string &);

  void sendResponse(const TcpConnectionPtr &conn, uint64_t requestId,
                    ResponseResult responseResult, RpcErrorCode errorCode,
                    std::string payload, std::string errorMessage);

  bool isRegistered(const std::string &service, const std::string &method);

  bool isRegisteredInSync(const std::string &service,
                          const std::string &method);

  bool isRegisteredInAsync(const std::string &service,
                           const std::string &method);

  void invokeSyncHandler(const TcpConnectionPtr &conn, RpcRequest request);

  void invokeAsyncHandler(const TcpConnectionPtr &conn, RpcRequest request);

  void cancelAsync(const TcpConnectionPtr &conn, uint64_t requestId);

  void onTcpServerStopped();

  void onTaskPoolStopped();

  void tryCompleteStop();

  /**
   * 队列中无request
   */
  void onRequestsDrained();

  /**
   * 调用TaskPool和TcpServer关闭
   */
  void beginClosing();

private:
  enum class Status { kNotStarted, kRunning, kDraining, kStopping, kStopped };

  std::shared_ptr<std::optional<size_t>> maxMessageSize_;
  EventLoop *loop_;
  std::optional<std::chrono::steady_clock::time_point> stopDeadline_;
  std::optional<TimerId> stopTaskId;
  Status status_;
  bool tcpServerStopped_;
  bool taskPoolStopped_;

  std::unique_ptr<TaskThreadPool> taskPool_;
  std::thread taskPoolStopThread_;

  LengthHeaderCodec lengthHeaderCodec_;
  std::unique_ptr<TcpServer> server_;
  std::unordered_map<std::string, std::unordered_map<std::string, RpcHandler>>
      syncHandlers_;
  std::unordered_map<std::string,
                     std::unordered_map<std::string, RpcAsyncHandler>>
      asyncHandlers_;

  std::shared_ptr<ServerRequestRegistry> serverRequestRegistry_;
  std::shared_ptr<ReplyDispatcher> replyDispatcher_;

  std::shared_ptr<std::function<void()>> drainedFunctor_;

  StopCompleteCallback stopCompleteCallback_;
};

#endif