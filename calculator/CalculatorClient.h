#ifndef CALCULATOR_CLIENT_H
#define CALCULATOR_CLIENT_H

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>

#include "InetAddress.h"
#include "RpcClient.h"

struct AddResult {
  ResponseResult status;
  RpcErrorCode errorCode;
  int32_t value;
  std::string errorMessage;
};

class CalculatorClient {
public:
  using AddCallback = std::function<void(AddResult)>;

  explicit CalculatorClient(EventLoop *loop, InetAddress serverAddr);

  uint64_t add(int32_t a, int32_t b, AddCallback callback);

  uint64_t add(int32_t a, int32_t b, AddCallback callback,
               std::chrono::milliseconds timeout);

  uint64_t add(int32_t a, int32_t b, AddCallback callback,
               RpcCallOptions options);

  void setDefaultTimeout(std::chrono::milliseconds timeout);

  void cancel(uint64_t requestId);

  void connect();

  void setConnectionCallback(ConnectionCallback callback);

  void setConnectionErrorCallback(ConnectionErrorCallback callback);

  void disconnect();

  void enableRetry();

  void disableRetry();

private:
  static RpcCallback wrapAddCallback(AddCallback callback);

  RpcClient client_;
};

#endif