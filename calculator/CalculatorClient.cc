#include "CalculatorClient.h"
#include "CalculatorProtocol.h"

#include <cassert>

CalculatorClient::CalculatorClient(EventLoop *loop, InetAddress serverAddr)
    : client_(loop, serverAddr) {}

uint64_t CalculatorClient::add(int32_t a, int32_t b, AddCallback callback) {
  assert(callback);
  CalculatorProtocol protocol(a, b);
  std::string payload = protocol.requestEncode();

  return client_.call("Calculator", "Add", payload,
                      wrapAddCallback(std::move(callback)));
}

uint64_t CalculatorClient::add(int32_t a, int32_t b, AddCallback callback,
                               std::chrono::milliseconds timeout) {
  assert(callback);
  CalculatorProtocol protocol(a, b);
  std::string payload = protocol.requestEncode();

  return client_.call("Calculator", "Add", payload,
                      wrapAddCallback(std::move(callback)), timeout);
}

uint64_t CalculatorClient::add(int32_t a, int32_t b, AddCallback callback,
                               RpcCallOptions options) {
  assert(callback);
  CalculatorProtocol protocol(a, b);
  std::string payload = protocol.requestEncode();

  return client_.call("Calculator", "Add", payload,
                      wrapAddCallback(std::move(callback)), std::move(options));
}

uint64_t CalculatorClient::divmod(int32_t dividend, int32_t divisor,
                                  DivModCallback callback) {

  assert(callback);
  CalculatorProtocol protocol(dividend, divisor);
  std::string payload = protocol.requestEncode();

  return client_.call("Calculator", "DivMod", payload,
                      wrapDivModCallback(std::move(callback)));
}

uint64_t CalculatorClient::divmod(int32_t dividend, int32_t divisor,
                                  DivModCallback callback,
                                  std::chrono::milliseconds timeout) {
  assert(callback);
  CalculatorProtocol protocol(dividend, divisor);
  std::string payload = protocol.requestEncode();

  return client_.call("Calculator", "DivMod", payload,
                      wrapDivModCallback(std::move(callback)), timeout);
}

uint64_t CalculatorClient::divmod(int32_t dividend, int32_t divisor,
                                  DivModCallback callback,
                                  RpcCallOptions options) {
  assert(callback);
  CalculatorProtocol protocol(dividend, divisor);
  std::string payload = protocol.requestEncode();

  return client_.call("Calculator", "DivMod", payload,
                      wrapDivModCallback(std::move(callback)),
                      std::move(options));
}

void CalculatorClient::setDefaultTimeout(std::chrono::milliseconds timeout) {
  client_.setDefaultTimeout(timeout);
}

void CalculatorClient::cancel(uint64_t requestId) { client_.cancel(requestId); }

void CalculatorClient::connect() { client_.connect(); }

void CalculatorClient::setConnectionCallback(ConnectionCallback callback) {
  client_.setConnectionCallback(std::move(callback));
}

void CalculatorClient::setConnectionErrorCallback(
    ConnectionErrorCallback callback) {
  client_.setConnectionErrorCallback(std::move(callback));
}

void CalculatorClient::disconnect() { client_.disconnect(); }

void CalculatorClient::enableRetry() { client_.enableRetry(); }

void CalculatorClient::disableRetry() { client_.disableRetry(); }

RpcCallback CalculatorClient::wrapAddCallback(AddCallback callback) {
  return [addCallback = std::move(callback)](const RpcResponse &response) {
    if (response.getResponseResult() == ResponseResult::kSuccess) {
      CalculatorProtocol calculatorAdd;
      std::string errorMessage;
      bool result =
          calculatorAdd.resultDecode(response.getPayload(), errorMessage);
      if (!result || !errorMessage.empty()) {
        addCallback({ResponseResult::kUnsuccess, RpcErrorCode::INTERNAL_ERROR,
                     0, errorMessage});
        return;
      }

      addCallback({ResponseResult::kSuccess, RpcErrorCode::NO_ERROR,
                   calculatorAdd.getResult(), ""});
    } else {
      addCallback({ResponseResult::kUnsuccess, response.getErrorCode(), 0,
                   response.getErrorMessage()});
    }
  };
}

RpcCallback CalculatorClient::wrapDivModCallback(DivModCallback callback) {
  return [divCallback = std::move(callback)](const RpcResponse &response) {
    if (response.getResponseResult() == ResponseResult::kSuccess) {
      std::string errorMessage;
      int32_t quotient;
      int32_t remainder;

      bool result = CalculatorProtocol::decodeDivModResult(
          response.getPayload(), quotient, remainder, errorMessage);
      if (!result || !errorMessage.empty()) {
        divCallback({ResponseResult::kUnsuccess, RpcErrorCode::INTERNAL_ERROR,
                     0, 0, errorMessage});
        return;
      }

      divCallback({ResponseResult::kSuccess, RpcErrorCode::NO_ERROR, quotient,
                   remainder, ""});
    } else {
      divCallback({ResponseResult::kUnsuccess, response.getErrorCode(), 0, 0,
                   response.getErrorMessage()});
    }
  };
}