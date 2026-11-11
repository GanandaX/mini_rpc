#include "CalculatorClient.h"
#include "CalculatorProtocol.h"
#include "CalculatorService.h"
#include "RpcClient.h"
#include "RpcServer.h"

#include <assert.h>
#include <future>
#include <string>

void calculator_protocol_round_trips_signed_values_test() {
  CalculatorProtocol calculatorProtocol(INT32_MIN, INT32_MAX);
  std::string msg = calculatorProtocol.requestEncode();

  assert(msg.length() == 8);

  calculatorProtocol.setA(-1);
  calculatorProtocol.setB(-1);

  std::string errorMessage;
  bool decodeRes = calculatorProtocol.requestDecode(msg, errorMessage);
  assert(decodeRes);
  assert(errorMessage.empty());
  assert(calculatorProtocol.getA() == INT32_MIN);
  assert(calculatorProtocol.getB() == INT32_MAX);

  calculatorProtocol.setResult(0);
  msg = calculatorProtocol.resultEncode();

  assert(msg.length() == 4);

  calculatorProtocol.setResult(11);
  decodeRes = calculatorProtocol.resultDecode(msg, errorMessage);
  assert(decodeRes);
  assert(errorMessage.empty());
  assert(calculatorProtocol.getResult() == 0);
}

void calculator_protocol_uses_big_endian_wire_format_test() {
  CalculatorProtocol calculatorProtocol(0x01020304, -2);
  std::string msg = calculatorProtocol.requestEncode();
  assert(msg.length() == 8);

  std::string compareVal;
  compareVal.push_back(0x01);
  compareVal.push_back(0x02);
  compareVal.push_back(0x03);
  compareVal.push_back(0x04);
  compareVal.push_back(0xFF);
  compareVal.push_back(0xFF);
  compareVal.push_back(0xFF);
  compareVal.push_back(0xFE);
  assert(msg == compareVal);

  std::string errorMessage;
  CalculatorProtocol decodeCalculatorProtocol;
  bool decodeRes =
      decodeCalculatorProtocol.requestDecode(compareVal, errorMessage);
  assert(decodeRes);
  assert(decodeCalculatorProtocol.getA() == 0x01020304);
  assert(decodeCalculatorProtocol.getB() == -2);
}

void calculator_protocol_rejects_invalid_lengths_without_changing_state_test() {
  CalculatorProtocol calculatorProtocol(INT32_MIN, INT32_MAX);
  std::string msg = calculatorProtocol.requestEncode();

  assert(msg.length() == 8);

  calculatorProtocol.setA(17);
  calculatorProtocol.setB(23);

  std::string decodeMessage = msg;
  decodeMessage.append("1");

  std::string errorMessage;
  bool decodeRes =
      calculatorProtocol.requestDecode(decodeMessage, errorMessage);
  assert(!decodeRes);
  assert(errorMessage == "invalid add request");
  assert(calculatorProtocol.getA() == 17);
  assert(calculatorProtocol.getB() == 23);

  decodeMessage = msg.substr(0, msg.length() - 1);
  decodeRes = calculatorProtocol.requestDecode(decodeMessage, errorMessage);
  assert(!decodeRes);
  assert(errorMessage == "invalid add request");
  assert(calculatorProtocol.getA() == 17);
  assert(calculatorProtocol.getB() == 23);

  calculatorProtocol.setResult(0);
  msg = calculatorProtocol.resultEncode();
  assert(msg.length() == 4);
  calculatorProtocol.setResult(11);

  decodeMessage = msg;
  decodeMessage.append("1");
  decodeRes = calculatorProtocol.resultDecode(decodeMessage, errorMessage);
  assert(!decodeRes);
  assert(errorMessage == "invalid result response");
  assert(calculatorProtocol.getResult() == 11);

  decodeMessage = msg.substr(0, msg.length() - 1);
  decodeRes = calculatorProtocol.resultDecode(decodeMessage, errorMessage);
  assert(!decodeRes);
  assert(errorMessage == "invalid result response");
  assert(calculatorProtocol.getResult() == 11);
}

void rpc_calculator_add_returns_correct_result_test() {
  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t socketLen = sizeof(sockaddr_in);
  assert(0 ==
         ::bind(serverFd,
                reinterpret_cast<const sockaddr *>(serverAddr.getSockAddr()),
                socketLen));

  assert(0 ==
         ::getsockname(serverFd,
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &socketLen));

  std::promise<void> testCompletePromise;
  auto testCompleteFuture = testCompletePromise.get_future();

  int connected = 0;
  int rpcRoundSuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);

  server.setStopCompleteCallback([&]() {
    ++serverStopped;
    testCompletePromise.set_value();
    loop.quit();
  });

  CalculatorService service;
  bool registered = service.registerCalculatorService(server);
  assert(registered);

  server.start();

  CalculatorClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      client.add(1, 2, [&](AddResult result) {
        assert(result.status == ResponseResult::kSuccess);
        assert(result.errorCode == RpcErrorCode::NO_ERROR);
        assert(result.value == 3);
        assert(result.errorMessage.empty());

        if (result.status == ResponseResult::kSuccess) {
          ++rpcRoundSuccess;
          std::cout << "1 + 2 = " << result.value << std::endl;
        } else if (result.status == ResponseResult::kUnsuccess) {
          std::cout << "error: " << result.errorMessage << std::endl;
        }
        client.disconnect();
      });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      testCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server destroy timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundSuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_calculator_add_returns_error_on_integer_overflow_test() {
  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t socketLen = sizeof(sockaddr_in);
  assert(0 ==
         ::bind(serverFd,
                reinterpret_cast<const sockaddr *>(serverAddr.getSockAddr()),
                socketLen));

  assert(0 ==
         ::getsockname(serverFd,
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &socketLen));

  std::promise<void> testCompletePromise;
  auto testCompleteFuture = testCompletePromise.get_future();

  int connected = 0;
  int positiveOverflow = 0;
  int negtiveOverflow = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    ++serverStopped;
    testCompletePromise.set_value();
    loop.quit();
  });

  CalculatorService service;
  bool registered = service.registerCalculatorService(server);
  assert(registered);

  server.start();

  CalculatorClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      client.add(INT32_MAX, INT32_MAX, [&](AddResult result) {
        assert(result.status == ResponseResult::kUnsuccess);
        assert(result.errorCode == RpcErrorCode::BUSINESS_ERROR);
        assert(result.errorMessage == "integer overflow");

        if (result.status == ResponseResult::kSuccess) {
          std::cout << INT32_MAX << " + " << INT32_MAX << " = " << result.value
                    << std::endl;
        } else if (result.status == ResponseResult::kUnsuccess) {
          ++positiveOverflow;
          std::cout << "error: " << result.errorMessage << std::endl;
        }
      });

      client.add(INT32_MIN, INT32_MIN, [&](AddResult result) {
        assert(result.status == ResponseResult::kUnsuccess);
        assert(result.errorMessage == "integer overflow");

        if (result.status == ResponseResult::kSuccess) {
          std::cout << INT32_MIN << " + " << INT32_MIN << " = " << result.value
                    << std::endl;
        } else if (result.status == ResponseResult::kUnsuccess) {
          ++negtiveOverflow;
          std::cout << "error: " << result.errorMessage << std::endl;
        }
        client.disconnect();
      });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });

  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      testCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server destroy timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(positiveOverflow == 1);
  assert(negtiveOverflow == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_calculator_add_returns_error_for_invalid_payload_test() {
  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t socketLen = sizeof(sockaddr_in);
  assert(0 ==
         ::bind(serverFd,
                reinterpret_cast<const sockaddr *>(serverAddr.getSockAddr()),
                socketLen));

  assert(0 ==
         ::getsockname(serverFd,
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &socketLen));

  std::promise<void> testCompletePromise;
  auto testCompleteFuture = testCompletePromise.get_future();

  int connected = 0;
  int shortPayloadRejected = 0;
  int longPayloadRejected = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  server.setStopCompleteCallback([&]() {
    ++serverStopped;
    testCompletePromise.set_value();
    loop.quit();
  });

  CalculatorService service;
  bool registered = service.registerCalculatorService(server);
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      CalculatorProtocol calculatorAdd(INT32_MAX, INT32_MAX);
      std::string msg = calculatorAdd.requestEncode();
      msg = msg.substr(0, msg.length() - 1);

      client.call("Calculator", "Add", msg, [&](const RpcResponse &response) {
        loop.assertInLoopThread();
        assert(response.getResponseResult() == ResponseResult::kUnsuccess);
        assert(response.getPayload().empty());
        assert(response.getErrorMessage() == "invalid add request");

        ++shortPayloadRejected;
      });

      calculatorAdd.setA(INT32_MIN);
      calculatorAdd.setB(INT32_MIN);
      msg = calculatorAdd.requestEncode();
      msg.append("1");

      client.call("Calculator", "Add", msg, [&](const RpcResponse &response) {
        loop.assertInLoopThread();
        assert(response.getResponseResult() == ResponseResult::kUnsuccess);
        assert(response.getPayload().empty());
        assert(response.getErrorMessage() == "invalid add request");

        ++longPayloadRejected;
        client.disconnect();
      });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      testCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server destroy timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(shortPayloadRejected == 1);
  assert(longPayloadRejected == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void calculator_client_reports_error_for_invalid_result_payload_test() {
  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t socketLen = sizeof(sockaddr_in);
  assert(0 ==
         ::bind(serverFd,
                reinterpret_cast<const sockaddr *>(serverAddr.getSockAddr()),
                socketLen));

  assert(0 ==
         ::getsockname(serverFd,
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &socketLen));

  std::promise<void> testCompletePromise;
  auto testCompleteFuture = testCompletePromise.get_future();

  int connected = 0;
  int rpcRoundUnsuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);

  server.setStopCompleteCallback([&]() {
    ++serverStopped;
    testCompletePromise.set_value();
    loop.quit();
  });
  bool registered = server.registerAsyncMethod(
      "Calculator", "Add",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        std::string errorMessage;
        CalculatorProtocol calculatorAdd;
        bool res = calculatorAdd.requestDecode(payload, errorMessage);

        if (!res || !errorMessage.empty()) {
          reply(RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                     "invalid add request"});
          return;
        }

        int64_t result =
            static_cast<int64_t>(calculatorAdd.getA()) + calculatorAdd.getB();
        if (result > INT32_MAX || result < INT32_MIN) {
          reply(RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                     "integer overflow"});
          return;
        }

        calculatorAdd.setResult(result);
        std::string msg = calculatorAdd.resultEncode();
        msg = msg.substr(0, msg.length() - 1);
        reply(RpcServer::RpcResult{ResponseResult::kSuccess, msg, ""});
      });
  assert(registered);

  server.start();

  CalculatorClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      client.add(1, 2, [&](AddResult result) {
        assert(result.status == ResponseResult::kUnsuccess);
        assert(result.errorCode == RpcErrorCode::INTERNAL_ERROR);
        assert(result.value == 0);
        assert(result.errorMessage == "invalid result response");

        if (result.status == ResponseResult::kSuccess) {
          std::cout << "1 + 2 = " << result.value << std::endl;
        } else if (result.status == ResponseResult::kUnsuccess) {
          ++rpcRoundUnsuccess;
          std::cout << "error: " << result.errorMessage << std::endl;
        }
        client.disconnect();
      });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      testCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server destroy timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundUnsuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void calculator_client_uses_default_timeout_and_allows_zero_override_test() {

  std::cout << "-------------------" << std::endl;

  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t socketLen = sizeof(sockaddr_in);
  assert(0 ==
         ::bind(serverFd,
                reinterpret_cast<const sockaddr *>(serverAddr.getSockAddr()),
                socketLen));

  assert(0 ==
         ::getsockname(serverFd,
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &socketLen));

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int serverMessageReceived = 0;
  int timeoutRequest = 0;
  int successRequest = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback([&](const TcpConnectionPtr &conn,
                               const std::string &payload) {
    ++serverMessageReceived;

    RpcMessageType messageType;
    bool res = decodeHead(payload, messageType);
    assert(res);
    if (messageType == RpcMessageType::kCancel) {
      return;
    }

    if (messageType == RpcMessageType::kRequest) {
      RpcRequest request;
      std::string errorMessage;
      res = request.decode(payload, errorMessage);
      if (!res || !errorMessage.empty()) {
        std::cerr << __LINE__ << ": server decode error" << std::endl;
        std::abort();
        return;
      }

      RpcResponse response;
      CalculatorProtocol calculatorAdd;
      errorMessage.clear();
      bool res =
          calculatorAdd.requestDecode(request.getPayload(), errorMessage);

      if (!res || !errorMessage.empty()) {
        RpcResponse resp(request.getRequestId(), ResponseResult::kUnsuccess, "",
                         "invalid add request");
        response = resp;
      } else {
        int64_t result =
            static_cast<int64_t>(calculatorAdd.getA()) + calculatorAdd.getB();

        if (result > INT32_MAX || result < INT32_MIN) {
          RpcResponse resp(request.getRequestId(), ResponseResult::kUnsuccess,
                           "", "integer overflow");
          response = resp;
        } else {
          calculatorAdd.setResult(result);
          RpcResponse resp(request.getRequestId(), ResponseResult::kSuccess,
                           calculatorAdd.resultEncode(), "");
          response = resp;
        }
      }

      std::string outputMessage;
      errorMessage.clear();
      res = response.encode(outputMessage, errorMessage);
      if (!res || !errorMessage.empty()) {
        std::cerr << __LINE__ << ": server encode error" << std::endl;
        std::cerr << errorMessage << std::endl;
        std::abort();
        return;
      }

      if (conn->connected()) {
        loop.runAfter(
            std::chrono::milliseconds(1000), [&codec, conn, outputMessage]() {
              codec.send(conn, outputMessage.data(), outputMessage.length());
            });
      }
    } else {
      std::cerr << "invalid message" << std::endl;
      std::abort();
    }

    return;
  });

  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  CalculatorClient client(&loop, serverAddr);
  client.setDefaultTimeout(std::chrono::milliseconds(500));
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      client.add(1, 2, [&](AddResult result) {
        assert(result.status == ResponseResult::kUnsuccess);
        assert(result.errorCode == RpcErrorCode::REQUEST_TIMEOUT);
        assert(result.errorMessage == "rpc request timeout");

        ++timeoutRequest;
        if (timeoutRequest && successRequest) {
          client.disconnect();
        }
      });

      RpcCallOptions options;
      options.timeout = std::chrono::milliseconds(0);

      client.add(
          1, 2,
          [&](AddResult result) {
            assert(result.status == ResponseResult::kSuccess);
            assert(result.errorCode == RpcErrorCode::NO_ERROR);
            assert(result.value == 3);

            ++successRequest;
            if (timeoutRequest && successRequest) {
              client.disconnect();
            }
          },
          options);
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });

  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server destroy timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverMessageReceived == 3);
  assert(timeoutRequest == 1);
  assert(successRequest == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

int main() {
  calculator_protocol_round_trips_signed_values_test();
  calculator_protocol_uses_big_endian_wire_format_test();
  calculator_protocol_rejects_invalid_lengths_without_changing_state_test();
  rpc_calculator_add_returns_correct_result_test();
  rpc_calculator_add_returns_error_on_integer_overflow_test();
  rpc_calculator_add_returns_error_for_invalid_payload_test();
  calculator_client_reports_error_for_invalid_result_payload_test();
  calculator_client_uses_default_timeout_and_allows_zero_override_test();
}