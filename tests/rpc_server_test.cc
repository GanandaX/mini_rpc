#include <assert.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <sys/socket.h>
#include <thread>
#include <vector>

#include "EventLoopThread.h"
#include "InetAddress.h"
#include "LengthHeaderCodec.h"
#include "RpcClient.h"
#include "RpcMessage.h"
#include "RpcServer.h"
#include "TcpClient.h"

void rpc_server_processes_echo_request_test() {
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
  int rpcRoundSuccess = 0;
  int serverStopped = 0;

  EventLoop loop;
  std::unique_ptr<RpcServer> rpcServer =
      std::make_unique<RpcServer>(&loop, serverFd);
  rpcServer->setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      loop.queueInLoop([&]() {
        rpcServer.reset();
        ++serverStopped;
        stopCompletePromise.set_value();
        loop.quit();
      });
    });
  });
  bool registered = rpcServer->registerMethod(
      "EchoService", "Echo", [](const std::string &payload) {
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  rpcServer->start();

  LengthHeaderCodec codec;
  TcpClient client(&loop, serverAddr);
  codec.setMessageCallback(
      [&](const TcpConnectionPtr &, const std::string &msg) {
        RpcResponse response;
        std::string errorMsg;
        bool res = response.decode(msg, errorMsg);
        if (!res || !errorMsg.empty()) {
          std::cerr << "response decode error" << std::endl;
          std::abort();
        }

        assert(response.getRequestId() == 1);
        assert(response.getResponseResult() == ResponseResult::kSuccess);
        assert(response.getPayload() == "hello");
        assert(response.getErrorMessage().empty());

        ++rpcRoundSuccess;
        client.disconnect();
      });

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      RpcRequest request(1, "EchoService", "Echo", "hello");
      std::string output;
      std::string errorMsg;
      bool res = request.encode(output, errorMsg);
      if (!res || !errorMsg.empty()) {
        assert(res && errorMsg.empty());
        std::abort();
      }

      codec.send(conn, output.data(), output.length());
    } else {
      rpcServer->stop(std::chrono::milliseconds(0));
    }
  });
  client.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });
  client.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  client.setConnectionErrorCallback([&](int) {
    std::cerr << "client connect error" << std::endl;
    std::abort();
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundSuccess == 1);
  assert(serverStopped == 1);
}

void rpc_client_calls_echo_service_test() {
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
  int rpcRoundSuccess = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [](const std::string &payload) {
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "hello world");

                    ++rpcRoundSuccess;
                    client.disconnect();
                  });
    } else {
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
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundSuccess == 1);
  assert(serverStopped == 1);
}

void rpc_client_receives_error_for_unknown_service_test() {
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
  int rpcErrorResponseReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "UnknownService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::METHOD_NOT_FOUND);
            assert(response.getPayload().empty());
            assert(!response.getErrorMessage().empty());

            ++rpcErrorResponseReceived;
            client.disconnect();
          });
    } else {
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
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcErrorResponseReceived == 1);
  assert(serverStopped == 1);
}

void rpc_client_reports_error_when_not_connected_test() {
  int callResponseCnt = 0;

  EventLoop loop;
  InetAddress serverAddr("127.0.0.1", 0);
  RpcClient client(&loop, serverAddr);

  client.call(
      "UnknownService", "Echo", "hello world",
      [&](const RpcResponse &response) {
        assert(response.getResponseResult() == ResponseResult::kUnsuccess);
        assert(response.getErrorCode() == RpcErrorCode::CONNECTION_UNAVAILABLE);
        assert(response.getPayload().empty());
        assert(!response.getErrorMessage().empty());
        ++callResponseCnt;
      });

  assert(callResponseCnt == 1);
}

void rpc_client_reports_error_when_connection_closes_with_pending_request_test() {
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
  int rpcErrorCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;
  bool rpcFailureReported = false;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &) {
        ++serverRequestReceived;
        conn->forceClose();
        return;
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(!response.getErrorMessage().empty());

                    ++rpcErrorCallbackCount;
                    rpcFailureReported = true;
                  });
    } else {
      assert(rpcFailureReported);
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
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 1);
  assert(rpcErrorCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_rejects_duplicate_method_registration_test() {
  EventLoop loop;
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(fd >= 0);

  RpcServer server(&loop, fd);
  bool res = server.registerMethod(
      "EchoService", "echo",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        return RpcServer::RpcResult{};
      });
  assert(res == true);

  res = server.registerMethod(
      "EchoService", "reverse",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        return RpcServer::RpcResult{};
      });
  assert(res == true);

  res = server.registerMethod(
      "EchoService", "echo",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        return RpcServer::RpcResult{};
      });
  assert(res == false);
}

void rpc_client_receives_error_for_invalid_handler_result_test() {
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
  int handlerInvoked = 0;
  int rpcErrorResponseReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "Return fail service", "return fail",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        ++handlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kUnsuccess,
                                    "payload must be empty for an error result",
                                    ""};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "Return fail service", "return fail", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "invalid handler result");

            ++rpcErrorResponseReceived;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(handlerInvoked == 1);
  assert(rpcErrorResponseReceived == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_matches_out_of_order_responses_by_request_id_test() {
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

  int serverRequestReceived = 0;
  int firstCallbackCount = 0;
  int secondCallbackCount = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  bool secondCallbackRan = false;

  std::vector<RpcRequest> requests;

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        if (serverRequestReceived == 1) {
          assert(request.getService() == "first service");
          assert(request.getMethod() == "first method");
        } else if (serverRequestReceived == 2) {
          assert(request.getService() == "second service");
          assert(request.getMethod() == "second method");
        }

        requests.emplace_back(request);
        if (requests.size() == 2) {
          for (int i = requests.size() - 1; i >= 0; --i) {
            RpcResponse response(
                requests.at(i).getRequestId(), ResponseResult::kSuccess,
                (i == 0) ? "response for first" : "response for second", "");
            std::string outputMessage;
            errorMessage.clear();
            bool res = response.encode(outputMessage, errorMessage);
            if (!res || !errorMessage.empty()) {
              conn->forceClose();
              return;
            }

            if (conn->connected()) {
              codec.send(conn, outputMessage.data(), outputMessage.length());
            }
          }
        }
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  server.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      client.call("first service", "first method", "first",
                  [&](const RpcResponse &response) {
                    ++firstCallbackCount;
                    assert(response.getRequestId() == 1);
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "response for first");
                    assert(response.getErrorMessage().empty());

                    assert(secondCallbackRan);
                    client.disconnect();
                  });
      client.call("second service", "second method", "second",
                  [&](const RpcResponse &response) {
                    ++secondCallbackCount;
                    assert(response.getRequestId() == 2);
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "response for second");
                    assert(response.getErrorMessage().empty());

                    secondCallbackRan = true;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(3000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverRequestReceived == 2);
  assert(firstCallbackCount == 1);
  assert(secondCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_closes_connection_for_unknown_response_request_id_test() {
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

  int serverRequestReceived = 0;
  int forgedResponseSent = 0;
  int pendingRequestFailed = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  bool pendingFailureReported = false;

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback([&](const TcpConnectionPtr &conn,
                               const std::string &msg) {
    ++serverRequestReceived;

    RpcRequest request;
    std::string errorMessage;
    bool res = request.decode(msg, errorMessage);
    if (!res || !errorMessage.empty()) {
      conn->forceClose();
      return;
    }

    RpcResponse response(request.getRequestId() + 1000,
                         ResponseResult::kSuccess, request.getPayload(), "");
    std::string outputMessage;
    errorMessage.clear();
    res = response.encode(outputMessage, errorMessage);
    if (!res || !errorMessage.empty()) {
      conn->forceClose();
      return;
    }

    if (conn->connected()) {
      ++forgedResponseSent;
      codec.send(conn, outputMessage.data(), outputMessage.length());
    }
  });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  server.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      client.call(
          "service", "method", "payload", [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(!response.getErrorMessage().empty());

            ++pendingRequestFailed;
            pendingFailureReported = true;
          });
    } else {
      ++clientDisconnected;
      assert(pendingFailureReported);
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(3000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverRequestReceived == 1);
  assert(forgedResponseSent == 1);
  assert(pendingRequestFailed == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_receives_business_error_from_registered_handler_test() {
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
  int handlerInvoked = 0;
  int rpcBusinessErrorReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "service", "method", [&](const std::string &payload) {
        ++handlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                    "business failed"};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "service", "method", "hello world", [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "business failed");

            ++rpcBusinessErrorReceived;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(handlerInvoked == 1);
  assert(rpcBusinessErrorReceived == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_reports_timeout_when_server_does_not_respond_test() {
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

  int serverRequestReceived = 0;
  int timeoutCallbackCount = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  bool timeoutReported = false;

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        assert(request.getService() == "service");
        assert(request.getMethod() == "method");
        assert(request.getPayload() == "payload");
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  server.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      client.call(
          "service", "method", "payload",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::REQUEST_TIMEOUT);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request timeout");

            ++timeoutCallbackCount;
            timeoutReported = true;
            client.disconnect();
          },
          std::chrono::milliseconds(100));
    } else {
      ++clientDisconnected;
      assert(timeoutReported);
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(3000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverRequestReceived == 2);
  assert(timeoutCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_does_not_timeout_when_timeout_is_zero_test() {
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

  int serverRequestReceived = 0;
  int connectionCloseCallbackCount = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  bool connectionCloseReported = false;

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        assert(request.getService() == "service");
        assert(request.getMethod() == "method");
        assert(request.getPayload() == "payload");
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  server.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      client.call(
          "service", "method", "payload",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "connect closed");

            ++connectionCloseCallbackCount;
            connectionCloseReported = true;
          },
          std::chrono::milliseconds(0));

      conn->loop()->runAfter(std::chrono::milliseconds(100), [&] {
        assert(connectionCloseCallbackCount == 0);
        client.disconnect();
      });
    } else {
      ++clientDisconnected;
      assert(connectionCloseReported);
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(3000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverRequestReceived == 1);
  assert(connectionCloseCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_executes_handler_on_io_loop_test() {
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
  std::atomic<int> handlerInvoked{0};
  std::atomic<bool> handlerRanOnIoThread{false};
  int rpcRoundSuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd, 1);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        assert(!loop.isInLoopThread());
        ++handlerInvoked;
        handlerRanOnIoThread.store(true);
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "hello world");
                    assert(response.getErrorMessage().empty());

                    ++rpcRoundSuccess;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(handlerInvoked.load() == 1);
  assert(handlerRanOnIoThread.load());
  assert(rpcRoundSuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_distributes_connections_across_io_loops_test() {
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

  std::mutex mutex;
  std::set<std::thread::id> threadIds;

  int clientConnected = 0;
  std::atomic<int> handlerInvoked{0};
  int client1RpcRoundSuccess = 0;
  int client2RpcRoundSuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd, 2);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        assert(!loop.isInLoopThread());
        ++handlerInvoked;
        {
          std::lock_guard<std::mutex> locker(mutex);
          threadIds.insert(std::this_thread::get_id());
        }
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  RpcClient client_1(&loop, serverAddr);
  client_1.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnected;
      client_1.call("EchoService", "Echo", "hello world",
                    [&](const RpcResponse &response) {
                      loop.assertInLoopThread();
                      assert(response.getResponseResult() ==
                             ResponseResult::kSuccess);
                      assert(response.getPayload() == "hello world");
                      assert(response.getErrorMessage().empty());

                      ++client1RpcRoundSuccess;
                      client_1.disconnect();
                    });
    } else {
      ++clientDisconnected;
      if (clientDisconnected == 2) {
        server.stop(std::chrono::milliseconds(0));
      }
    }
  });
  client_1.setConnectionErrorCallback([&](int) { std::abort(); });
  client_1.connect();

  RpcClient client_2(&loop, serverAddr);
  client_2.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnected;
      client_2.call("EchoService", "Echo", "hello world",
                    [&](const RpcResponse &response) {
                      loop.assertInLoopThread();
                      assert(response.getResponseResult() ==
                             ResponseResult::kSuccess);
                      assert(response.getPayload() == "hello world");
                      assert(response.getErrorMessage().empty());

                      ++client2RpcRoundSuccess;
                      client_2.disconnect();
                    });
    } else {
      ++clientDisconnected;
      if (clientDisconnected == 2) {
        server.stop(std::chrono::milliseconds(0));
      }
    }
  });
  client_2.setConnectionErrorCallback([&](int) { std::abort(); });
  client_2.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 2);
  assert(client1RpcRoundSuccess == 1);
  assert(client2RpcRoundSuccess == 1);
  assert(clientDisconnected == 2);
  assert(serverStopped == 1);
  assert(handlerInvoked.load() == 2);
  {
    std::lock_guard<std::mutex> lock(mutex);
    assert(threadIds.size() == 2);
  }
}

void rpc_client_allows_call_from_non_loop_thread_test() {
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

  int clientConnected = 0;
  int handlerInvoked{0};
  int clientRpcRoundSuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        ++handlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);

  EventLoopThread threadLoop;
  auto loopThread = threadLoop.startLoop();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnected;

      loopThread->queueInLoop([&]() {
        assert(!loop.isInLoopThread());
        client.call("EchoService", "Echo", "hello world",
                    [&](const RpcResponse &response) {
                      loop.assertInLoopThread();
                      assert(response.getResponseResult() ==
                             ResponseResult::kSuccess);
                      assert(response.getPayload() == "hello world");
                      assert(response.getErrorMessage().empty());

                      ++clientRpcRoundSuccess;
                      client.disconnect();
                    });
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 1);
  assert(clientRpcRoundSuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
  assert(handlerInvoked == 1);
}

void rpc_server_returns_error_and_remains_available_when_handler_throws_test() {
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

  int clientConnected = 0;
  int handlerInvoked{0};
  int clientCallbackInvoked = 0;
  int clientRpcRoundSuccess = 0;
  int clientRpcRoundUnsuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        ++handlerInvoked;
        if (handlerInvoked == 1) {
          throw std::runtime_error("run time error");
        }
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      assert(loop.isInLoopThread());
      ++clientConnected;

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            ++clientCallbackInvoked;

            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "handler exception");
            ++clientRpcRoundUnsuccess;

            client.call("EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          ++clientCallbackInvoked;

                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());
                          ++clientRpcRoundSuccess;
                          client.disconnect();
                        });
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 1);
  assert(clientRpcRoundSuccess == 1);
  assert(clientRpcRoundUnsuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
  assert(handlerInvoked == 2);
  assert(clientCallbackInvoked == 2);
}

void rpc_client_ignores_late_response_after_request_timeout_test() {
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

  int clientConnected = 0;
  int serverResponseSent = 0;
  int serverRequestReceived{0};
  int clientCallbackInvoked = 0;
  int clientRpcRoundSuccess = 0;
  int clientRpcRoundUnsuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  uint64_t timedOutRequestId = 0;

  EventLoop loop;
  TcpServer server(&loop, serverFd);
  LengthHeaderCodec codec;
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

  codec.setMessageCallback([&](const TcpConnectionPtr &conn,
                               const std::string &msg) {
    if (msg.empty()) {
      return;
    }

    RpcMessageType messageType;
    bool decodeRes = decodeHead(msg, messageType);
    if (!decodeRes) {
      conn->forceClose();
      return;
    }

    if (messageType == RpcMessageType::kCancel) {
      return;
    }

    RpcRequest request;
    std::string errorMessage;
    bool res = request.decode(msg, errorMessage);
    if (!res || !errorMessage.empty()) {
      conn->forceClose();
      return;
    }

    ++serverRequestReceived;

    if (serverRequestReceived == 1) {
      timedOutRequestId = request.getRequestId();
      return;
    }

    if (serverRequestReceived == 2) {
      assert(timedOutRequestId != 0);
      {
        RpcResponse response(timedOutRequestId, ResponseResult::kSuccess,
                             "response timeout", "");
        std::string outputMessage;
        std::string errorMessage;
        bool res = response.encode(outputMessage, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        if (conn->connected()) {
          ++serverResponseSent;
          codec.send(conn, outputMessage.data(), outputMessage.length());
        }
      }
      {
        RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                             request.getPayload(), "");
        std::string outputMessage;
        std::string errorMessage;
        bool res = response.encode(outputMessage, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        if (conn->connected()) {
          ++serverResponseSent;
          codec.send(conn, outputMessage.data(), outputMessage.length());
        }
      }
    }
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      assert(loop.isInLoopThread());
      ++clientConnected;

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            ++clientCallbackInvoked;

            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request timeout");
            ++clientRpcRoundUnsuccess;

            client.call("EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          ++clientCallbackInvoked;

                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());
                          ++clientRpcRoundSuccess;
                          client.disconnect();
                        });
          },
          std::chrono::milliseconds(100));
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
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 1);
  assert(clientRpcRoundSuccess == 1);
  assert(clientRpcRoundUnsuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
  assert(serverRequestReceived == 2);
  assert(clientCallbackInvoked == 2);
  assert(serverResponseSent == 2);
}

void rpc_client_reconnects_after_connection_closes_and_allows_new_call_test() {
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

  int clientConnected = 0;
  int serverResponseSent = 0;
  int serverRequestReceived{0};
  int clientCallbackInvoked = 0;
  int clientRpcRoundSuccess = 0;
  int clientRpcRoundUnsuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  TcpServer server(&loop, serverFd);
  LengthHeaderCodec codec;
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

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        ++serverRequestReceived;

        if (serverRequestReceived == 1) {
          conn->forceClose();
          return;
        }

        {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            ++serverResponseSent;
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
        }
      });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      assert(loop.isInLoopThread());
      ++clientConnected;

      if (clientConnected == 1) {
        client.call("EchoService", "Echo", "hello world",
                    [&](const RpcResponse &response) {
                      loop.assertInLoopThread();
                      ++clientCallbackInvoked;

                      assert(response.getResponseResult() ==
                             ResponseResult::kUnsuccess);
                      assert(response.getPayload().empty());
                      assert(response.getErrorMessage() == "connect closed");
                      ++clientRpcRoundUnsuccess;
                    });
      }

      if (clientConnected == 2) {
        client.call("EchoService", "Echo", "hello world",
                    [&](const RpcResponse &response) {
                      loop.assertInLoopThread();
                      ++clientCallbackInvoked;

                      assert(response.getResponseResult() ==
                             ResponseResult::kSuccess);
                      assert(response.getPayload() == "hello world");
                      assert(response.getErrorMessage().empty());
                      ++clientRpcRoundSuccess;
                      client.disconnect();
                    });
      }
    } else {
      ++clientDisconnected;

      if (clientDisconnected == 2) {
        server.stop(std::chrono::milliseconds(0));
      }
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.enableRetry();
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 2);
  assert(clientRpcRoundSuccess == 1);
  assert(clientRpcRoundUnsuccess == 1);
  assert(clientDisconnected == 2);
  assert(serverStopped == 1);
  assert(serverRequestReceived == 2);
  assert(clientCallbackInvoked == 2);
  assert(serverResponseSent == 1);
}

void rpc_client_reconnects_after_initial_connection_failure_test() {
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

  int clientConnected = 0;
  int handlerInvoked = 0;
  int clientConnectErrorCnt = 0;
  int clientCallbackInvoked = 0;
  int clientRpcRoundSuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        ++handlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      assert(loop.isInLoopThread());
      ++clientConnected;

      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    ++clientCallbackInvoked;

                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "hello world");
                    assert(response.getErrorMessage().empty());
                    ++clientRpcRoundSuccess;
                    client.disconnect();
                  });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) {
    ++clientConnectErrorCnt;
    server.start();
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 1);
  assert(clientRpcRoundSuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
  assert(handlerInvoked == 1);
  assert(clientCallbackInvoked == 1);
  assert(clientConnectErrorCnt == 1);
}

void rpc_client_disabling_retry_prevents_reconnection_test() {
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

  int clientConnected = 0;
  int clientConnectErrorCnt = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      assert(loop.isInLoopThread());
      ++clientConnected;
    } else {
      ++clientDisconnected;
    }
  });
  client.setConnectionErrorCallback([&](int) {
    ++clientConnectErrorCnt;
    client.disableRetry();
    server.start();

    loop.runAfter(std::chrono::milliseconds(200),
                  [&]() { server.stop(std::chrono::milliseconds(0)); });
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 0);
  assert(clientDisconnected == 0);
  assert(serverStopped == 1);
  assert(clientConnectErrorCnt == 1);
}

void rpc_client_rejects_call_when_max_pending_requests_is_reached_test() {
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
  int pendingRequestClosed = 0;
  int overLimitRequestRejected = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &) {
        ++serverRequestReceived;
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

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::CONNECTION_CLOSED);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "connect closed");

            ++pendingRequestClosed;
          });
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::RESOURCE_EXHAUSTED);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "too many pending requests");

            ++overLimitRequestRejected;
            client.disconnect();
          });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.setMaxPendingRequests(1);
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 1);
  assert(pendingRequestClosed == 1);
  assert(overLimitRequestRejected == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_releases_pending_capacity_before_response_callback_test() {
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
  int firstRequestCnt = 0;
  int secondRequestCnt = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
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

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kSuccess);
            assert(response.getPayload() == "hello world");
            assert(response.getErrorMessage().empty());

            ++firstRequestCnt;

            client.call("EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());

                          ++secondRequestCnt;
                          client.disconnect();
                        });
          });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.setMaxPendingRequests(1);
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 2);
  assert(firstRequestCnt == 1);
  assert(secondRequestCnt == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_releases_pending_capacity_after_request_timeout_test() {
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
  int timeoutCallbackCount = 0;
  int successCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        if (msg.empty()) {
          return;
        }

        RpcMessageType messageType;
        bool decodeRes = decodeHead(msg, messageType);
        if (!decodeRes) {
          conn->forceClose();
          return;
        }

        if (messageType == RpcMessageType::kCancel) {
          return;
        }

        ++serverRequestReceived;

        if (serverRequestReceived == 1) {
          return;
        }

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
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

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request timeout");

            ++timeoutCallbackCount;

            client.call("EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());

                          ++successCallbackCount;
                          client.disconnect();
                        });
          },
          std::chrono::milliseconds(100));
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.setMaxPendingRequests(1);
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 2);
  assert(timeoutCallbackCount == 1);
  assert(successCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_uses_default_timeout_and_allows_explicit_zero_override_test() {
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
  int timeoutCallbackCount = 0;
  int successCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        if (serverRequestReceived == 1) {
          return;
        }

        if (msg.empty()) {
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
          return;
        }

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          loop.runAfter(std::chrono::milliseconds(200),
                        [&codec, conn, msg = std::move(outputMessage)]() {
                          if (conn->connected()) {
                            codec.send(conn, msg.data(), msg.length());
                          }
                        });
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

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(response.getErrorMessage() == "rpc request timeout");

                    ++timeoutCallbackCount;

                    client.call(
                        "EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());

                          ++successCallbackCount;
                          client.disconnect();
                        },
                        std::chrono::milliseconds(0));
                  });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.setDefaultTimeout(std::chrono::milliseconds(100));
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 3);
  assert(timeoutCallbackCount == 1);
  assert(successCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_ignores_late_response_after_request_cancellation_test() {
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

  uint64_t serverFirstRequestId = 0;

  int connected = 0;
  int cancelCallbackCount = 0;
  int successCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        if (msg.empty()) {
          std::abort();
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
          assert(cancel.decode(msg, errorMessage));
          assert(errorMessage.empty());
          return;
        }

        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        if (serverRequestReceived == 1) {
          serverFirstRequestId = request.getRequestId();
          return;
        }

        {
          RpcResponse response(serverFirstRequestId, ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
        }

        {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
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

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      uint64_t canceledRequestId = client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request canceled");

            ++cancelCallbackCount;

            client.call(
                "EchoService", "Echo", "hello world",
                [&](const RpcResponse &response) {
                  assert(response.getResponseResult() ==
                         ResponseResult::kSuccess);
                  assert(response.getPayload() == "hello world");
                  assert(response.getErrorMessage().empty());

                  ++successCallbackCount;
                  client.disconnect();
                },
                std::chrono::milliseconds(0));
          },
          std::chrono::milliseconds(0));

      client.cancel(canceledRequestId);
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
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 2);
  assert(cancelCallbackCount == 1);
  assert(successCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_returns_error_for_unregistered_method_test() {
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
  std::atomic<int> handlerInvoked = 0;
  int errorResponseReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd, 1);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        assert(!loop.isInLoopThread());
        ++handlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  bool unregistered = server.unregisterMethod("EchoService", "Echo");
  assert(unregistered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "service or method not exist");

            ++errorResponseReceived;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(handlerInvoked.load() == 0);
  assert(serverStopped == 1);
  assert(clientDisconnected == 1);
  assert(errorResponseReceived == 1);
}

void rpc_server_keeps_other_methods_after_unregistering_one_method_test() {
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
  std::atomic<int> echoHandlerInvoked = 0;
  std::atomic<int> unregisteredHandlerInvoked = 0;
  int errorResponseReceived = 0;
  int successResponseReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd, 1);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        assert(!loop.isInLoopThread());
        ++echoHandlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  registered = server.registerMethod(
      "EchoService", "Unregist later", [&](const std::string &payload) {
        assert(!loop.isInLoopThread());
        ++unregisteredHandlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  bool unregistered = server.unregisterMethod("EchoService", "Unregist later");
  assert(unregistered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Unregist later", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "service or method not exist");

            ++errorResponseReceived;

            client.call("EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());

                          ++successResponseReceived;
                          client.disconnect();
                        });
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(echoHandlerInvoked.load() == 1);
  assert(unregisteredHandlerInvoked.load() == 0);
  assert(serverStopped == 1);
  assert(clientDisconnected == 1);
  assert(errorResponseReceived == 1);
  assert(successResponseReceived == 1);
}

void rpc_client_allows_request_cancellation_from_non_loop_thread_test() {
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
  std::atomic<int> handlerInvoked = 0;
  int errorResponseReceived = 0;
  int successResponseReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd, 1);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        assert(!loop.isInLoopThread());
        ++handlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);

  EventLoopThread loopThread;
  auto threadLoop = loopThread.startLoop();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      threadLoop->queueInLoop([&]() {
        uint64_t requestId = client.call(
            "EchoService", "Echo", "hello world",
            [&](const RpcResponse &response) {
              loop.assertInLoopThread();
              assert(response.getResponseResult() ==
                     ResponseResult::kUnsuccess);
              assert(response.getPayload().empty());
              assert(response.getErrorMessage() == "rpc request canceled");

              ++errorResponseReceived;
            });
        client.cancel(requestId);

        client.call("EchoService", "Echo", "hello world",
                    [&](const RpcResponse &response) {
                      loop.assertInLoopThread();
                      assert(response.getResponseResult() ==
                             ResponseResult::kSuccess);
                      assert(response.getPayload() == "hello world");
                      assert(response.getErrorMessage().empty());

                      ++successResponseReceived;
                      client.disconnect();
                    });
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(handlerInvoked.load() == 2);
  assert(serverStopped == 1);
  assert(clientDisconnected == 1);
  assert(errorResponseReceived == 1);
  assert(successResponseReceived == 1);
}

void rpc_client_releases_pending_capacity_after_request_cancellation_test() {
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
  int cancelCallbackCount = 0;
  int tooManyPendingCallbackCount = 0;
  int successCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        if (msg.empty()) {
          std::abort();
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
          assert(cancel.decode(msg, errorMessage));
          assert(errorMessage.empty());
          return;
        }

        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        if (serverRequestReceived == 1) {
          return;
        }

        {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
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

  RpcClient client(&loop, serverAddr);
  client.setMaxPendingRequests(1);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      uint64_t canceledRequestId = client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request canceled");

            ++cancelCallbackCount;
          },
          std::chrono::milliseconds(0));

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "too many pending requests");
            ++tooManyPendingCallbackCount;

            client.cancel(canceledRequestId);

            client.call(
                "EchoService", "Echo", "hello world",
                [&](const RpcResponse &response) {
                  assert(response.getResponseResult() ==
                         ResponseResult::kSuccess);
                  assert(response.getPayload() == "hello world");
                  assert(response.getErrorMessage().empty());
                  ++successCallbackCount;

                  client.disconnect();
                },
                std::chrono::milliseconds(0));
          },
          std::chrono::milliseconds(0));

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
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 2);
  assert(cancelCallbackCount == 1);
  assert(tooManyPendingCallbackCount == 1);
  assert(successCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_allows_calls_from_multiple_non_loop_threads_test() {
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
  int successCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  std::vector<RpcRequest> serverReceiveRequests;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        serverReceiveRequests.push_back(request);
        if (serverRequestReceived != 2) {
          return;
        }

        for (int i = serverReceiveRequests.size() - 1; i >= 0; --i) {
          RpcResponse response(serverReceiveRequests[i].getRequestId(),
                               ResponseResult::kSuccess,
                               serverReceiveRequests[i].getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
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

  EventLoopThread eventLoopThread1;
  EventLoopThread eventLoopThread2;

  auto threadLoop1 = eventLoopThread1.startLoop();
  auto threadLoop2 = eventLoopThread2.startLoop();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      threadLoop1->queueInLoop([&]() {
        client.call(
            "EchoService", "Echo", "request from thread 1",
            [&](const RpcResponse &response) {
              loop.assertInLoopThread();
              assert(response.getResponseResult() == ResponseResult::kSuccess);
              assert(response.getPayload() == "request from thread 1");
              assert(response.getErrorMessage().empty());

              ++successCallbackCount;

              if (successCallbackCount == 2) {
                client.disconnect();
              }
            },
            std::chrono::milliseconds(0));
      });

      threadLoop2->queueInLoop([&]() {
        client.call(
            "EchoService", "Echo", "request from thread 2",
            [&](const RpcResponse &response) {
              loop.assertInLoopThread();
              assert(response.getResponseResult() == ResponseResult::kSuccess);
              assert(response.getPayload() == "request from thread 2");
              assert(response.getErrorMessage().empty());

              ++successCallbackCount;

              if (successCallbackCount == 2) {
                client.disconnect();
              }
            },
            std::chrono::milliseconds(0));
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 2);
  assert(successCallbackCount == 2);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_closes_connection_for_malformed_request_test() {
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
  int clientPeerHalfClosed = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd, 1);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  TcpClient client(&loop, serverAddr);
  LengthHeaderCodec codec;
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      std::string output('a', 12);
      codec.send(conn, output.data(), output.size());
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++clientPeerHalfClosed;
    conn->shutdown();
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
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverStopped == 1);
  assert(clientPeerHalfClosed == 1);
  assert(clientDisconnected == 1);
}

void rpc_client_closes_connection_for_malformed_response_test() {
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
  int failCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        if (conn->connected()) {
          std::string outputMessage('a', 12);
          codec.send(conn, outputMessage.data(), outputMessage.length());
        }
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

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      client.call(
          "EchoService", "Echo", "request from thread 1",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "connect closed");

            ++failCallbackCount;
          },
          std::chrono::milliseconds(0));
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
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 1);
  assert(failCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_sends_delayed_response_from_async_handler_test() {
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
  int rpcRoundSuccess = 0;
  int clientDisconnected = 0;
  bool delayedReplyExecuted = false;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        loop.runAfter(std::chrono::milliseconds(500),
                      [&delayedReplyExecuted, payload,
                       reply = std::move(reply)]() mutable {
                        delayedReplyExecuted = true;
                        reply(RpcServer::RpcResult{ResponseResult::kSuccess,
                                                   payload, ""});
                      });
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(delayedReplyExecuted);

                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "hello world");

                    ++rpcRoundSuccess;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundSuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_ignores_duplicate_reply_from_async_handler_test() {
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
  int rpcRoundSuccess = 0;
  int clientDisconnected = 0;
  int asyncHandlerInvoked = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        loop.runAfter(std::chrono::milliseconds(500),
                      [&asyncHandlerInvoked, payload,
                       reply = std::move(reply)]() mutable {
                        ++asyncHandlerInvoked;
                        reply(RpcServer::RpcResult{ResponseResult::kSuccess,
                                                   payload, ""});

                        if (asyncHandlerInvoked == 1) {
                          reply(RpcServer::RpcResult{ResponseResult::kSuccess,
                                                     "second response", ""});
                        }
                      });
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kSuccess);
            assert(response.getPayload() == "hello world");

            ++rpcRoundSuccess;

            client.call("EchoService", "Echo", "hello world2",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world2");

                          ++rpcRoundSuccess;
                          client.disconnect();
                        });
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundSuccess == 2);
  assert(clientDisconnected == 1);
  assert(asyncHandlerInvoked == 2);
  assert(serverStopped == 1);
}

void rpc_server_returns_error_when_async_handler_throws_test() {
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
  int rpcRoundError = 0;
  int clientDisconnected = 0;
  int asyncHandlerInvoked = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        ++asyncHandlerInvoked;
        throw std::runtime_error("error");
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::INTERNAL_ERROR);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "handler exception");
            ++rpcRoundError;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundError == 1);
  assert(clientDisconnected == 1);
  assert(asyncHandlerInvoked == 1);
  assert(serverStopped == 1);
}

void rpc_server_returns_error_for_invalid_async_handler_result_test() {
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
  int rpcRoundError = 0;
  int clientDisconnected = 0;
  int asyncHandlerInvoked = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        ++asyncHandlerInvoked;
        reply(RpcServer::RpcResult{ResponseResult::kSuccess, "",
                                   "unexpected error"});
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "invalid handler result");
            ++rpcRoundError;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundError == 1);
  assert(clientDisconnected == 1);
  assert(asyncHandlerInvoked == 1);
  assert(serverStopped == 1);
}

void rpc_server_sends_response_when_async_reply_is_called_from_another_thread_test() {
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
  int rpcRoundSuccess = 0;
  int clientDisconnected = 0;
  std::atomic<int> asyncHandlerInvoked = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        threadLoop->queueInLoop([threadLoop, &asyncHandlerInvoked,
                                 reply = std::move(reply)]() mutable {
          threadLoop->assertInLoopThread();
          ++asyncHandlerInvoked;
          reply(RpcServer::RpcResult{ResponseResult::kSuccess, "hello world",
                                     ""});
        });
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "hello world");
                    assert(response.getErrorMessage().empty());
                    ++rpcRoundSuccess;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundSuccess == 1);
  assert(clientDisconnected == 1);
  assert(asyncHandlerInvoked.load() == 1);
  assert(serverStopped == 1);
}

void rpc_server_rejects_duplicate_method_registration_across_sync_and_async_handlers_test() {
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

  EventLoop loop;
  RpcServer server(&loop, serverFd);

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [](const std::string &payload, RpcCancellationToken cancellation,
         RpcServer::RpcReply reply) {});
  assert(registered);

  registered = server.registerMethod(
      "EchoService", "Echo",
      [](const std::string &payload) -> RpcServer::RpcResult {
        return RpcServer::RpcResult{};
      });
  assert(!registered);

  registered = server.registerMethod(
      "EchoService", "Echo another",
      [](const std::string &payload) -> RpcServer::RpcResult {
        return RpcServer::RpcResult{};
      });
  assert(registered);

  registered = server.registerAsyncMethod("EchoService", "Echo another",
                                          [](const std::string &payload,
                                             RpcCancellationToken cancellation,
                                             RpcServer::RpcReply reply) {});
  assert(!registered);
}

void rpc_server_handles_async_reply_after_client_disconnects_test() {
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
  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int rpcRoundError = 0;
  int clientDisconnected = 0;
  std::atomic<int> asyncReplyInvoked = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      stopCompletePromise.set_value();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        threadLoop->queueInLoop([&serverStopCompleteFuture, threadLoop,
                                 cancellation, &asyncReplyInvoked,
                                 reply = std::move(reply), &loop]() mutable {
          threadLoop->assertInLoopThread();
          auto waitForRes = serverStopCompleteFuture.wait_for(
              std::chrono::milliseconds(2000));
          if (waitForRes != std::future_status::ready) {
            std::cerr << "wait for server stop complete timeout" << std::endl;
            std::abort();
          }

          assert(cancellation.isCanceled());
          ++asyncReplyInvoked;
          reply(RpcServer::RpcResult{ResponseResult::kSuccess, "hello world",
                                     ""});

          loop.queueInLoop([&loop]() { loop.quit(); });
        });

        client.disconnect();
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(response.getErrorMessage() == "connect closed");
                    ++rpcRoundError;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundError == 1);
  assert(clientDisconnected == 1);
  assert(asyncReplyInvoked.load() == 1);
  assert(serverStopped == 1);
}

void rpc_server_safely_handles_async_reply_after_server_destruction_test() {
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
  std::promise<void> serverDestroyedPromise;
  auto serverDestroyedFuture = serverDestroyedPromise.get_future();

  int connected = 0;
  int rpcRoundError = 0;
  int clientDisconnected = 0;
  std::atomic<int> asyncReplyInvoked = 0;
  int serverStopped = 0;

  EventLoop loop;
  std::unique_ptr<RpcServer> server =
      std::make_unique<RpcServer>(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();

  server->setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;

      loop.queueInLoop([&]() {
        server.reset();
        serverDestroyedPromise.set_value();
      });
    });
  });
  bool registered = server->registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        threadLoop->queueInLoop([&serverDestroyedFuture, threadLoop,
                                 &asyncReplyInvoked, reply = std::move(reply),
                                 &testCompletePromise, &loop]() mutable {
          threadLoop->assertInLoopThread();
          auto waitForRes =
              serverDestroyedFuture.wait_for(std::chrono::milliseconds(2000));
          if (waitForRes != std::future_status::ready) {
            std::cerr << "wait for server destory timeout" << std::endl;
            std::abort();
          }

          ++asyncReplyInvoked;
          reply(RpcServer::RpcResult{ResponseResult::kSuccess, "hello world",
                                     ""});

          loop.queueInLoop([&testCompletePromise, &loop]() {
            testCompletePromise.set_value();
            loop.quit();
          });
        });

        loop.queueInLoop(
            [&server]() { server->stop(std::chrono::milliseconds(0)); });
      });
  assert(registered);

  server->start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(response.getErrorMessage() == "connect closed");
                    ++rpcRoundError;
                  });
    } else {
      ++clientDisconnected;
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

  assert(!server);
  assert(connected == 1);
  assert(rpcRoundError == 1);
  assert(clientDisconnected == 1);
  assert(asyncReplyInvoked.load() == 1);
  assert(serverStopped == 1);
}

void rpc_client_sends_cancel_message_for_pending_request_test() {
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

  uint64_t serverRequestId = 0;

  int connected = 0;
  int rpcErrorCallbackCount = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;
  int serverRequestReceived = 0;
  int serverCancelReceived = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        if (msg.empty()) {
          std::abort();
        }

        RpcMessageType messageType;
        bool decodeRes = decodeHead(msg, messageType);
        if (!decodeRes) {
          conn->forceClose();
          return;
        }

        if (messageType == RpcMessageType::kRequest) {
          assert(!serverCancelReceived);
          ++serverRequestReceived;

          RpcRequest request;
          std::string errorMsg;
          bool res = request.decode(msg, errorMsg);
          if (!res || !errorMsg.empty()) {
            std::abort();
          }

          if (serverRequestReceived == 1) {
            serverRequestId = request.getRequestId();
          }
        } else if (messageType == RpcMessageType::kCancel) {
          assert(serverRequestReceived == 1);
          ++serverCancelReceived;

          RpcCancel cancel;
          std::string errorMsg;
          bool res = cancel.decode(msg, errorMsg);
          if (!res || !errorMsg.empty()) {
            std::abort();
          }
          assert(cancel.getRequestId() == serverRequestId);
          conn->forceClose();
        } else {
          std::abort();
        }
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      uint64_t requestId = client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::REQUEST_CANCELED);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request canceled");

            ++rpcErrorCallbackCount;
          });
      client.cancel(requestId);
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
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 1);
  assert(serverCancelReceived == 1);
  assert(rpcErrorCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_cancels_async_request_before_late_reply_test() {
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

  std::promise<void> secondRequestInvokedPromise;
  auto secondRequestInvokedFuture = secondRequestInvokedPromise.get_future();
  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int firstRequest = 0;
  int secondRequest = 0;
  int clientDisconnected = 0;
  std::atomic<int> lateReplyAttempted = 0;
  int syncReplyInvoked = 0;
  bool firstReplyAttempted = false;
  bool secondResponseReceived = false;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "First_Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        threadLoop->queueInLoop([&secondRequestInvokedFuture, threadLoop,
                                 &lateReplyAttempted, reply = std::move(reply),
                                 cancellation, &loop, &firstReplyAttempted,
                                 &secondResponseReceived, &client]() mutable {
          threadLoop->assertInLoopThread();
          auto waitForRes = secondRequestInvokedFuture.wait_for(
              std::chrono::milliseconds(1000));
          if (waitForRes != std::future_status::ready) {
            std::cerr << "wait for second request invoked timeout" << std::endl;
            std::abort();
          }

          assert(cancellation.isCanceled());

          ++lateReplyAttempted;
          reply(RpcServer::RpcResult{ResponseResult::kSuccess, "hello world",
                                     ""});

          loop.queueInLoop(
              [&firstReplyAttempted, &secondResponseReceived, &client]() {
                firstReplyAttempted = true;
                if (firstReplyAttempted && secondResponseReceived) {
                  client.disconnect();
                }
              });
        });
      });
  assert(registered);

  registered = server.registerMethod(
      "EchoService", "Second_Echo",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        loop.assertInLoopThread();
        ++syncReplyInvoked;
        secondRequestInvokedPromise.set_value();
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      uint64_t requestId = client.call(
          "EchoService", "First_Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request canceled");
            ++firstRequest;
          });

      client.cancel(requestId);

      client.call("EchoService", "Second_Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "hello world");
                    assert(response.getErrorMessage().empty());

                    ++secondRequest;

                    secondResponseReceived = true;
                    if (firstReplyAttempted && secondResponseReceived) {
                      client.disconnect();
                    }
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(firstRequest == 1);
  assert(secondRequest == 1);
  assert(clientDisconnected == 1);
  assert(lateReplyAttempted.load() == 1);
  assert(syncReplyInvoked == 1);
  assert(serverStopped == 1);
}

void rpc_server_drops_async_reply_after_request_cancellation_test() {
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

  std::promise<void> secondRequestInvokedPromise;
  auto secondRequestInvokedFuture = secondRequestInvokedPromise.get_future();
  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int secondRequest = 0;
  int clientDisconnected = 0;
  std::atomic<int> lateReplyAttempted = 0;
  bool cancellationChecked = false;
  bool secondResponseReceived = false;
  int serverStopped = 0;

  uint64_t firstRequestId = 1;
  uint64_t secondRequestId = 2;
  RpcServer::RpcReply firstRequestReply;

  EventLoop loop;
  LengthHeaderCodec codec;
  RpcServer server(&loop, serverFd);
  TcpClient client(&loop, serverAddr);

  client.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        RpcResponse response;
        std::string errorMessage;
        bool res = response.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          std::cerr << "client decode error" << std::endl;
          std::abort();
        }

        if (response.getRequestId() == firstRequestId) {
          std::cerr << "should not receive first request response" << std::endl;
          std::abort();
        }

        if (response.getRequestId() == secondRequestId) {
          loop.assertInLoopThread();
          assert(response.getResponseResult() == ResponseResult::kSuccess);
          assert(response.getPayload() == "hello world");
          assert(response.getErrorMessage().empty());

          ++secondRequest;

          secondResponseReceived = true;
          if (cancellationChecked && secondResponseReceived) {
            client.disconnect();
          }

          return;
        }

        std::cerr << "system error" << std::endl;
        std::abort();
      });

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "First_Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        firstRequestReply = reply;

        threadLoop->queueInLoop([&secondRequestInvokedFuture, threadLoop,
                                 &lateReplyAttempted, cancellation, &loop,
                                 &cancellationChecked, &secondResponseReceived,
                                 &client]() mutable {
          threadLoop->assertInLoopThread();
          auto waitForRes = secondRequestInvokedFuture.wait_for(
              std::chrono::milliseconds(1000));
          if (waitForRes != std::future_status::ready) {
            std::cerr << "wait for second request invoked timeout" << std::endl;
            std::abort();
          }
          ++lateReplyAttempted;

          assert(cancellation.isCanceled());

          loop.queueInLoop(
              [&cancellationChecked, &secondResponseReceived, &client] {
                cancellationChecked = true;
                if (cancellationChecked && secondResponseReceived) {
                  client.disconnect();
                }
              });
        });
      });
  assert(registered);

  registered = server.registerAsyncMethod(
      "EchoService", "Second_Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        loop.assertInLoopThread();
        secondRequestInvokedPromise.set_value();

        loop.queueInLoop(
            [&firstRequestReply, secondReply = std::move(reply)]() {
              firstRequestReply(RpcServer::RpcResult{ResponseResult::kSuccess,
                                                     "hello world", ""});
              secondReply(RpcServer::RpcResult{ResponseResult::kSuccess,
                                               "hello world", ""});
            });
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      std::string output;
      std::string errorMessage;
      RpcRequest firstRequestObj{1, "EchoService", "First_Echo", "hello world"};
      bool res = firstRequestObj.encode(output, errorMessage);
      if (!res || !errorMessage.empty()) {
        std::cerr << "first request encoding error" << std::endl;
        std::abort();
      }
      codec.send(conn, output.data(), output.size());

      output.clear();
      errorMessage.clear();
      RpcCancel firstCancelObj{1};
      res = firstCancelObj.encode(output, errorMessage);
      if (!res || !errorMessage.empty()) {
        std::cerr << "first cancel encoding error" << std::endl;
        std::abort();
      }
      codec.send(conn, output.data(), output.size());

      output.clear();
      errorMessage.clear();
      RpcRequest secondRequestObj{2, "EchoService", "Second_Echo",
                                  "hello world"};
      res = secondRequestObj.encode(output, errorMessage);
      if (!res || !errorMessage.empty()) {
        std::cerr << "second request encoding error" << std::endl;
        std::abort();
      }
      codec.send(conn, output.data(), output.size());
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(secondRequest == 1);
  assert(clientDisconnected == 1);
  assert(lateReplyAttempted.load() == 1);
  assert(serverStopped == 1);
}

void rpc_server_closes_connection_for_duplicate_async_request_id_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int firstHandlerInvoked = 0;
  int secondHandlerInvoked = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  RpcServer server(&loop, serverFd);
  TcpClient client(&loop, serverAddr);

  client.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        std::cerr << "system error" << std::endl;
        std::abort();
      });

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "First_Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        loop.assertInLoopThread();
        ++firstHandlerInvoked;
      });
  assert(registered);

  registered = server.registerAsyncMethod("EchoService", "Second_Echo",
                                          [&](const std::string &payload,
                                              RpcCancellationToken cancellation,
                                              RpcServer::RpcReply reply) {
                                            loop.assertInLoopThread();
                                            ++secondHandlerInvoked;
                                          });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      std::string output;
      std::string errorMessage;
      RpcRequest firstRequestObj{1, "EchoService", "First_Echo", "hello world"};
      bool res = firstRequestObj.encode(output, errorMessage);
      if (!res || !errorMessage.empty()) {
        std::cerr << "first request encoding error" << std::endl;
        std::abort();
      }
      codec.send(conn, output.data(), output.size());

      output.clear();
      errorMessage.clear();
      RpcRequest secondRequestObj{1, "EchoService", "Second_Echo",
                                  "hello world"};
      res = secondRequestObj.encode(output, errorMessage);
      if (!res || !errorMessage.empty()) {
        std::cerr << "second request encoding error" << std::endl;
        std::abort();
      }
      codec.send(conn, output.data(), output.size());
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(firstHandlerInvoked == 1);
  assert(secondHandlerInvoked == 0);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_closes_connection_for_malformed_cancel_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  RpcServer server(&loop, serverFd);
  TcpClient client(&loop, serverAddr);

  client.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        std::cerr << "system error" << std::endl;
        std::abort();
      });

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      std::string output;
      std::string errorMessage;
      RpcCancel cancelObj{1};
      bool res = cancelObj.encode(output, errorMessage);
      if (!res || !errorMessage.empty()) {
        std::cerr << "cancel encoding error" << std::endl;
        std::abort();
      }
      codec.send(conn, output.data(), output.size() - 1);
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_ignores_cancel_for_unknown_request_id_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int echoHandlerCnt = 0;
  int echoRequestCnt = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  RpcServer server(&loop, serverFd);
  TcpClient client(&loop, serverAddr);

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        loop.assertInLoopThread();
        ++echoHandlerCnt;
        reply(RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""});
      });
  assert(registered);

  server.start();

  client.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        RpcResponse response;
        std::string errorMessage;
        bool res = response.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          std::cerr << "client decode error" << std::endl;
          std::abort();
        }

        if (response.getRequestId() != 2) {
          std::cerr << "system error" << std::endl;
          std::abort();
        }

        loop.assertInLoopThread();
        assert(response.getResponseResult() == ResponseResult::kSuccess);
        assert(response.getPayload() == "hello world");
        assert(response.getErrorMessage().empty());

        ++echoRequestCnt;
        client.disconnect();
      });

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      std::string output;
      std::string errorMessage;
      RpcCancel cancelObj{1024};
      bool res = cancelObj.encode(output, errorMessage);
      if (!res || !errorMessage.empty()) {
        std::cerr << "cancel encoding error" << std::endl;
        std::abort();
      }
      codec.send(conn, output.data(), output.size());

      output.clear();
      errorMessage.clear();
      RpcRequest requestObj{2, "EchoService", "Echo", "hello world"};
      res = requestObj.encode(output, errorMessage);
      if (!res || !errorMessage.empty()) {
        std::cerr << "request encoding error" << std::endl;
        std::abort();
      }
      codec.send(conn, output.data(), output.size());
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(echoHandlerCnt == 1);
  assert(echoRequestCnt == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_cancels_async_request_after_client_timeout_test() {
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

  std::promise<void> secondRequestInvokedPromise;
  auto secondRequestInvokedFuture = secondRequestInvokedPromise.get_future();
  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int firstRequest = 0;
  int secondRequest = 0;
  int clientDisconnected = 0;
  std::atomic<int> lateReplyAttempted = 0;
  int syncReplyInvoked = 0;
  bool firstReplyAttempted = false;
  bool secondResponseReceived = false;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "First_Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        threadLoop->queueInLoop([&secondRequestInvokedFuture, threadLoop,
                                 &lateReplyAttempted, reply = std::move(reply),
                                 cancellation, &loop, &firstReplyAttempted,
                                 &secondResponseReceived, &client]() mutable {
          threadLoop->assertInLoopThread();
          auto waitForRes = secondRequestInvokedFuture.wait_for(
              std::chrono::milliseconds(1000));
          if (waitForRes != std::future_status::ready) {
            std::cerr << "wait for second request invoked timeout" << std::endl;
            std::abort();
          }

          assert(cancellation.isCanceled());

          ++lateReplyAttempted;
          reply(RpcServer::RpcResult{ResponseResult::kSuccess, "hello world",
                                     ""});

          loop.queueInLoop(
              [&firstReplyAttempted, &secondResponseReceived, &client]() {
                firstReplyAttempted = true;
                if (firstReplyAttempted && secondResponseReceived) {
                  client.disconnect();
                }
              });
        });
      });
  assert(registered);

  registered = server.registerMethod(
      "EchoService", "Second_Echo",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        loop.assertInLoopThread();
        ++syncReplyInvoked;
        secondRequestInvokedPromise.set_value();
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      uint64_t requestId = client.call(
          "EchoService", "First_Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request timeout");
            ++firstRequest;

            client.call("EchoService", "Second_Echo", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());

                          ++secondRequest;

                          secondResponseReceived = true;
                          if (firstReplyAttempted && secondResponseReceived) {
                            client.disconnect();
                          }
                        });
          },
          std::chrono::milliseconds(200));

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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(firstRequest == 1);
  assert(secondRequest == 1);
  assert(clientDisconnected == 1);
  assert(lateReplyAttempted.load() == 1);
  assert(syncReplyInvoked == 1);
  assert(serverStopped == 1);
}

void rpc_client_does_not_send_cancel_after_response_before_timeout_test() {
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
  int requestCallbackCount = 0;
  int serverRequestReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        if (msg.empty()) {
          std::abort();
        }

        RpcMessageType messageType;
        bool decodeRes = decodeHead(msg, messageType);
        if (!decodeRes) {
          conn->forceClose();
          return;
        }

        if (messageType == RpcMessageType::kRequest) {
          ++serverRequestReceived;

          RpcRequest request;
          std::string errorMsg;
          std::string output;
          bool res = request.decode(msg, errorMsg);
          if (!res || !errorMsg.empty()) {
            std::abort();
          }

          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          output.clear();
          errorMsg.clear();
          res = response.encode(output, errorMsg);
          if (!res || !errorMsg.empty()) {
            std::abort();
          }
          codec.send(conn, output.data(), output.length());

          loop.runAfter(std::chrono::milliseconds(1000),
                        [conn]() { conn->forceClose(); });
        } else {
          std::abort();
        }
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kSuccess);
            assert(response.getPayload() == "hello world");
            assert(response.getErrorMessage().empty());

            ++requestCallbackCount;
          },
          std::chrono::milliseconds(500));
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
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 1);
  assert(requestCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_rejects_async_request_when_active_request_limit_is_reached_test() {
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

  std::promise<void> secondRequestCallbackInvokedPromise;
  auto secondRequestCallbackInvokedFuture =
      secondRequestCallbackInvokedPromise.get_future();
  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  std::atomic<int> serverHandled = 0;
  int firstRequestCallback = 0;
  int secondRequestCallback = 0;
  int thirdRequestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoopThread loopThread;
  auto threadLoop = loopThread.startLoop();

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        threadLoop->queueInLoop([&, reply = std::move(reply)]() mutable {
          ++serverHandled;
          auto waitForRes = secondRequestCallbackInvokedFuture.wait_for(
              std::chrono::milliseconds(500));
          if (waitForRes != std::future_status::ready) {
            std::cerr << "wait for second request callback timeout"
                      << std::endl;
            std::abort();
          }

          reply({ResponseResult::kSuccess, "hello world", ""});
        });
      });
  assert(registered);

  registered = server.registerAsyncMethod(
      "EchoService", "Echo_",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        ++serverHandled;
        reply({ResponseResult::kSuccess, payload, ""});
      });
  assert(registered);

  server.setMaxActiveRequestsPerConnection(1);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kSuccess);
            assert(response.getPayload() == "hello world");
            assert(response.getErrorMessage().empty());
            ++firstRequestCallback;

            client.call("EchoService", "Echo_", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());

                          ++thirdRequestCallback;
                          client.disconnect();
                        });
          });

      client.call("EchoService", "Echo_", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(response.getErrorMessage() ==
                           "server request limit reached");
                    ++secondRequestCallback;
                    secondRequestCallbackInvokedPromise.set_value();
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled.load() == 2);
  assert(firstRequestCallback == 1);
  assert(secondRequestCallback == 1);
  assert(thirdRequestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_rejects_async_request_when_global_active_request_limit_is_reached_test() {
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

  std::promise<void> secondRequestCallbackInvokedPromise;
  auto secondRequestCallbackInvokedFuture =
      secondRequestCallbackInvokedPromise.get_future();
  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int client1Connected = 0;
  int client2Connected = 0;
  std::atomic<int> serverHandled = 0;
  int firstRequestCallback = 0;
  int secondRequestCallback = 0;
  int thirdRequestCallback = 0;
  int client1Disconnected = 0;
  int client2Disconnected = 0;
  int serverStopped = 0;

  EventLoopThread loopThread;
  auto threadLoop = loopThread.startLoop();

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client1(&loop, serverAddr);
  RpcClient client2(&loop, serverAddr);

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        loop.assertInLoopThread();
        client2.connect();
        threadLoop->queueInLoop([&, reply = std::move(reply)]() mutable {
          ++serverHandled;
          auto waitForRes = secondRequestCallbackInvokedFuture.wait_for(
              std::chrono::milliseconds(500));
          if (waitForRes != std::future_status::ready) {
            std::cerr << "wait for second request callback timeout"
                      << std::endl;
            std::abort();
          }

          reply({ResponseResult::kSuccess, "hello world", ""});
        });
      });
  assert(registered);

  registered = server.registerAsyncMethod(
      "EchoService", "Echo_",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        ++serverHandled;
        reply({ResponseResult::kSuccess, payload, ""});
      });
  assert(registered);

  server.setMaxActiveRequests(1);
  server.start();

  client1.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++client1Connected;
      client1.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kSuccess);
            assert(response.getPayload() == "hello world");
            assert(response.getErrorMessage().empty());
            ++firstRequestCallback;

            client2.call("EchoService", "Echo_", "hello world",
                         [&](const RpcResponse &response) {
                           loop.assertInLoopThread();
                           assert(response.getResponseResult() ==
                                  ResponseResult::kSuccess);
                           assert(response.getPayload() == "hello world");
                           assert(response.getErrorMessage().empty());

                           ++thirdRequestCallback;
                           client1.disconnect();
                           client2.disconnect();
                         });
          });
    } else {
      ++client1Disconnected;
      if (client2Disconnected) {
        server.stop(std::chrono::milliseconds(0));
      }
    }
  });
  client1.setConnectionErrorCallback([&](int) { std::abort(); });
  client1.connect();

  client2.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++client2Connected;
      client2.call("EchoService", "Echo_", "hello world",
                   [&](const RpcResponse &response) {
                     loop.assertInLoopThread();
                     assert(response.getResponseResult() ==
                            ResponseResult::kUnsuccess);
                     assert(response.getPayload().empty());
                     assert(response.getErrorMessage() ==
                            "server request limit reached");
                     ++secondRequestCallback;

                     secondRequestCallbackInvokedPromise.set_value();
                   });
    } else {
      ++client2Disconnected;
      if (client1Disconnected) {
        server.stop(std::chrono::milliseconds(0));
      }
    }
  });
  client2.setConnectionErrorCallback([&](int) { std::abort(); });

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(client1Connected == 1);
  assert(client2Connected == 1);
  assert(serverHandled.load() == 2);
  assert(firstRequestCallback == 1);
  assert(secondRequestCallback == 1);
  assert(thirdRequestCallback == 1);
  assert(client1Disconnected == 1);
  assert(client2Disconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_releases_active_request_capacity_after_cancellation_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  std::atomic<int> serverHandled = 0;
  int firstRequestCallback = 0;
  int secondRequestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) { ++serverHandled; });
  assert(registered);

  registered = server.registerAsyncMethod(
      "EchoService", "Echo_",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        ++serverHandled;
        reply({ResponseResult::kSuccess, payload, ""});
      });
  assert(registered);

  server.setMaxActiveRequestsPerConnection(1);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      uint64_t requestId = client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request canceled");
            ++firstRequestCallback;

            client.call("EchoService", "Echo_", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());

                          ++secondRequestCallback;
                          client.disconnect();
                        });
          });

      client.cancel(requestId);
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled.load() == 2);
  assert(firstRequestCallback == 1);
  assert(secondRequestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_closes_connection_for_unsupported_protocol_version_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  RpcServer server(&loop, serverFd);
  TcpClient client(&loop, serverAddr);

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  client.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      std::string output;
      std::string errorMessage;
      RpcRequest request(1, "service", "method", "123");
      bool res = request.encode(output, errorMessage);
      if (!res || !errorMessage.empty()) {
        std::cerr << "request encoding error" << std::endl;
        std::abort();
      }

      uint16_t versionNet = 100;
      versionNet = htons(versionNet);
      ::memcpy(output.data(), &versionNet, sizeof(versionNet));
      codec.send(conn, output.data(), output.size());
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_closes_connection_for_unsupported_protocol_version_test() {
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
  int requestCallbackCount = 0;
  int serverRequestReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        if (msg.empty()) {
          std::abort();
        }

        RpcMessageType messageType;
        bool decodeRes = decodeHead(msg, messageType);
        if (!decodeRes) {
          conn->forceClose();
          return;
        }
        assert(messageType == RpcMessageType::kRequest);

        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMsg;
        std::string output;
        bool res = request.decode(msg, errorMsg);
        if (!res || !errorMsg.empty()) {
          std::abort();
        }

        RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                             request.getPayload(), "");
        output.clear();
        errorMsg.clear();
        res = response.encode(output, errorMsg);
        if (!res || !errorMsg.empty()) {
          std::abort();
        }

        uint16_t versionNet = 100;
        versionNet = htons(versionNet);
        ::memcpy(output.data(), &versionNet, sizeof(versionNet));
        codec.send(conn, output.data(), output.length());
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(response.getErrorMessage() == "connect closed");
                    ++requestCallbackCount;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 1);
  assert(requestCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_closes_connection_for_unexpected_response_message_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  RpcServer server(&loop, serverFd);
  TcpClient client(&loop, serverAddr);

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  client.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      std::string output;
      std::string errorMessage;
      RpcResponse response(1, ResponseResult::kSuccess, "hello world", "");
      bool res = response.encode(output, errorMessage);
      if (!res || !errorMessage.empty()) {
        std::cerr << "response encoding error" << std::endl;
        std::abort();
      }

      codec.send(conn, output.data(), output.size());
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_closes_connection_for_unexpected_request_message_test() {
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
  int requestCallbackCount = 0;
  int serverRequestReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        if (msg.empty()) {
          std::abort();
        }

        RpcMessageType messageType;
        bool decodeRes = decodeHead(msg, messageType);
        if (!decodeRes) {
          conn->forceClose();
          return;
        }
        assert(messageType == RpcMessageType::kRequest);

        RpcRequest request;
        std::string errorMsg;
        std::string output;
        bool res = request.decode(msg, errorMsg);
        if (!res || !errorMsg.empty()) {
          std::abort();
        }

        ++serverRequestReceived;

        codec.send(conn, msg.data(), msg.length());
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(response.getErrorMessage() == "connect closed");
                    ++requestCallbackCount;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 1);
  assert(requestCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_rejects_oversized_request_before_sending_test() {
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
  int requestCallbackCount = 0;
  int serverRequestReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        if (msg.empty()) {
          std::abort();
        }

        RpcMessageType messageType;
        bool decodeRes = decodeHead(msg, messageType);
        if (!decodeRes) {
          conn->forceClose();
          return;
        }
        assert(messageType == RpcMessageType::kRequest);

        ++serverRequestReceived;
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  server.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", std::string(30, 'M'),
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::MESSAGE_TOO_LARGE);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc message too large");
            ++requestCallbackCount;
            client.disconnect();
          });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.setMaxMessageSize(30);
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 0);
  assert(requestCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_returns_error_for_oversized_response_and_remains_available_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int serverHandled = 0;
  int firstRequestCallback = 0;
  int secondRequestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        ++serverHandled;
        reply({ResponseResult::kSuccess, std::string(100, 'M'), ""});
      });
  assert(registered);

  registered = server.registerAsyncMethod(
      "EchoService", "Echo_",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        ++serverHandled;
        reply({ResponseResult::kSuccess, payload, ""});
      });
  assert(registered);
  server.setMaxMessageSize(100);
  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::MESSAGE_TOO_LARGE);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc message too large");
            ++firstRequestCallback;

            client.call("EchoService", "Echo_", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getErrorCode() ==
                                 RpcErrorCode::NO_ERROR);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());

                          ++secondRequestCallback;
                          client.disconnect();
                        });
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 2);
  assert(firstRequestCallback == 1);
  assert(secondRequestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_returns_error_for_oversized_sync_response_and_remains_available_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int serverHandled = 0;
  int firstRequestCallback = 0;
  int secondRequestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        ++serverHandled;
        return {ResponseResult::kSuccess, std::string(100, 'M'), ""};
      });
  assert(registered);

  registered = server.registerMethod(
      "EchoService", "Echo_",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        ++serverHandled;
        return {ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);
  server.setMaxMessageSize(100);
  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc message too large");
            ++firstRequestCallback;

            client.call("EchoService", "Echo_", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());

                          ++secondRequestCallback;
                          client.disconnect();
                        });
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 2);
  assert(firstRequestCallback == 1);
  assert(secondRequestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_closes_connection_for_request_exceeding_message_size_limit_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  RpcServer server(&loop, serverFd);
  TcpClient client(&loop, serverAddr);

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.setMaxMessageSize(30);
  server.start();

  client.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      std::string output;
      std::string errorMessage;
      RpcRequest request(1, "service", "method", "hello world");
      bool res = request.encode(output, errorMessage);
      if (!res || !errorMessage.empty()) {
        std::cerr << "request encoding error" << std::endl;
        std::abort();
      }

      assert(output.size() > 30);
      codec.send(conn, output.data(), output.size());
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_closes_connection_for_response_exceeding_message_size_limit_test() {
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
  int requestCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        assert(RpcResponse::OutputMessageLen(50, 0) > 50);
        RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                             std::string(50, 'M'), "");
        std::string outputMessage;
        errorMessage.clear();
        res = response.encode(outputMessage, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        if (conn->connected()) {
          assert(outputMessage.length() > 50);
          codec.send(conn, outputMessage.data(), outputMessage.length());
        }
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  server.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      assert(RpcRequest::OutputMessageLen(7, 6, 0) <= 50);
      client.call("service", "method", "", [&](const RpcResponse &response) {
        assert(response.getResponseResult() == ResponseResult::kUnsuccess);
        assert(response.getPayload().empty());
        assert(response.getErrorMessage() == "connect closed");

        ++requestCallbackCount;
      });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.setMaxMessageSize(50);
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 1);
  assert(requestCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_resends_pending_request_after_reconnect_test() {
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
  int rpcSuccessCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  uint64_t firstCallRequestId;
  std::string firstCallService;
  std::string firstCallMethod;
  std::string firstCallPayload;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          std::cerr << __LINE__ << ": server decode error" << std::endl;
          conn->forceClose();
          return;
        }

        if (serverRequestReceived == 1) {
          firstCallRequestId = request.getRequestId();
          firstCallService = request.getService();
          firstCallMethod = request.getMethod();
          firstCallPayload = request.getPayload();

          conn->forceClose();
        } else if (serverRequestReceived == 2) {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          errorMessage.clear();
          res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            std::cerr << __LINE__ << ": server encode error" << std::endl;
            conn->forceClose();
            return;
          }

          assert(firstCallRequestId == request.getRequestId());
          assert(firstCallService == request.getService());
          assert(firstCallMethod == request.getMethod());
          assert(firstCallPayload == request.getPayload());

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
        } else {
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

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      if (connected != 1) {
        return;
      }

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kSuccess);
            assert(response.getPayload() == "hello world");
            assert(response.getErrorMessage().empty());

            ++rpcSuccessCallbackCount;
            client.disconnect();
          },
          std::chrono::milliseconds(500), true, std::chrono::milliseconds(500));
    } else {
      ++clientDisconnected;
      if (clientDisconnected == 2) {
        server.stop(std::chrono::milliseconds(0));
      }
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
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 2);
  assert(serverRequestReceived == 2);
  assert(rpcSuccessCallbackCount == 1);
  assert(clientDisconnected == 2);
  assert(serverStopped == 1);
}

void rpc_client_fails_pending_request_when_reconnect_wait_expires_test() {
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
  int rpcErrorCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int completionCount = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback([&](const TcpConnectionPtr &, const std::string &) {
    ++serverRequestReceived;
    server.stop(std::chrono::milliseconds(0));
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
      if (serverStopped && rpcErrorCallbackCount) {
        ++completionCount;
        stopCompletePromise.set_value();
        loop.quit();
      }
    });
  });
  server.start();

  auto start = std::chrono::steady_clock::now();

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setDefaultResendLimit(2);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::RECONNECT_TIMEOUT);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc reconnect timeout");
            ++rpcErrorCallbackCount;
            client.disconnect();

            auto end = std::chrono::steady_clock::now();
            assert(end - start >= std::chrono::milliseconds(900));

            if (serverStopped && rpcErrorCallbackCount) {
              ++completionCount;
              stopCompletePromise.set_value();
              loop.quit();
            }
          },
          std::chrono::milliseconds(500), true, std::chrono::milliseconds(500));
    } else {
      ++clientDisconnected;

      if (clientDisconnected == 1) {
        start = std::chrono::steady_clock::now();
      }
    }
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 1);
  assert(rpcErrorCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(completionCount == 1);
  assert(serverStopped == 1);
}

void rpc_client_fails_request_when_resend_budget_is_exhausted_test() {
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
  int rpcErrorCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &) {
        ++serverRequestReceived;
        conn->forceClose();
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

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      if (connected != 1) {
        return;
      }

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "connect closed");
            ++rpcErrorCallbackCount;
            client.disconnect();
          },
          std::chrono::milliseconds(500), true, std::chrono::milliseconds(500));
    } else {
      ++clientDisconnected;

      if (clientDisconnected == 2) {
        server.stop(std::chrono::milliseconds(0));
      }
    }
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 2);
  assert(serverRequestReceived == 2);
  assert(rpcErrorCallbackCount == 1);
  assert(clientDisconnected == 2);
  assert(serverStopped == 1);
}

void rpc_client_resends_only_opted_in_requests_after_disconnect_test() {
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
  int rpcSuccessCallbackCount = 0;
  int rpcErrorCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  std::optional<uint64_t> firstCallRequestId;
  std::string firstCallService;
  std::string firstCallMethod;
  std::string firstCallPayload;

  std::optional<uint64_t> secondCallRequestId;
  std::string secondCallService;
  std::string secondCallMethod;
  std::string secondCallPayload;

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          std::cerr << __LINE__ << ": server decode error" << std::endl;
          conn->forceClose();
          return;
        }

        if (!firstCallRequestId.has_value()) {
          firstCallRequestId = request.getRequestId();
          firstCallService = request.getService();
          firstCallMethod = request.getMethod();
          firstCallPayload = request.getPayload();
        } else if (!secondCallRequestId.has_value()) {
          secondCallRequestId = request.getRequestId();
          secondCallService = request.getService();
          secondCallMethod = request.getMethod();
          secondCallPayload = request.getPayload();
        } else {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          errorMessage.clear();
          res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            std::cerr << __LINE__ << ": server encode error" << std::endl;
            conn->forceClose();
            return;
          }

          assert(firstCallRequestId == request.getRequestId());
          assert(firstCallService == request.getService());
          assert(firstCallMethod == request.getMethod());
          assert(firstCallPayload == request.getPayload());

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }

          return;
        }

        if (firstCallRequestId.has_value() && secondCallRequestId.has_value()) {
          conn->forceClose();
        }
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

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      if (connected != 1) {
        return;
      }

      client.call(
          "EchoService", "Echo", "hello world1",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kSuccess);
            assert(response.getPayload() == "hello world1");
            assert(response.getErrorMessage().empty());
            ++rpcSuccessCallbackCount;
            client.disconnect();
          },
          std::chrono::milliseconds(500), true, std::chrono::milliseconds(500));

      client.call("EchoService", "Echo", "hello world2",
                  [&](const RpcResponse &response) {
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(response.getErrorMessage() == "connect closed");
                    assert(connected == 1);
                    ++rpcErrorCallbackCount;
                  });
    } else {
      ++clientDisconnected;

      if (clientDisconnected == 2) {
        server.stop(std::chrono::milliseconds(0));
      }
    }
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 2);
  assert(serverRequestReceived == 3);
  assert(rpcSuccessCallbackCount == 1);
  assert(rpcErrorCallbackCount == 1);
  assert(clientDisconnected == 2);
  assert(serverStopped == 1);
}

void rpc_client_does_not_resend_request_canceled_while_waiting_for_reconnect_test() {
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
  int rpcSuccessCallbackCount = 0;
  int connectionClosedCallbackCount = 0; // A
  int canceledCallbackCount = 0;         // B
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  std::optional<uint64_t> firstCallRequestId;
  std::optional<uint64_t> secondCallRequestId;
  std::optional<uint64_t> thirdCallRequestId;

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          std::cerr << __LINE__ << ": server decode error" << std::endl;
          conn->forceClose();
          return;
        }

        if (!firstCallRequestId.has_value()) {
          firstCallRequestId = request.getRequestId();
        } else if (!secondCallRequestId.has_value()) {
          secondCallRequestId = request.getRequestId();
        } else if (!thirdCallRequestId.has_value()) {
          thirdCallRequestId = request.getRequestId();
        } else {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          errorMessage.clear();
          res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            std::cerr << __LINE__ << ": server encode error" << std::endl;
            conn->forceClose();
            return;
          }

          assert(thirdCallRequestId == request.getRequestId());

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }

          return;
        }

        if (firstCallRequestId.has_value() && secondCallRequestId.has_value() &&
            thirdCallRequestId.has_value()) {
          conn->forceClose();
        }
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

  uint64_t secondRequestId = 0;
  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      if (connected != 1) {
        return;
      }

      client.call("EchoService", "Echo", "hello world1",
                  [&](const RpcResponse &response) {
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(response.getErrorMessage() == "connect closed");
                    assert(connected == 1);
                    client.cancel(secondRequestId);
                    ++connectionClosedCallbackCount;
                  });

      secondRequestId = client.call(
          "EchoService", "Echo", "hello world2",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request canceled");
            ++canceledCallbackCount;
          },
          std::chrono::milliseconds(500), true, std::chrono::milliseconds(500));

      client.call(
          "EchoService", "Echo", "hello world3",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kSuccess);
            assert(response.getPayload() == "hello world3");
            assert(response.getErrorMessage().empty());
            ++rpcSuccessCallbackCount;
            client.disconnect();
          },
          std::chrono::milliseconds(500), true,
          std::chrono::milliseconds(1000));

    } else {
      ++clientDisconnected;

      if (clientDisconnected == 2) {
        server.stop(std::chrono::milliseconds(0));
      }
    }
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 2);
  assert(serverRequestReceived == 4);
  assert(rpcSuccessCallbackCount == 1);
  assert(connectionClosedCallbackCount == 1);
  assert(canceledCallbackCount == 1);
  assert(clientDisconnected == 2);
  assert(serverStopped == 1);
}

void rpc_client_fails_waiting_requests_on_manual_disconnect_test() {
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
  int firstRequestClosedCallbackCount = 0;  // A
  int secondRequestClosedCallbackCount = 0; // B
  int thirdRequestClosedCallbackCount = 0;  // C
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  std::optional<uint64_t> firstCallRequestId;
  std::optional<uint64_t> secondCallRequestId;
  std::optional<uint64_t> thirdCallRequestId;

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          std::cerr << __LINE__ << ": server decode error" << std::endl;
          conn->forceClose();
          return;
        }

        if (!firstCallRequestId.has_value()) {
          firstCallRequestId = request.getRequestId();
        } else if (!secondCallRequestId.has_value()) {
          secondCallRequestId = request.getRequestId();
        } else if (!thirdCallRequestId.has_value()) {
          thirdCallRequestId = request.getRequestId();
        } else {
          std::abort();
        }

        if (firstCallRequestId.has_value() && secondCallRequestId.has_value() &&
            thirdCallRequestId.has_value()) {
          conn->forceClose();
        }
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

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      if (connected != 1) {
        return;
      }

      client.call("EchoService", "Echo", "hello world1",
                  [&](const RpcResponse &response) {
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(response.getErrorMessage() == "connect closed");
                    assert(connected == 1);
                    client.disconnect();
                    ++firstRequestClosedCallbackCount;
                  });

      client.call(
          "EchoService", "Echo", "hello world2",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "connect closed");
            ++secondRequestClosedCallbackCount;
          },
          std::chrono::milliseconds(500), true, std::chrono::milliseconds(500));

      client.call(
          "EchoService", "Echo", "hello world3",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "connect closed");
            ++thirdRequestClosedCallbackCount;
          },
          std::chrono::milliseconds(500), true,
          std::chrono::milliseconds(1000));
    } else {
      ++clientDisconnected;

      loop.runAfter(std::chrono::milliseconds(1000), [&]() {
        assert(connected == 1);
        assert(clientDisconnected == 1);
        server.stop(std::chrono::milliseconds(0));
      });
    }
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 3);
  assert(firstRequestClosedCallbackCount == 1);
  assert(secondRequestClosedCallbackCount == 1);
  assert(thirdRequestClosedCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_restarts_response_timeout_after_resend_test() {
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
  int successRequestCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        if (serverRequestReceived == 1) {
          loop.runAfter(std::chrono::milliseconds(600),
                        [conn]() { conn->forceClose(); });
          return;
        }

        loop.runAfter(std::chrono::milliseconds(600), [conn, msg, &codec]() {
          RpcRequest request;
          std::string errorMessage;
          bool res = request.decode(msg, errorMessage);
          if (!res || !errorMessage.empty()) {
            std::cerr << __LINE__ << ": server decode error" << std::endl;
            conn->forceClose();
            return;
          }

          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          errorMessage.clear();
          res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            std::cerr << __LINE__ << ": server encode error" << std::endl;
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
        });
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

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      if (connected != 1) {
        return;
      }

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kSuccess);
            assert(response.getPayload() == "hello world");
            assert(response.getErrorMessage().empty());
            ++successRequestCallbackCount;
            client.disconnect();
          },
          std::chrono::milliseconds(1000), true,
          std::chrono::milliseconds(500));
    } else {
      ++clientDisconnected;

      if (clientDisconnected == 1) {
        return;
      }
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 2);
  assert(serverRequestReceived == 2);
  assert(successRequestCallbackCount == 1);
  assert(clientDisconnected == 2);
  assert(serverStopped == 1);
}

void rpc_client_times_out_waiting_for_response_after_resend_test() {
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
  int unsuccessRequestCallbackCount = 0;
  int clientDisconnected = 0;
  int serverMessageReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  uint64_t requestId = 0;

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverMessageReceived;

        if (serverMessageReceived == 1) {
          RpcMessageType messageType;
          bool res = decodeHead(msg, messageType);
          assert(res);
          assert(messageType == RpcMessageType::kRequest);

          RpcRequest request;
          std::string errorMessage;
          res = request.decode(msg, errorMessage);
          if (!res || !errorMessage.empty()) {
            std::cerr << __LINE__ << ": server decode error" << std::endl;
            std::abort();
            return;
          }

          requestId = request.getRequestId();

        } else if (serverMessageReceived == 2) {
          RpcMessageType messageType;
          bool res = decodeHead(msg, messageType);
          assert(res);
          assert(messageType == RpcMessageType::kRequest);

          RpcRequest request;
          std::string errorMessage;
          res = request.decode(msg, errorMessage);
          if (!res || !errorMessage.empty()) {
            std::cerr << __LINE__ << ": server decode error" << std::endl;
            std::abort();
            return;
          }

          assert(requestId == request.getRequestId());
        } else if (serverMessageReceived == 3) {
          RpcMessageType messageType;
          bool res = decodeHead(msg, messageType);
          assert(res);
          assert(messageType == RpcMessageType::kCancel);

          RpcCancel cancel;
          std::string errorMessage;
          res = cancel.decode(msg, errorMessage);
          if (!res || !errorMessage.empty()) {
            std::cerr << __LINE__ << ": server decode error" << std::endl;
            std::abort();
            return;
          }

          assert(requestId == cancel.getRequestId());

        } else {
          std::abort();
        }

        if (serverMessageReceived == 1) {
          loop.runAfter(std::chrono::milliseconds(600),
                        [conn]() { conn->forceClose(); });
          return;
        }
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

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      if (connected != 1) {
        return;
      }

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request timeout");
            ++unsuccessRequestCallbackCount;
            client.disconnect();
          },
          std::chrono::milliseconds(1000), true,
          std::chrono::milliseconds(500));
    } else {
      ++clientDisconnected;

      if (clientDisconnected == 2) {
        server.stop(std::chrono::milliseconds(0));
      }
    }
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 2);
  assert(serverMessageReceived == 3);
  assert(unsuccessRequestCallbackCount == 1);
  assert(clientDisconnected == 2);
  assert(serverStopped == 1);
}

void rpc_client_respects_configured_resend_limit_test() {
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
  int unsuccessRequestCallbackCount = 0;
  int clientDisconnected = 0;
  int serverMessageReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  uint64_t requestId = 0;
  std::string serverName;
  std::string methodName;
  std::string payload;

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverMessageReceived;

        RpcMessageType messageType;
        bool res = decodeHead(msg, messageType);
        assert(res);
        assert(messageType == RpcMessageType::kRequest);

        RpcRequest request;
        std::string errorMessage;
        res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          std::cerr << __LINE__ << ": server decode error" << std::endl;
          std::abort();
          return;
        }

        if (serverMessageReceived == 1) {
          requestId = request.getRequestId();
          serverName = request.getService();
          methodName = request.getMethod();
          payload = request.getPayload();
        } else {
          assert(requestId == request.getRequestId());
          assert(serverName == request.getService());
          assert(methodName == request.getMethod());
          assert(payload == request.getPayload());
        }

        conn->forceClose();
      });

  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    ++serverStopped;
    stopCompletePromise.set_value();
    loop.quit();
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setDefaultResendLimit(2);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      if (connected != 1) {
        return;
      }

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "connect closed");
            ++unsuccessRequestCallbackCount;
            client.disconnect();
          },
          std::chrono::milliseconds(1000), true,
          std::chrono::milliseconds(500));
    } else {
      ++clientDisconnected;

      if (clientDisconnected == 3) {
        loop.runAfter(std::chrono::milliseconds(1000),
                      [&]() { server.stop(std::chrono::milliseconds(0)); });
      }
    }
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 3);
  assert(serverMessageReceived == 3);
  assert(unsuccessRequestCallbackCount == 1);
  assert(clientDisconnected == 3);
  assert(serverStopped == 1);
}

void rpc_client_uses_per_call_resend_limit_over_default_test() {
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
  int unsuccessRequestCallbackCount = 0;
  int clientDisconnected = 0;
  int serverMessageReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  uint64_t requestId = 0;
  std::string serverName;
  std::string methodName;
  std::string payload;

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverMessageReceived;

        RpcMessageType messageType;
        bool res = decodeHead(msg, messageType);
        assert(res);
        assert(messageType == RpcMessageType::kRequest);

        RpcRequest request;
        std::string errorMessage;
        res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          std::cerr << __LINE__ << ": server decode error" << std::endl;
          std::abort();
          return;
        }

        if (serverMessageReceived == 1) {
          requestId = request.getRequestId();
          serverName = request.getService();
          methodName = request.getMethod();
          payload = request.getPayload();
        } else {
          assert(requestId == request.getRequestId());
          assert(serverName == request.getService());
          assert(methodName == request.getMethod());
          assert(payload == request.getPayload());
        }

        conn->forceClose();
      });

  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    ++serverStopped;
    stopCompletePromise.set_value();
    loop.quit();
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setDefaultResendLimit(1);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      if (connected != 1) {
        return;
      }

      struct RpcCallOptions options;
      options.allowResend = true;
      options.timeout = std::chrono::milliseconds(1000);
      options.reconnectTimeout = std::chrono::milliseconds(500);
      options.resendLimit = 2;

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "connect closed");
            ++unsuccessRequestCallbackCount;
            client.disconnect();
          },
          options);
    } else {
      ++clientDisconnected;

      if (clientDisconnected == 3) {
        loop.runAfter(std::chrono::milliseconds(1000),
                      [&]() { server.stop(std::chrono::milliseconds(0)); });
      }
    }
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 3);
  assert(serverMessageReceived == 3);
  assert(unsuccessRequestCallbackCount == 1);
  assert(clientDisconnected == 3);
  assert(serverStopped == 1);
}

void rpc_client_disables_resend_when_per_call_limit_is_zero_test() {
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
  int unsuccessRequestCallbackCount = 0;
  int clientDisconnected = 0;
  int serverMessageReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverMessageReceived;

        RpcMessageType messageType;
        bool res = decodeHead(msg, messageType);
        assert(res);
        assert(messageType == RpcMessageType::kRequest);

        RpcRequest request;
        std::string errorMessage;
        res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          std::cerr << __LINE__ << ": server decode error" << std::endl;
          std::abort();
          return;
        }

        conn->forceClose();
      });

  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    ++serverStopped;
    stopCompletePromise.set_value();
    loop.quit();
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setDefaultResendLimit(1);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      if (connected != 1) {
        return;
      }

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "connect closed");
            ++unsuccessRequestCallbackCount;
            client.disconnect();
          },
          std::chrono::milliseconds(1000), true, std::chrono::milliseconds(500),
          0);
    } else {
      ++clientDisconnected;

      loop.runAfter(std::chrono::milliseconds(1000),
                    [&]() { server.stop(std::chrono::milliseconds(0)); });
    }
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverMessageReceived == 1);
  assert(unsuccessRequestCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_returns_internal_error_for_invalid_async_handler_error_code_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int serverHandled = 0;
  int requestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        ++serverHandled;
        reply(
            {ResponseResult::kUnsuccess, RpcErrorCode::ERROR_CODE_CNT, "", ""});
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::INTERNAL_ERROR);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "invalid handler result");
            ++requestCallback;

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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 1);
  assert(requestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_executes_async_handler_on_task_pool_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int serverHandled = 0;
  int requestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  server.setWorkerThreadNum(4);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        assert(!loop.isInLoopThread());
        ++serverHandled;
        reply({ResponseResult::kSuccess, RpcErrorCode::NO_ERROR, payload, ""});
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getErrorCode() == RpcErrorCode::NO_ERROR);
                    assert(response.getPayload() == "hello world");
                    assert(response.getErrorMessage().empty());
                    ++requestCallback;

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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 1);
  assert(requestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_waits_for_running_task_before_stop_complete_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  std::promise<void> releaseHandlerPromise;
  auto releaseHandlerFuture = releaseHandlerPromise.get_future();

  int connected = 0;
  int serverHandled = 0;
  int requestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  bool handlerFinished = false;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  server.setWorkerThreadNum(4);
  server.setStopCompleteCallback([&]() {
    assert(handlerFinished);

    loop.queueInLoop([&]() {
      ++serverStopped;

      if (serverStopped == 1 && clientDisconnected == 1) {
        serverStopCompletePromise.set_value();
        loop.quit();
      }
    });
  });

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        assert(!loop.isInLoopThread());
        ++serverHandled;

        loop.queueInLoop([&]() {
          server.stop(std::chrono::milliseconds(0));
          releaseHandlerPromise.set_value();
        });

        auto waitForRes =
            releaseHandlerFuture.wait_for(std::chrono::milliseconds(2000));
        if (waitForRes != std::future_status::ready) {
          std::cerr << "wait for handler waitting timeout" << std::endl;
          std::abort();
        }

        handlerFinished = true;
        reply({ResponseResult::kSuccess, RpcErrorCode::NO_ERROR, payload, ""});
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::CONNECTION_CLOSED);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "connect closed");
            ++requestCallback;
          });
    } else {
      ++clientDisconnected;

      if (serverStopped == 1 && clientDisconnected == 1) {
        serverStopCompletePromise.set_value();
        loop.quit();
      }
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 1);
  assert(requestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
  assert(handlerFinished);
}

void rpc_server_processes_cancellation_while_task_pool_handler_is_running_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int serverHandled = 0;
  int requestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  uint64_t requestId = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  server.setWorkerThreadNum(4);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        assert(!loop.isInLoopThread());
        ++serverHandled;

        loop.queueInLoop([&]() { client.cancel(requestId); });

        auto start = std::chrono::steady_clock::now();

        while (!cancellation.isCanceled()) {
          if (std::chrono::steady_clock::now() - start >=
              std::chrono::milliseconds(2000)) {
            std::abort();
          }
          std::this_thread::yield();
        }

        loop.queueInLoop([&]() { client.disconnect(); });

        reply({ResponseResult::kSuccess, RpcErrorCode::NO_ERROR, payload, ""});
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      requestId = client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::REQUEST_CANCELED);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request canceled");
            ++requestCallback;
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 1);
  assert(requestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_returns_resource_exhausted_when_task_queue_is_full_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  std::promise<void> releaseFirstTask;
  auto releaseFirstTaskFuture = releaseFirstTask.get_future();

  int connected = 0;
  int serverHandled = 0;
  int firstRequestCallback = 0;
  int secondRequestCallback = 0;
  int thirdRequestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  server.setWorkerThreadNum(1);
  server.setMaxWorkerQueue(1);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        assert(!loop.isInLoopThread());
        ++serverHandled;

        if (serverHandled == 1) {
          loop.queueInLoop([&]() {
            client.call("EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getErrorCode() ==
                                 RpcErrorCode::NO_ERROR);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());
                          ++secondRequestCallback;

                          client.disconnect();
                        });

            client.call("EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          assert(response.getResponseResult() ==
                                 ResponseResult::kUnsuccess);
                          assert(response.getErrorCode() ==
                                 RpcErrorCode::RESOURCE_EXHAUSTED);
                          assert(response.getPayload().empty());
                          assert(response.getErrorMessage() ==
                                 "task queue full");
                          ++thirdRequestCallback;
                          releaseFirstTask.set_value();
                        });
          });

          auto waitForRes =
              releaseFirstTaskFuture.wait_for(std::chrono::milliseconds(1000));
          if (waitForRes != std::future_status::ready) {
            std::abort();
          }
        }

        reply({ResponseResult::kSuccess, RpcErrorCode::NO_ERROR, payload, ""});
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getErrorCode() == RpcErrorCode::NO_ERROR);
                    assert(response.getPayload() == "hello world");
                    assert(response.getErrorMessage().empty());
                    ++firstRequestCallback;
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 2);
  assert(firstRequestCallback == 1);
  assert(secondRequestCallback == 1);
  assert(thirdRequestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_allows_pending_async_response_during_graceful_stop_test() {

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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  std::promise<void> releaseFirstTask;
  auto releaseFirstTaskFuture = releaseFirstTask.get_future();

  int connected = 0;
  int serverHandled = 0;
  int firstRequestCallback = 0;
  int secondRequestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  server.setWorkerThreadNum(2);
  server.setMaxWorkerQueue(1);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        assert(!loop.isInLoopThread());
        ++serverHandled;

        if (serverHandled == 1) {
          loop.queueInLoop([&]() {
            server.stop(std::chrono::milliseconds(5000));
            client.call("EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          assert(response.getResponseResult() ==
                                 ResponseResult::kUnsuccess);
                          assert(response.getErrorCode() ==
                                 RpcErrorCode::INTERNAL_ERROR);
                          assert(response.getPayload().empty());
                          assert(response.getErrorMessage() ==
                                 "server stopping");
                          ++secondRequestCallback;

                          releaseFirstTask.set_value();
                        });
          });

          auto waitForRes =
              releaseFirstTaskFuture.wait_for(std::chrono::milliseconds(2000));
          if (waitForRes != std::future_status::ready) {
            std::cerr << "wait for second request complete timeout"
                      << std::endl;
            std::abort();
          }

          reply(
              {ResponseResult::kSuccess, RpcErrorCode::NO_ERROR, payload, ""});
        } else {
          std::abort();
        }
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getErrorCode() == RpcErrorCode::NO_ERROR);
                    assert(response.getPayload() == "hello world");
                    assert(response.getErrorMessage().empty());
                    ++firstRequestCallback;
                  });
    } else {
      ++clientDisconnected;
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 1);
  assert(firstRequestCallback == 1);
  assert(secondRequestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_cancels_pending_request_and_closes_connection_at_grace_deadline_test() {

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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  std::promise<void> releaseFirstTask;
  auto releaseFirstTaskFuture = releaseFirstTask.get_future();

  int connected = 0;
  int serverHandled = 0;
  int requestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  std::optional<RpcCancellationToken> savedToken;
  std::chrono::steady_clock::time_point start;

  server.setWorkerThreadNum(2);
  server.setMaxWorkerQueue(1);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      if (clientDisconnected) {
        serverStopCompletePromise.set_value();
        loop.quit();
      }
    });
  });

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        assert(!loop.isInLoopThread());
        ++serverHandled;

        savedToken = cancellation;

        loop.queueInLoop([&]() {
          start = std::chrono::steady_clock::now();
          server.stop(std::chrono::milliseconds(1000));
          assert(!savedToken->isCanceled());
        });
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::CONNECTION_CLOSED);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "connect closed");
            ++requestCallback;
          });
    } else {
      ++clientDisconnected;
      assert((std::chrono::steady_clock::now() - start) >
             std::chrono::milliseconds(1000));
      assert(savedToken->isCanceled());
      if (serverStopped) {
        serverStopCompletePromise.set_value();
        loop.quit();
      }
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 1);
  assert(requestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_times_out_async_request_when_handler_does_not_reply_test() {

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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int serverHandled = 0;
  int requestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  std::optional<RpcCancellationToken> savedToken;

  server.setAsyncHandlerTimeout(std::chrono::milliseconds(500));

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        assert(loop.isInLoopThread());
        ++serverHandled;
        savedToken = cancellation;
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::REQUEST_TIMEOUT);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "handler running timeout");
            ++requestCallback;

            assert(savedToken.has_value());
            assert(savedToken->isCanceled());

            client.disconnect();
          },
          std::chrono::milliseconds(0));
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 1);
  assert(requestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_allows_request_id_reuse_after_overlapping_requests_time_out_test() {

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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int serverHandled = 0;
  int requestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  LengthHeaderCodec codec;
  TcpClient client(&loop, serverAddr);

  TcpConnectionPtr clientConn;
  std::optional<RpcCancellationToken> savedToken;

  server.setAsyncHandlerTimeout(std::chrono::milliseconds(500));

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        assert(loop.isInLoopThread());
        ++serverHandled;

        if (serverHandled == 1) {
          savedToken = cancellation;

          loop.queueInLoop([&]() {
            uint64_t requestId = 2;
            std::string service = "EchoService";
            std::string method = "Echo";
            std::string payload = "hello world";

            RpcRequest request(requestId, service, method, payload);
            std::string output;
            std::string errorMsg;
            bool res = request.encode(output, errorMsg);
            if (!res || !errorMsg.empty()) {
              std::abort();
            }

            codec.send(clientConn, output.data(), output.length());
          });

        } else if (serverHandled == 2) {
          assert(savedToken.has_value());
          assert(!savedToken->isCanceled());
        } else if (serverHandled == 3) {
          reply({ResponseResult::kSuccess, payload, ""});
        } else {
          std::abort();
        }
      });
  assert(registered);

  server.start();

  client.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        RpcResponse response;
        std::string errorMsg;
        bool res = response.decode(msg, errorMsg);
        if (!res || !errorMsg.empty()) {
          std::abort();
          return;
        }
        ++requestCallback;

        if (response.getRequestId() == 1) {
          if (requestCallback == 1) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::REQUEST_TIMEOUT);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "handler running timeout");
          } else if (requestCallback == 3) {
            assert(response.getResponseResult() == ResponseResult::kSuccess);
            assert(response.getErrorCode() == RpcErrorCode::NO_ERROR);
            assert(response.getPayload() == "hello world");
            assert(response.getErrorMessage().empty());

            client.disconnect();
          } else {
            std::abort();
          }
        } else if (response.getRequestId() == 2) {
          assert(requestCallback == 2);

          assert(response.getResponseResult() == ResponseResult::kUnsuccess);
          assert(response.getErrorCode() == RpcErrorCode::REQUEST_TIMEOUT);
          assert(response.getPayload().empty());
          assert(response.getErrorMessage() == "handler running timeout");

          uint64_t requestId = 1;
          std::string service = "EchoService";
          std::string method = "Echo";
          std::string payload = "hello world";

          RpcRequest request(requestId, service, method, payload);
          std::string output;
          std::string errorMsg;
          bool res = request.encode(output, errorMsg);
          if (!res || !errorMsg.empty()) {
            std::abort();
          }

          codec.send(clientConn, output.data(), output.length());
        } else {
          std::abort();
        }
      });

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      clientConn = conn;

      uint64_t requestId = 1;
      std::string service = "EchoService";
      std::string method = "Echo";
      std::string payload = "hello world";

      RpcRequest request(requestId, service, method, payload);
      std::string output;
      std::string errorMsg;
      bool res = request.encode(output, errorMsg);
      if (!res || !errorMsg.empty()) {
        std::abort();
      }

      codec.send(conn, output.data(), output.length());
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });

  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 3);
  assert(requestCallback == 3);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_completes_graceful_stop_when_last_async_request_times_out_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int serverHandled = 0;
  int requestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  std::chrono::steady_clock::time_point start;

  std::optional<RpcCancellationToken> savedToken;

  server.setAsyncHandlerTimeout(std::chrono::milliseconds(300));

  server.setStopCompleteCallback([&]() {
    assert(std::chrono::steady_clock::now() - start <
           std::chrono::milliseconds(2000));
    loop.queueInLoop([&]() {
      ++serverStopped;
      serverStopCompletePromise.set_value();
      loop.quit();
    });
  });

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        assert(loop.isInLoopThread());
        ++serverHandled;
        savedToken = cancellation;
        loop.queueInLoop([&]() {
          start = std::chrono::steady_clock::now();
          server.stop(std::chrono::milliseconds(2000));
          assert(!savedToken->isCanceled());
        });
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::REQUEST_TIMEOUT);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "handler running timeout");
            ++requestCallback;
          },
          std::chrono::milliseconds(0));
    } else {
      ++clientDisconnected;
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 1);
  assert(requestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_applies_method_timeout_without_global_timeout_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int serverHandled = 0;
  int requestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  server.setStopCompleteCallback([&]() {
    ++serverStopped;
    serverStopCompletePromise.set_value();
    loop.quit();
  });

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        assert(loop.isInLoopThread());
        ++serverHandled;
      },
      std::chrono::milliseconds(500));
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getErrorCode() == RpcErrorCode::REQUEST_TIMEOUT);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "handler running timeout");
            ++requestCallback;

            client.disconnect();
          },
          std::chrono::milliseconds(0));
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(1000));
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 1);
  assert(requestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_prefers_method_timeout_over_global_timeout_test() {
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

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  int connected = 0;
  int serverHandled = 0;
  int requestCallback = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  server.setAsyncHandlerTimeout(std::chrono::milliseconds(200));

  server.setStopCompleteCallback([&]() {
    ++serverStopped;
    serverStopCompletePromise.set_value();
    loop.quit();
  });

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        assert(loop.isInLoopThread());
        ++serverHandled;
        loop.runAfter(std::chrono::milliseconds(250), [reply, msg = payload]() {
          reply(RpcServer::RpcResult{ResponseResult::kSuccess, msg, ""});
        });
      },
      std::chrono::milliseconds(500));
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kSuccess);
            assert(response.getErrorCode() == RpcErrorCode::NO_ERROR);
            assert(response.getPayload() == "hello world");
            assert(response.getErrorMessage().empty());
            ++requestCallback;

            client.disconnect();
          },
          std::chrono::milliseconds(0));
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(1000));
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
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverHandled == 1);
  assert(requestCallback == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

int main() {
  rpc_server_processes_echo_request_test();
  rpc_client_calls_echo_service_test();
  rpc_client_receives_error_for_unknown_service_test();
  rpc_client_reports_error_when_not_connected_test();
  rpc_client_reports_error_when_connection_closes_with_pending_request_test();
  rpc_server_rejects_duplicate_method_registration_test();
  rpc_client_receives_error_for_invalid_handler_result_test();
  rpc_client_matches_out_of_order_responses_by_request_id_test();
  rpc_client_closes_connection_for_unknown_response_request_id_test();
  rpc_client_receives_business_error_from_registered_handler_test();
  rpc_client_reports_timeout_when_server_does_not_respond_test();
  rpc_client_does_not_timeout_when_timeout_is_zero_test();
  rpc_server_executes_handler_on_io_loop_test();
  rpc_server_distributes_connections_across_io_loops_test();
  rpc_client_allows_call_from_non_loop_thread_test();
  rpc_server_returns_error_and_remains_available_when_handler_throws_test();
  rpc_client_ignores_late_response_after_request_timeout_test();
  rpc_client_reconnects_after_connection_closes_and_allows_new_call_test();
  rpc_client_reconnects_after_initial_connection_failure_test();
  rpc_client_disabling_retry_prevents_reconnection_test();
  rpc_client_rejects_call_when_max_pending_requests_is_reached_test();
  rpc_client_releases_pending_capacity_before_response_callback_test();
  rpc_client_releases_pending_capacity_after_request_timeout_test();
  rpc_client_uses_default_timeout_and_allows_explicit_zero_override_test();
  rpc_client_ignores_late_response_after_request_cancellation_test();
  rpc_server_returns_error_for_unregistered_method_test();
  rpc_server_keeps_other_methods_after_unregistering_one_method_test();
  rpc_client_allows_request_cancellation_from_non_loop_thread_test();
  rpc_client_releases_pending_capacity_after_request_cancellation_test();
  rpc_client_allows_calls_from_multiple_non_loop_threads_test();
  rpc_server_closes_connection_for_malformed_request_test();
  rpc_client_closes_connection_for_malformed_response_test();
  rpc_server_sends_delayed_response_from_async_handler_test();
  rpc_server_ignores_duplicate_reply_from_async_handler_test();
  rpc_server_returns_error_when_async_handler_throws_test();
  rpc_server_returns_error_for_invalid_async_handler_result_test();
  rpc_server_sends_response_when_async_reply_is_called_from_another_thread_test();
  rpc_server_rejects_duplicate_method_registration_across_sync_and_async_handlers_test();
  rpc_server_handles_async_reply_after_client_disconnects_test();
  rpc_server_safely_handles_async_reply_after_server_destruction_test();
  rpc_client_sends_cancel_message_for_pending_request_test();
  rpc_server_cancels_async_request_before_late_reply_test();
  rpc_server_drops_async_reply_after_request_cancellation_test();
  rpc_server_closes_connection_for_duplicate_async_request_id_test();
  rpc_server_closes_connection_for_malformed_cancel_test();
  rpc_server_ignores_cancel_for_unknown_request_id_test();
  rpc_server_cancels_async_request_after_client_timeout_test();
  rpc_client_does_not_send_cancel_after_response_before_timeout_test();
  rpc_server_rejects_async_request_when_active_request_limit_is_reached_test();
  rpc_server_rejects_async_request_when_global_active_request_limit_is_reached_test();
  rpc_server_releases_active_request_capacity_after_cancellation_test();
  rpc_server_closes_connection_for_unsupported_protocol_version_test();
  rpc_client_closes_connection_for_unsupported_protocol_version_test();
  rpc_server_closes_connection_for_unexpected_response_message_test();
  rpc_client_closes_connection_for_unexpected_request_message_test();
  rpc_client_rejects_oversized_request_before_sending_test();
  rpc_server_returns_error_for_oversized_response_and_remains_available_test();
  rpc_server_returns_error_for_oversized_sync_response_and_remains_available_test();
  rpc_server_closes_connection_for_request_exceeding_message_size_limit_test();
  rpc_client_closes_connection_for_response_exceeding_message_size_limit_test();
  rpc_client_resends_pending_request_after_reconnect_test();
  rpc_client_fails_pending_request_when_reconnect_wait_expires_test();
  rpc_client_fails_request_when_resend_budget_is_exhausted_test();
  rpc_client_resends_only_opted_in_requests_after_disconnect_test();
  rpc_client_does_not_resend_request_canceled_while_waiting_for_reconnect_test();
  rpc_client_fails_waiting_requests_on_manual_disconnect_test();
  rpc_client_restarts_response_timeout_after_resend_test();
  rpc_client_times_out_waiting_for_response_after_resend_test();
  rpc_client_respects_configured_resend_limit_test();
  rpc_client_uses_per_call_resend_limit_over_default_test();
  rpc_client_disables_resend_when_per_call_limit_is_zero_test();
  rpc_server_returns_internal_error_for_invalid_async_handler_error_code_test();
  rpc_server_executes_async_handler_on_task_pool_test();
  rpc_server_waits_for_running_task_before_stop_complete_test();
  rpc_server_processes_cancellation_while_task_pool_handler_is_running_test();
  rpc_server_returns_resource_exhausted_when_task_queue_is_full_test();
  rpc_server_allows_pending_async_response_during_graceful_stop_test();
  rpc_server_cancels_pending_request_and_closes_connection_at_grace_deadline_test();
  rpc_server_times_out_async_request_when_handler_does_not_reply_test();
  rpc_server_allows_request_id_reuse_after_overlapping_requests_time_out_test();
  rpc_server_completes_graceful_stop_when_last_async_request_times_out_test();
  rpc_server_applies_method_timeout_without_global_timeout_test();
  rpc_server_prefers_method_timeout_over_global_timeout_test();
}