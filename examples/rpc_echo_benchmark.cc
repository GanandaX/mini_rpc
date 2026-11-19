#include <InetAddress.h>
#include <RpcClient.h>

#include <cmath>
#include <stdlib.h>
#include <cerrno>

void clientCall(
    RpcClient &client, std::string &service, std::string &method,
    std::string &payload,
    std::function<bool(const RpcResponse &response)> &callbackFunc) {

  client.call(service, method, payload, [&](const RpcResponse &response) {
    if (callbackFunc(response)) {
      clientCall(client, service, method, payload, callbackFunc);
    } else {
      client.disconnect();
    }
  });
}

void printResult(std::chrono::steady_clock::time_point start,
                 uint64_t receivedFrameCnt, size_t payloadSize) {
  int64_t elapsedMilliseconds =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - start)
          .count();
  std::cout << "received frame count: " << receivedFrameCnt << std::endl
            << "elapsed: " << elapsedMilliseconds << " milliseconds"
            << std::endl
            << "task/second: "
            << receivedFrameCnt / (elapsedMilliseconds / 1000.0) << std::endl
            << "payload size: " << payloadSize << std::endl;
}

int main(int argc, char *argv[]) {
  if (argc != 7) {
    std::cerr << "usage: ./rpc_echo_benchmark server_ip server_port service "
                 "method payload_length test_seconds"
              << std::endl;
    return -1;
  }

  char *endptr = nullptr;
  errno = 0;
  long parsedPort = strtol(argv[2], &endptr, 10);
  if (errno == ERANGE || endptr == argv[2] || *endptr != '\0') {
    std::cout << "trans error" << std::endl;
    return -1;
  }
  if (parsedPort < 1 || parsedPort > 65535) {
    std::cerr << "port overlimit" << std::endl;
    return -1;
  }

  endptr = nullptr;
  errno = 0;
  if (argv[5][0] == '-') {
    std::cout << "payload length error" << std::endl;
    return -1;
  }
  uint64_t parsedPayloadLength = strtoll(argv[5], &endptr, 10);
  if (errno == ERANGE || endptr == argv[5] || *endptr != '\0') {
    std::cout << "trans error" << std::endl;
    return -1;
  }
  if (parsedPayloadLength > (64 << 10)) {
    std::cerr << "payload length overlimit" << std::endl;
    return -1;
  }

  endptr = nullptr;
  errno = 0;
  double parsedTestSeconds = strtod(argv[6], &endptr);
  if (errno == ERANGE || endptr == argv[6] || *endptr != '\0') {
    std::cout << "trans error" << std::endl;
    return -1;
  }
  if (!std::isfinite(parsedTestSeconds)) {
    std::cerr << "test time infinite " << std::endl;
    return -1;
  }
  if (parsedTestSeconds * 1000 < 1 || parsedTestSeconds > 3600) {
    std::cerr << "time overlimit" << std::endl;
    return -1;
  }

  std::string serverIp(argv[1]);
  int serverPort = parsedPort;
  std::string service(argv[3]);
  std::string method(argv[4]);
  uint64_t payloadLength = parsedPayloadLength;
  double testSeconds = parsedTestSeconds;

  auto serverAddres = InetAddress(serverIp.data(), serverPort);

  EventLoop loop;
  RpcClient client(&loop, serverAddres);

  bool measurementFinished = false;
  uint64_t receivedFrameCnt = 0;
  std::chrono::steady_clock::time_point start;
  std::string payload(payloadLength, 'A');
  size_t payloadSize = payload.size();

  std::function<bool(const RpcResponse &)> callbackFunc =
      [&](const RpcResponse &response) mutable -> bool {
    bool validatePass = true;

    if (response.getResponseResult() != ResponseResult::kSuccess ||
        response.getErrorCode() != RpcErrorCode::NO_ERROR) {
      validatePass = false;
    }

    if (response.getPayload() != payload) {
      validatePass = false;
    }

    if (!validatePass) {
      std::cerr << "fail: " << response.getErrorMessage() << std::endl;
      printResult(start, receivedFrameCnt, payloadSize);
      std::abort();
    }

    if (measurementFinished) {
      return false;
    }
    ++receivedFrameCnt;
    return true;
  };

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      start = std::chrono::steady_clock::now();

      loop.runAfter(std::chrono::milliseconds((uint64_t)(testSeconds * 1000)),
                    [&]() {
                      measurementFinished = true;
                      printResult(start, receivedFrameCnt, payloadSize);
                    });

      clientCall(client, service, method, payload, callbackFunc);
    } else {
      loop.quit();
    }
  });
  client.setConnectionErrorCallback([&](int errorCode) {
    std::cerr << "connect error: " << errorCode << std::endl;
    std::abort();
  });
  client.setDefaultTimeout(std::chrono::milliseconds(500));
  client.connect();

  loop.loop();
  return 0;
}