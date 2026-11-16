#include "CalculatorService.h"
#include "CalculatorProtocol.h"
#include "RpcServer.h"

bool CalculatorService::registerCalculatorService(RpcServer &server) {
  bool res = server.registerMethod(
      "Calculator", "Add",
      [](const std::string &payload) -> RpcServer::RpcResult {
        std::string errorMessage;
        CalculatorProtocol calculatorAdd;
        bool res = calculatorAdd.requestDecode(payload, errorMessage);

        if (!res || !errorMessage.empty()) {
          return RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                      "invalid add request"};
        }

        int64_t result =
            static_cast<int64_t>(calculatorAdd.getA()) + calculatorAdd.getB();
        if (result > INT32_MAX || result < INT32_MIN) {
          return RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                      "integer overflow"};
        }

        calculatorAdd.setResult(result);
        std::string msg = calculatorAdd.resultEncode();
        return RpcServer::RpcResult{ResponseResult::kSuccess, msg, ""};
      });

  if (!res) {
    return false;
  }

  res = server.registerMethod(
      "Calculator", "DivMod",
      [](const std::string &payload) -> RpcServer::RpcResult {
        std::string errorMessage;
        CalculatorProtocol protocol;
        bool res = protocol.requestDecode(payload, errorMessage);

        if (!res || !errorMessage.empty()) {
          return RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                      "invalid div_mod request"};
        }

        if (protocol.getA() == INT32_MIN && protocol.getB() == -1) {
          return RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                      "integer overflow"};
        }

        if (protocol.getB() == 0) {
          return RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                      "divisor equals 0"};
        }

        int32_t quotient = protocol.getA() / protocol.getB();
        int32_t remainder = protocol.getA() % protocol.getB();

        std::string msg = protocol.encodeDivModResult(quotient, remainder);
        return RpcServer::RpcResult{ResponseResult::kSuccess, msg, ""};
      });

  if (!res) {
    server.unregisterMethod("Calculator", "Add");
    return false;
  }

  return true;
}