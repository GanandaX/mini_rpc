#include "RpcMessage.h"

#include <cstdlib>
#include <stddef.h>
#include <stdint.h>
#include <string>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  if (size > 1024) {
    return 0;
  }

  std::string message;
  if (size != 0) {
    message.assign(reinterpret_cast<const char *>(data), size);
  }

  RpcRequest request(42, "old-service", "old-method", "old-payload");
  std::string error;
  bool res = request.decode(message, error);
  if (!res) {
    if (request.getRequestId() != 42 || request.getService() != "old-service" ||
        request.getMethod() != "old-method" ||
        request.getPayload() != "old-payload") {
      std::abort();
    }
  }
  return 0;
}
