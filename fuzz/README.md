# RpcRequest 解码模糊测试

这个入口把 libFuzzer 生成的字节交给 `RpcRequest::decode()`。
如果解码失败，还会检查请求对象原有的四个字段是否保持不变。

在 `mini_rpc` 根目录构建：

```bash
mkdir -p build-fuzz/corpus
clang++-10 -std=c++17 -fsanitize=fuzzer,address \
  -Isrc/include \
  fuzz/rpc_request_decode_fuzz.cc src/RpcMessage.cc \
  -o build-fuzz/rpc_request_decode_fuzz
```

复制一条合法请求作为初始输入，再运行 10 秒：

```bash
cp fuzz/corpus/valid_request build-fuzz/corpus/
./build-fuzz/rpc_request_decode_fuzz build-fuzz/corpus \
  -max_total_time=10 -max_len=1024 > build-fuzz/fuzz.log 2>&1
echo $?
tail -n 25 build-fuzz/fuzz.log
```

`build-fuzz/` 不纳入 Git，运行中新生成的输入留在这里。日志中的 `left len error` 是解码器遇到不完整字段时的提示；`DONE` 表示本轮运行结束，不代表所有输入都已覆盖。若出现崩溃，应保留工具生成的输入文件，用它复现并定位问题。