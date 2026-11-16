#ifndef CALCULATOR_PROTOCOL_H
#define CALCULATOR_PROTOCOL_H

#include <cstdint>
#include <string>

class CalculatorProtocol {
public:
  explicit CalculatorProtocol();
  explicit CalculatorProtocol(int32_t a, int32_t b);

  void setA(int32_t a);
  void setB(int32_t b);
  void setResult(int32_t result);

  int32_t getA() const;
  int32_t getB() const;
  int32_t getResult() const;

  std::string requestEncode() const;
  bool requestDecode(const std::string &msg, std::string &errorMessage);

  std::string resultEncode() const;
  bool resultDecode(const std::string &msg, std::string &errorMessage);

  static std::string encodeDivModResult(int32_t quotient, int32_t remainder);

  static bool decodeDivModResult(const std::string &payload, int32_t &quotient,
                                 int32_t &remainder, std::string &errorMessage);

private:
  static std::string encodeInt32Pair(int32_t first, int32_t second);

private:
  int32_t a_;
  int32_t b_;

  int32_t result_;
};

#endif