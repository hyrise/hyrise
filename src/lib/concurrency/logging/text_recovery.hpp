#pragma once

#include "types.hpp"

namespace opossum {

class TextRecovery {
 public:
  TextRecovery(const TextRecovery&) = delete;
  TextRecovery& operator=(const TextRecovery&) = delete;

  static TextRecovery& getInstance();

  void recover();

 private:
  TextRecovery(){}

  std::string _get_substr_and_incr_begin(const std::string &line, size_t& begin, const char delimiter);
};

}  // namespace opossum
