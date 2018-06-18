#pragma once

#include <fstream>

#include "types.hpp"

namespace opossum {

class BinaryRecovery {
 public:
  BinaryRecovery(const BinaryRecovery&) = delete;
  BinaryRecovery& operator=(const BinaryRecovery&) = delete;

  static BinaryRecovery& getInstance();

  void recover();

 private:
  BinaryRecovery(){}
};

}  // namespace opossum
