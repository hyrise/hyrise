#pragma once

#include "types.hpp"

#include <fstream>

namespace opossum {

class BinaryRecovery {
 public:   
  BinaryRecovery(const BinaryRecovery&) = delete;
  BinaryRecovery& operator=(const BinaryRecovery&) = delete;

  static BinaryRecovery& getInstance();

  void recover();

 private:
  BinaryRecovery(){};

  // bool _is_empty(std::ifstream& file);
};

}  // namespace opossum