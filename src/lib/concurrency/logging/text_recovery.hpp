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
  TextRecovery(){};
};

}  // namespace opossum