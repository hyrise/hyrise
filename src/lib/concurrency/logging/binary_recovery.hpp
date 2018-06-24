#pragma once

#include "abstract_recovery.hpp"

namespace opossum {

class BinaryRecovery : public AbstractRecovery {
 public:
  BinaryRecovery(const BinaryRecovery&) = delete;
  BinaryRecovery& operator=(const BinaryRecovery&) = delete;

  static BinaryRecovery& getInstance();

  void recover() override ;

 private:
  BinaryRecovery(){}
};

}  // namespace opossum
