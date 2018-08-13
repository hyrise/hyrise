#pragma once

#include <fstream>

#include "abstract_recovery.hpp"

namespace opossum {

class BinaryRecovery : public AbstractRecovery {
 public:
  BinaryRecovery(const BinaryRecovery&) = delete;
  BinaryRecovery& operator=(const BinaryRecovery&) = delete;

  static BinaryRecovery& getInstance();

  void recover() override;

 private:
  BinaryRecovery() {}

  AllTypeVariant _read_AllTypeVariant(std::ifstream& file, DataType data_type);

  template <typename T>
  T _read(std::ifstream& file) {
    T result;
    file.read(reinterpret_cast<char*>(&result), sizeof(T));
    return result;
  }
};

}  // namespace opossum
