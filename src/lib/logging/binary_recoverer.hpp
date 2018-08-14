#pragma once

#include <fstream>

#include "abstract_recoverer.hpp"

namespace opossum {

class BinaryRecoverer : public AbstractRecoverer {
 public:
  BinaryRecoverer(const BinaryRecoverer&) = delete;
  BinaryRecoverer& operator=(const BinaryRecoverer&) = delete;

  static BinaryRecoverer& getInstance();

  void recover() override;

 private:
  BinaryRecoverer() {}

  AllTypeVariant _read_AllTypeVariant(std::ifstream& file, DataType data_type);

  template <typename T>
  T _read(std::ifstream& file) {
    T result;
    file.read(reinterpret_cast<char*>(&result), sizeof(T));
    return result;
  }
};

}  // namespace opossum
