#pragma once

#include <fstream>

#include "abstract_recoverer.hpp"

namespace opossum {

class BinaryRecoverer : public AbstractRecoverer {
 public:
  BinaryRecoverer(const BinaryRecoverer&) = delete;
  BinaryRecoverer& operator=(const BinaryRecoverer&) = delete;

  static BinaryRecoverer& get();

  // Recovers db from logfiles and returns the number of loaded tables
  uint32_t recover() override;

 private:
  BinaryRecoverer() {}

  AllTypeVariant _read_all_type_variant(std::ifstream& file, DataType data_type);

  template <typename T>
  T _read(std::ifstream& file);
};

}  // namespace opossum
