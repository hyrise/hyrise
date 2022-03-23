#pragma once

#include <iostream>
#include <memory>
#include <string>

#include "types.hpp"

namespace opossum {

class EncodingConfig;
class Table;

class BenchmarkTableEncoder {
 public:
  static bool encode(const EncodingConfig& encoding_config, const std::string table_name, const std::shared_ptr<Table>& table);
  static bool encode(const EncodingConfig& encoding_config, const std::string table_name, const std::shared_ptr<Table>& table, const ChunkID chunk_id);
};

}  // namespace opossum
