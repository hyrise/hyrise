#pragma once

#include <iostream>
#include <memory>
#include <string>

namespace opossum {

class EncodingConfig;
class Table;

class BenchmarkTableEncoder {
 public:
  BenchmarkTableEncoder(const std::string table_name, const EncodingConfig& encoding_config);
  bool encode(const std::shared_ptr<Table>& table, std::ostream& out = std::cout);
 private:
  const std::string _table_name;
  const EncodingConfig& _encoding_config;
};

}  // namespace opossum
