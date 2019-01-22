#pragma once

#include <memory>
#include <string>

namespace opossum {

class EncodingConfig;
class Table;

class BenchmarkTableEncoder {
 public:
  // @param out   stream for logging info
  // @return      true, if any encoding operation was performed.
  //              false, if the @param table was already encoded as required by @param encoding_config
  static bool encode(const std::string& table_name, const std::shared_ptr<Table>& table,
                     const EncodingConfig& encoding_config);
};

}  // namespace opossum
