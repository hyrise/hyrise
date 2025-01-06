#pragma once

#include <memory>
#include <string>

#include "storage/encoding_type.hpp"

namespace hyrise {

class EncodingConfig;
class Table;

class BenchmarkTableEncoder {
 public:
  static ChunkEncodingSpec get_required_chunk_encoding_spec(const std::string& table_name, const std::shared_ptr<Table>& table,
                                          const EncodingConfig& encoding_config);

  // @return      true, if any encoding operation was performed.
  //              false, if the @param table was already encoded as required by @param encoding_config
  static bool encode(const std::string& table_name, const std::shared_ptr<Table>& table,
                     const EncodingConfig& encoding_config);
};

}  // namespace hyrise
