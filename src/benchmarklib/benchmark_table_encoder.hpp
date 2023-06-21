#pragma once

#include <memory>
#include <string>
#include <mutex>

namespace hyrise {

class EncodingConfig;
class Table;

class BenchmarkTableEncoder {
 public:
  // @return      true, if any encoding operation was performed.
  //              false, if the @param table was already encoded as required by @param encoding_config
  static bool encode(const std::string& table_name, const std::shared_ptr<Table>& table,
                     const EncodingConfig& encoding_config);
 private:
  // Used to synchronize output messages when using multiple threads.
  static std::mutex output_stream_mutex;
};

}  // namespace hyrise
