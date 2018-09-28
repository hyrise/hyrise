#pragma once

#include <functional>
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "storage/storage_manager.hpp"
#include "benchmark_utils.hpp"
#include "types.hpp"

namespace opossum {

class AbstractBenchmarkTableGenerator {
 public:
  explicit AbstractBenchmarkTableGenerator(const ChunkOffset chunk_size, EncodingConfig encoding_config, bool store)
    : _chunk_size(chunk_size), _encoding_config(std::move(encoding_config)), _store(store) {}
  virtual ~AbstractBenchmarkTableGenerator() = default;

  virtual std::map<std::string, std::shared_ptr<opossum::Table>> generate_all_tables() = 0;


 protected:
  const ChunkOffset _chunk_size;
  const EncodingConfig _encoding_config;
  const bool _store;
};

}  // namespace opossum
