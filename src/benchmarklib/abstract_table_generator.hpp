#pragma once

#include "types.hpp"
#include "storage/chunk.hpp"
#include "encoding_config.hpp"

namespace opossum {

class BenchmarkConfig;

class AbstractTableGenerator {
 public:
  enum class BinaryFileCaching { Yes, No };

  explicit AbstractTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config);
  virtual ~AbstractTableGenerator() = default;

  void generate_and_store();

 protected:
  virtual void _generate() = 0;

  const std::shared_ptr<BenchmarkConfig> _benchmark_config;
};

}
