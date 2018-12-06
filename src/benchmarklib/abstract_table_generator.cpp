#include "abstract_table_generator.hpp"

namespace opossum {

AbstractTableGenerator::AbstractTableGenerator(const std::shared_ptr<BenchmarkConfig>& benchmark_config):
  _benchmark_config(benchmark_config) {}

void AbstractTableGenerator::generate_and_store() {
  _generate();
}

}  // namespace opossum
