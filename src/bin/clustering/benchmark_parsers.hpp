#pragma once

#include <memory>

#include "benchmark_runner.hpp"

namespace opossum {
  std::shared_ptr<BenchmarkRunner> parse_tpcds_args(int argc, char* argv[]);
  std::shared_ptr<BenchmarkRunner> parse_tpch_args(int argc, char* argv[]);
  std::shared_ptr<BenchmarkRunner> parse_job_args(int argc, char* argv[]);
} // namespace opossum

