#pragma once

#include <cstdint>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "storage/table.hpp"
#include "storage/base_column_encoder.hpp"
#include "storage/column_encoding_utils.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/value_column/value_column_iterable.hpp"

#include "abstract_column_benchmark.hpp"

namespace opossum {

class SingleDistributionBenchmark : public AbstractColumnBenchmark {
 public:
  static const auto row_count = 1'000'000;
  static const auto max_value = (1u << 5) - 3u;
  static const auto sorted = true;
  static constexpr auto null_fraction = 0.0f;
  static constexpr auto name = "Uniform";
  static constexpr auto unique_count_exp = "5";
  static constexpr auto comment = "Memory Usage";

 private:
  std::vector<ColumnEncodingSpec> _encoding_specs() {
    return { {EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned}, {EncodingType::Dictionary, VectorCompressionType::SimdBp128}, {EncodingType::FrameOfReference, VectorCompressionType::FixedSizeByteAligned}, {EncodingType::FrameOfReference, VectorCompressionType::SimdBp128}, {EncodingType::RunLength} };
  }

  void _generate_statistics(const ValueColumn<int32_t>& value_column);
  void _create_report() const;

 public:
  void run() final;

 private:
  struct MeasurementResultSet{
    ColumnEncodingSpec encoding_spec;
    size_t num_iterations;
    size_t allocated_memory;
    std::vector<double> results_in_mis;
  };

  uint32_t _run_count;
  std::vector<MeasurementResultSet> _result_sets;
};

}  // namespace opossum
