#include "micro_benchmark_basic_fixture.hpp"

#include <memory>

#include "benchmark/benchmark.h"
#include "hyrise.hpp"
#include "operators/table_wrapper.hpp"
#include "synthetic_table_generator.hpp"
#include "types.hpp"

namespace hyrise {

const auto MB = uint32_t{1'000'000};

void MicroBenchmarkBasicFixture::SetUp(::benchmark::State& /*state*/) {
  const auto chunk_size = ChunkOffset{2'000};
  const auto row_count = size_t{40'000};

  const auto table_generator = std::make_shared<SyntheticTableGenerator>();

  _table_wrapper_a = std::make_shared<TableWrapper>(table_generator->generate_table(2ul, row_count, chunk_size));
  _table_wrapper_b = std::make_shared<TableWrapper>(table_generator->generate_table(2ul, row_count, chunk_size));
  _table_dict_wrapper = std::make_shared<TableWrapper>(
      table_generator->generate_table(2ul, row_count, chunk_size, SegmentEncodingSpec{EncodingType::Dictionary}));

  _table_wrapper_a->never_clear_output();
  _table_wrapper_b->never_clear_output();
  _table_dict_wrapper->never_clear_output();

  _table_wrapper_a->execute();
  _table_wrapper_b->execute();
  _table_dict_wrapper->execute();
}

void MicroBenchmarkBasicFixture::TearDown(::benchmark::State& /*state*/) {
  Hyrise::reset();
}

void MicroBenchmarkBasicFixture::_clear_cache() {
  micro_benchmark_clear_cache();
}

uint32_t MicroBenchmarkBasicFixture::_align_to_pagesize(const uint32_t buffer_size_mb, const uint32_t page_size) {
  auto buffer_size = buffer_size_mb * MB;
  const auto multiplier = static_cast<uint32_t>(std::ceil(static_cast<float>(buffer_size) / static_cast<float>(page_size)));
  return page_size * multiplier;
}

}  // namespace hyrise
