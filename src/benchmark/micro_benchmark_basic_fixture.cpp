#include "micro_benchmark_basic_fixture.hpp"

#include <memory>

#include "benchmark/benchmark.h"
#include "hyrise.hpp"
#include "operators/table_wrapper.hpp"
#include "synthetic_table_generator.hpp"
#include "types.hpp"

namespace hyrise {

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
  // The table objects needs to be manually removed here to not interfere with the buffer manager
  // that is reset below.
  _table_wrapper_a = nullptr;
  _table_wrapper_b = nullptr;
  _table_dict_wrapper = nullptr;

  Hyrise::reset();
}

void MicroBenchmarkBasicFixture::_clear_cache() {
  micro_benchmark_clear_cache();
}

}  // namespace hyrise
