#include <memory>

#include "benchmark/benchmark.h"
#include "micro_benchmark_basic_fixture.hpp"

#include "import_export/binary/binary_parser.hpp"
#include "import_export/binary/binary_writer.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/lz4_segment.hpp"
#include "storage/lz4_segment/lz4_compare.hpp"
#include "storage/table.hpp"

namespace opossum {

/**
 * Welcome to the benchmark playground. Here, you can quickly compare two
 * approaches in a minimal setup. Of course you can also use it to just benchmark
 * one single thing.
 *
 * In this example, a minimal TableScan-like operation is used to evaluate the
 * performance impact of pre-allocating the result vector (PosList in hyrise).
 *
 * A few tips:
 * * The optimizer is not your friend. If you do a bunch of calculations and
 *   don't actually use the result, it will optimize your code out and you will
 *   benchmark only noise.
 * * benchmark::DoNotOptimize(<expression>); marks <expression> as "globally
 *   aliased", meaning that the compiler has to assume that any operation that
 *   *could* access this memory location will do so.
 *   However, despite the name, this will not prevent the compiler from
 *   optimizing this expression itself!
 * * benchmark::ClobberMemory(); can be used to force calculations to be written
 *   to memory. It acts as a memory barrier. In combination with DoNotOptimize(e),
 *   this function effectively declares that it could touch any part of memory,
 *   in particular globally aliased memory.
 * * More information on that: https://stackoverflow.com/questions/40122141/
 */

using ValueT = int32_t;

class BenchmarkPlaygroundFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    MicroBenchmarkBasicFixture::SetUp(state);

    _clear_cache();

    const auto column_definitions = TableColumnDefinitions{{"a", DataType::String, true},
                                                           {"b", DataType::String, false},
                                                           {"c", DataType::Int, true},
                                                           {"d", DataType::Int, false}};
    _table = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size);

    for (auto i = ChunkOffset{0}; i < chunk_size; ++i) {
      const AllTypeVariant str_null = i % 2 == 0 ? NULL_VALUE : pmr_string{"blubb" + std::to_string(i)};
      const AllTypeVariant int_null = i % 2 == 0 ? NULL_VALUE : int32_t(i);
      _table->append({str_null, pmr_string{"blubb" + std::to_string(i)}, int_null, int32_t(i)});
    }

    for (auto i = ChunkOffset{0}; i < chunk_size / 2; ++i) {
      const AllTypeVariant str_null = i % 2 == 0 ? NULL_VALUE : pmr_string{"blubb" + std::to_string(i)};
      const AllTypeVariant int_null = i % 2 == 0 ? NULL_VALUE : int32_t(i);
      _table->append({str_null, pmr_string{"blubb" + std::to_string(i)}, int_null, int32_t(i)});
    }

    _table->last_chunk()->finalize();
    ChunkEncoder::encode_all_chunks(_table, SegmentEncodingSpec{EncodingType::LZ4});
  }
  void TearDown(::benchmark::State& state) override {
    MicroBenchmarkBasicFixture::TearDown(state);
    std::remove(filename.c_str());
  }

  void decompress(const BaseSegment& segment) {
    std::cout << "not implemented" << std::endl;
  }

  template <typename T>
  void decompress(const LZ4Segment<T>& segment) {
    segment.decompress();
  }

 protected:
  std::shared_ptr<Table> _table;

  const std::string filename = "blubb.bin";

  const ChunkOffset chunk_size = ChunkOffset{2000};
};

/**
BENCHMARK_F(BenchmarkPlaygroundFixture, BM_Playground_Reference)(benchmark::State& state) {
  // Add some benchmark-specific setup here

  for (auto _ : state) {
    std::vector<size_t> result;
    benchmark::DoNotOptimize(result.data());  // Do not optimize out the vector
    const auto size = _vec.size();
    for (size_t i = 0; i < size; ++i) {
      if (_vec[i] == 2) {
        result.push_back(i);
        benchmark::ClobberMemory();  // Force that record to be written to memory
      }
    }
  }
}

BENCHMARK_F(BenchmarkPlaygroundFixture, BM_Playground_PreAllocate)(benchmark::State& state) {
  // Add some benchmark-specific setup here

  for (auto _ : state) {
    std::vector<size_t> result;
    benchmark::DoNotOptimize(result.data());  // Do not optimize out the vector
    // pre-allocate result vector
    result.reserve(250'000);
    const auto size = _vec.size();
    for (size_t i = 0; i < size; ++i) {
      if (_vec[i] == 2) {
        result.push_back(i);
        benchmark::ClobberMemory();  // Force that record to be written to memory
      }
    }
  }
}

*/

BENCHMARK_F(BenchmarkPlaygroundFixture, CompareLZ4s)(benchmark::State& state) {
  BinaryWriter::write(*_table, filename);

  auto imp_table = BinaryParser::parse(filename);

  for (auto chunk = ChunkID{0}; chunk < ChunkID{2}; ++chunk) {
    for (auto column = ColumnID{0}; column < ColumnID{2}; ++column) {
      std::cout << "Compare Chunk " << chunk << " Column " << column << std::endl<< std::endl;
      LZ4Compare::compare_segments(_table->get_chunk(chunk)->get_segment(column),
                                   imp_table->get_chunk(chunk)->get_segment(column));
    }
  }

  for (auto chunk = ChunkID{0}; chunk < ChunkID{2}; ++chunk) {
    for (auto column = ColumnID{0}; column < ColumnID{2}; ++column) {
      std::cout << "Decompress Chunk " << chunk << " Column " << column << std::endl<< std::endl;
      const auto segment = imp_table->get_chunk(chunk)->get_segment(column);
      resolve_data_and_segment_type(*segment, [&](const auto data_type_t, const auto& resolved_segment) {
        decompress(resolved_segment);
      });
    }
  }

}

}  // namespace opossum
