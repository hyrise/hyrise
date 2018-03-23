#include "abstract_column_benchmark.hpp"

#include <cmath>
#include <cstdint>
#include <memory>
#include <chrono>
#include <algorithm>
#include <random>

#include "benchmark_utilities/arithmetic_column_generator.hpp"

namespace opossum {

AbstractColumnBenchmark::AbstractColumnBenchmark() : _random_seed{std::random_device{}()} {
  static const auto numa_node = 35;
  _memory_resource = std::make_unique<BenchmarkMemoryResource>(numa_node);
}

auto AbstractColumnBenchmark::get_generator(const DistributionInfo& info) const -> std::function<ValueColumnPtr()> {
  auto column_generator = benchmark_utilities::ArithmeticColumnGenerator<int32_t>{get_alloc(), _random_seed};
  column_generator.set_row_count(info.row_count);
  column_generator.set_sorted(info.sorted);
  column_generator.set_null_fraction(info.null_fraction);

  return [column_generator, max_value = info.max_value]() {
    // return column_generator.uniformly_distributed_column(0, max_value);
    return column_generator.uniformly_distributed_column_with_runs(0, 65'535, max_value);
    //return column_generator.normally_distributed_column(8, max_value, 0.1, 20.0);
  };
}

PolymorphicAllocator<size_t> AbstractColumnBenchmark::get_alloc() const {
  return PolymorphicAllocator<size_t>{_memory_resource.get()};
}

std::vector<double> AbstractColumnBenchmark::to_mis(const std::vector<Duration>& durations, uint32_t row_count) {
  const auto to_mis = [row_count](auto x) {
    const auto x_in_micro_sec = std::chrono::duration_cast<std::chrono::microseconds>(x).count();
    return static_cast<double>(row_count) / x_in_micro_sec;
  };

  auto result = std::vector<double>(durations.size());
  std::transform(durations.cbegin(), durations.cend(), result.begin(), to_mis);

  return result;
}

std::vector<double> AbstractColumnBenchmark::to_ms(const std::vector<Duration>& durations) {
  const auto to_ms = [](auto x) {
    return std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(x).count();
  };

  auto result = std::vector<double>(durations.size());
  std::transform(durations.cbegin(), durations.cend(), result.begin(), to_ms);

  return result;
}

std::pair<ChunkOffset, std::vector<bool>> AbstractColumnBenchmark::_generate_access_bitmap(ChunkOffset row_count,
                                                                                           float point_access_factor) const {
  const auto num_positions = static_cast<ChunkOffset>(std::round(row_count * point_access_factor));
  auto accesses = std::vector<bool>(row_count, false);
  std::fill_n(accesses.begin(), num_positions, true);

  std::default_random_engine engine{_random_seed};
  std::shuffle(accesses.begin(), accesses.end(), engine);

  return {num_positions, accesses};
}

std::shared_ptr<PosList> AbstractColumnBenchmark::_generate_pos_list(ChunkOffset row_count, float point_access_factor) const {
  const auto [num_positions, accesses] = _generate_access_bitmap(row_count, point_access_factor);

  auto pos_list = PosList{};
  pos_list.reserve(num_positions);

  for (ChunkOffset chunk_offset{0}; chunk_offset < row_count; ++chunk_offset) {
    const auto access = accesses[chunk_offset];
    if (access) pos_list.push_back(RowID{ChunkID{0u}, chunk_offset});
  }

  return std::make_shared<PosList>(std::move(pos_list));
}

ChunkOffsetsList AbstractColumnBenchmark::_generate_chunk_offsets_list(ChunkOffset row_count,
                                                                       float point_access_factor) const {
  const auto [num_positions, accesses] = _generate_access_bitmap(row_count, point_access_factor);

  auto chunk_offsets_list = ChunkOffsetsList{};
  chunk_offsets_list.reserve(num_positions);

  for (ChunkOffset chunk_offset{0}; chunk_offset < row_count; ++chunk_offset) {
    const auto access = accesses[chunk_offset];
    if (access) chunk_offsets_list.push_back({chunk_offset, chunk_offset});
  }

  return chunk_offsets_list;
}

std::shared_ptr<const AbstractOperator> AbstractColumnBenchmark::_get_filtered_table(
    const std::shared_ptr<BaseColumn>& base_column, const float point_access_factor) const {
  auto referenced_table = [&]() {
    auto table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int}}, TableType::Data, base_column->size());
    table->append_chunk({base_column});

    return table;
  }();

  auto wrapped_table = [&]() {
    auto pos_list = _generate_pos_list(base_column->size(), point_access_factor);
    auto ref_column = std::make_shared<ReferenceColumn>(referenced_table, ColumnID{0u}, pos_list);

    auto table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int}}, TableType::References, base_column->size());
    table->append_chunk({ref_column});

    auto wrapped_table = std::make_shared<TableWrapper>(table);
    wrapped_table->execute();

    return wrapped_table;
  }();

  return wrapped_table;
}

}  // namespace opossum
