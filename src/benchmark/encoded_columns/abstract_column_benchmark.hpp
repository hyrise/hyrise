#pragma once

#include <cstdint>
#include <memory>
#include <functional>

#include "storage/base_column.hpp"
#include "types.hpp"

#include "operators/table_wrapper.hpp"
#include "operators/table_scan.hpp"
#include "operators/materialize.hpp"

#include "resolve_type.hpp"
#include "storage/base_column.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/column_iterables/chunk_offset_mapping.hpp"

#include "benchmark_state.hpp"
#include "benchmark_memory_resource.hpp"

namespace opossum {

struct DistributionInfo {
  uint32_t row_count;
  uint32_t max_value;
  bool sorted;
  float null_fraction;
};

inline auto sum = int32_t{0};

class AbstractColumnBenchmark {
 public:
  AbstractColumnBenchmark();
  virtual ~AbstractColumnBenchmark() = default;

  virtual void run() = 0;

 protected:
  static constexpr auto max_num_iterations = 200u;
  static constexpr auto max_duration = std::chrono::seconds{5};

 protected:
  PolymorphicAllocator<size_t> get_alloc() const;

  using ValueColumnPtr = std::shared_ptr<ValueColumn<int32_t>>;
  std::function<ValueColumnPtr()> get_generator(const DistributionInfo& info) const;

  const BenchmarkState benchmark_decompression_with_iterable(const std::shared_ptr<BaseColumn>& base_column) const {
    auto benchmark_state = BenchmarkState{max_num_iterations, max_duration};

    resolve_column_type<int32_t>(*base_column, [&](auto& typed_column) {
      while (benchmark_state.keep_running()) {
        benchmark_state.measure([&]() {
          auto iterable = create_iterable_from_column<int32_t>(typed_column);

          // auto sum = int32_t{0};
          iterable.for_each([&](auto value) {
            if (value.is_null()) return;
            sum += value.value();
          });
        });
      }
    });

    return benchmark_state;
  }

  const BenchmarkState benchmark_decompression_with_iterable(const std::shared_ptr<BaseColumn>& base_column, const float point_access_factor) const {
    auto benchmark_state = BenchmarkState{max_num_iterations, max_duration};

    const auto chunk_offsets_list = _generate_chunk_offsets_list(base_column->size(), point_access_factor);

    resolve_column_type<int32_t>(*base_column, [&](auto& typed_column) {
      using ColumnT = std::decay_t<decltype(typed_column)>;

      if constexpr (!std::is_same_v<ColumnT, ReferenceColumn>) {
        while (benchmark_state.keep_running()) {

          benchmark_state.measure([&]() {
            auto iterable = create_iterable_from_column<int32_t>(typed_column);

            iterable.for_each(&chunk_offsets_list, [&](auto value) {
              if (value.is_null()) return;
              sum += value.value();
            });
          });
        }
      } else {
        Fail("Ups");
      }
    });


    return benchmark_state;
  }

  const BenchmarkState benchmark_table_scan(const std::shared_ptr<BaseColumn>& base_column, const int32_t right_value,
                                            PredicateCondition predicate_condition = PredicateCondition::Equals) {
    auto column_definitions = TableColumnDefinitions{{"a", DataType::Int}};
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, base_column->size());

    table->append_chunk({base_column});

    auto wrapped_table = std::make_shared<TableWrapper>(table);
    wrapped_table->execute();

    auto benchmark_state = BenchmarkState{max_num_iterations, max_duration};

    while (benchmark_state.keep_running()) {
      auto table_scan = std::make_shared<TableScan>(wrapped_table, ColumnID{0u}, predicate_condition,
                                                    AllTypeVariant{right_value});
      benchmark_state.measure([&]() {
        table_scan->execute();
      });
    }

    return benchmark_state;
  }

  const BenchmarkState benchmark_table_scan(
      const std::shared_ptr<BaseColumn>& base_column,
      const int32_t right_value,
      const float point_access_factor,
      PredicateCondition predicate_condition = PredicateCondition::Equals) {
    auto benchmark_state = BenchmarkState{max_num_iterations, max_duration};

    while (benchmark_state.keep_running()) {
      auto filtered_table = _get_filtered_table(base_column, point_access_factor);

      auto table_scan = std::make_shared<TableScan>(filtered_table, ColumnID{0u}, predicate_condition,
                                                    AllTypeVariant{right_value});

      benchmark_state.measure([&]() {
        table_scan->execute();
      });
    }

    return benchmark_state;
  }

  const BenchmarkState benchmark_materialize(
      const std::shared_ptr<BaseColumn>& base_column,
      const float point_access_factor) {
    auto filtered_table = _get_filtered_table(base_column, point_access_factor);

    auto benchmark_state = BenchmarkState{max_num_iterations, max_duration};

    while (benchmark_state.keep_running()) {
      auto materialize = std::make_shared<Materialize>(filtered_table);

      benchmark_state.measure([&]() {
        materialize->execute();
      });
    }

    return benchmark_state;
  }

  template <typename Functor>
  auto memory_consumption(Functor functor) const {
    const auto allocated_before = _memory_resource->currently_allocated();
    auto result = functor();
    const auto allocated_after = _memory_resource->currently_allocated();
    const auto allocated_memory = allocated_after - allocated_before;
    return std::make_pair(std::move(result), allocated_memory);
  }

  static std::vector<double> to_mis(const std::vector<Duration>& durations, uint32_t row_count);
  static std::vector<double> to_ms(const std::vector<Duration>& durations);

 private:
  std::pair<ChunkOffset, std::vector<bool>> _generate_access_bitmap(ChunkOffset row_count,
                                                                    float point_access_factor) const;

  std::shared_ptr<PosList> _generate_pos_list(ChunkOffset row_count, float point_access_factor) const;

  ChunkOffsetsList _generate_chunk_offsets_list(ChunkOffset row_count, float point_access_factor) const;

  std::shared_ptr<const AbstractOperator> _get_filtered_table(const std::shared_ptr<BaseColumn>& base_column,
                                                              const float point_access_factor) const;

 private:
  const uint32_t _random_seed;
  std::unique_ptr<BenchmarkMemoryResource> _memory_resource;
};

}  // namespace opossum
