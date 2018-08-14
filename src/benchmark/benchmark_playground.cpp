#include <memory>

#include "benchmark/benchmark.h"
#include "benchmark_basic_fixture.hpp"

#include "storage/value_column.hpp"

namespace opossum {

class BenchmarkPlaygroundFixture : public BenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    BenchmarkBasicFixture::SetUp(state);

    _clear_cache();

    auto value_column = std::make_shared<ValueColumn<int>>();
    value_column->append({123});
    value_column->append({456});
    value_column->append({789});

    _value_column = value_column;
  }
  void TearDown(::benchmark::State& state) override { BenchmarkBasicFixture::TearDown(state); }

 protected:
  std::shared_ptr<const BaseColumn> _value_column;
};

BENCHMARK_F(BenchmarkPlaygroundFixture, BaseColumn_Subscript_Operator)(benchmark::State& state) {
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(_value_column);
    for (auto i = 0u; i < 333; i++) {
      const auto v1 = _value_column->operator[](ChunkOffset{1});
      const auto v0 = _value_column->operator[](ChunkOffset{2});
      const auto v2 = _value_column->operator[](ChunkOffset{0});
      benchmark::DoNotOptimize(v1);
      benchmark::DoNotOptimize(v0);
      benchmark::DoNotOptimize(v2);
      benchmark::ClobberMemory();
    }
  }
}

BENCHMARK_F(BenchmarkPlaygroundFixture, BaseTypedColumn_get_typed_value)(benchmark::State& state) {
  auto base_typed_column = std::dynamic_pointer_cast<const BaseTypedColumn<int>>(_value_column);

  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(base_typed_column);
    for (auto i = 0u; i < 333; i++) {
      const auto v1 = base_typed_column->get_typed_value(ChunkOffset{1});
      const auto v0 = base_typed_column->get_typed_value(ChunkOffset{2});
      const auto v2 = base_typed_column->get_typed_value(ChunkOffset{0});
      benchmark::DoNotOptimize(v1);
      benchmark::DoNotOptimize(v0);
      benchmark::DoNotOptimize(v2);
      benchmark::ClobberMemory();
    }
  }
}

BENCHMARK_F(BenchmarkPlaygroundFixture, ValueColumn_get_typed_value)(benchmark::State& state) {
  auto value_column_int = std::dynamic_pointer_cast<const ValueColumn<int>>(_value_column);

  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(value_column_int);
    for (auto i = 0u; i < 333; i++) {
      const auto v1 = value_column_int->get_typed_value(ChunkOffset{1});
      const auto v0 = value_column_int->get_typed_value(ChunkOffset{2});
      const auto v2 = value_column_int->get_typed_value(ChunkOffset{0});
      benchmark::DoNotOptimize(v1);
      benchmark::DoNotOptimize(v0);
      benchmark::DoNotOptimize(v2);
      benchmark::ClobberMemory();
    }
  }
}

}  // namespace opossum
