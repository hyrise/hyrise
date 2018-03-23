#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class ValueColumn;

}  // namespace opossum

namespace benchmark_utilities {

template <typename T>
class ArithmeticColumnGenerator {
  static_assert(std::is_arithmetic_v<T>);

 public:
  ArithmeticColumnGenerator(opossum::PolymorphicAllocator<size_t> alloc, const uint32_t random_seed);

  void set_row_count(const uint32_t row_count);
  void set_sorted(bool sorted);
  void set_null_fraction(float fraction);  // [0.0, 1.0], 0.0 means no null values

  std::shared_ptr<opossum::ValueColumn<T>> uniformly_distributed_column(const T min, const T max) const;

  std::shared_ptr<opossum::ValueColumn<T>> uniformly_distributed_column_with_runs(const T min, const T max,
                                                                                  const T run_length_mean) const;

  std::shared_ptr<opossum::ValueColumn<T>> normally_distributed_column(const T mean, const T outlier_mean,
                                                                       const double outlier_factor,
                                                                       const double std_dev) const;

 private:
  opossum::pmr_concurrent_vector<bool> generate_null_values() const;

  std::shared_ptr<opossum::ValueColumn<T>> column_from_values(opossum::pmr_concurrent_vector<T> values) const;

  std::shared_ptr<opossum::ValueColumn<T>> column_from_data(opossum::pmr_concurrent_vector<T> values,
                                                            opossum::pmr_concurrent_vector<bool> null_values) const;

 private:
  const uint32_t _random_seed;
  const opossum::DataType _data_type;
  const opossum::PolymorphicAllocator<size_t> _alloc;
  uint32_t _row_count;
  bool _sorted;
  float _null_fraction;
};

}  // benchmark_utilities
