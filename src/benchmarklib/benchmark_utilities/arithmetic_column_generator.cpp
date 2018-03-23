#include "arithmetic_column_generator.hpp"

#include <random>
#include <memory>
#include <algorithm>
#include <iomanip>

#include "resolve_type.hpp"
#include "storage/value_column.hpp"


namespace benchmark_utilities {

namespace {

template <typename T, std::enable_if_t<std::is_integral_v<T>>* = nullptr>
auto get_uniform_dist(const T min, const T max) {
  return std::uniform_int_distribution{min, max};
}

template <typename T, std::enable_if_t<std::is_floating_point_v<T>>* = nullptr>
auto get_uniform_dist(const T min, const T max) {
  return std::uniform_real_distribution{min, max};
}

}  // namespace

using namespace opossum;

template <typename T>
ArithmeticColumnGenerator<T>::ArithmeticColumnGenerator(PolymorphicAllocator<size_t> alloc, const uint32_t random_seed)
    : _random_seed{random_seed},
      _data_type{data_type_from_type<T>()},
      _alloc{alloc},
      _row_count{1'000'000},
      _sorted{false},
      _null_fraction{0.0f} {}

template <typename T>
void ArithmeticColumnGenerator<T>::set_row_count(const uint32_t row_count) {
  _row_count = row_count;
}

template <typename T>
void ArithmeticColumnGenerator<T>::set_sorted(bool sorted) {
  _sorted = sorted;
}

template <typename T>
void ArithmeticColumnGenerator<T>::set_null_fraction(float fraction) {
  _null_fraction = fraction;
}

template <typename T>
std::shared_ptr<ValueColumn<T>> ArithmeticColumnGenerator<T>::uniformly_distributed_column(const T min, const T max) const {
  std::mt19937 gen{_random_seed};
  auto dist = get_uniform_dist(min, max);

  auto values = pmr_concurrent_vector<T>(_row_count, _alloc);

  for (auto i = 0u; i < _row_count; ++i) {
    values[i] = dist(gen);
  }

  if (_sorted) {
    if (_null_fraction > 0.0f) {
      const auto end_index = static_cast<size_t>(std::round((1.0f - _null_fraction) * _row_count));
      std::sort(values.begin(), values.begin() + end_index);
    } else {
      std::sort(values.begin(), values.end());
    }
  }

  if (_null_fraction > 0.0f) {
    auto null_values = generate_null_values();
    return column_from_data(std::move(values), std::move(null_values));
  }

  return column_from_values(std::move(values));
}

template <typename T>
std::shared_ptr<opossum::ValueColumn<T>> ArithmeticColumnGenerator<T>::uniformly_distributed_column_with_runs(
    const T min, const T max, const T run_length_mean) const {
  std::mt19937 gen{_random_seed};
  auto dist = get_uniform_dist(min, max);
  auto run_length_dist = std::poisson_distribution<size_t>{static_cast<double>(run_length_mean)};

  auto values = pmr_concurrent_vector<T>(_row_count, _alloc);

  auto i = size_t{0u};
  while (i < _row_count) {
    const auto value = dist(gen);
    const auto run_length = std::max(run_length_dist(gen), size_t{1u});

    const auto end = std::min((i + run_length), size_t{_row_count});
    for (; i < end; ++i) {
      values[i] = value;
    }
  }

  return column_from_values(std::move(values));
}

template <typename T>
std::shared_ptr<opossum::ValueColumn<T>> ArithmeticColumnGenerator<T>::normally_distributed_column(
    const T mean, const T outlier_mean, const double outlier_factor, const double std_dev) const {
  const auto round_if_integral = [](auto value) {
    if constexpr (std::is_integral_v<T>) {
      return std::round(value);
    } else {
      return value;
    }
  };

  std::mt19937 gen{_random_seed};
  auto dist = std::normal_distribution<double>{static_cast<double>(mean), std_dev};
  auto outlier_dist = std::normal_distribution<double>{static_cast<double>(outlier_mean), std_dev};

  auto is_outlier_dist = std::bernoulli_distribution{outlier_factor};

  auto values = pmr_concurrent_vector<T>(_row_count, _alloc);

  for (auto i = 0u; i < _row_count; ++i) {
    const auto is_outlier = is_outlier_dist(gen) ? 1u : 0u;
    const auto value = static_cast<T>(round_if_integral(dist(gen)));
    const auto outlier_value = static_cast<T>(round_if_integral(outlier_dist(gen)));
    values[i] = (!is_outlier * value) + (is_outlier * outlier_value);
  }

  return column_from_values(std::move(values));
}

template <typename T>
opossum::pmr_concurrent_vector<bool> ArithmeticColumnGenerator<T>::generate_null_values() const {
  std::mt19937 gen{_random_seed};

  auto null_values = pmr_concurrent_vector<bool>(_row_count, false, _alloc);

  if (_sorted) {
    const auto begin_index = static_cast<size_t>(std::round((1.0f - _null_fraction) * _row_count));
    std::fill(null_values.begin() + begin_index, null_values.end(), true);
  } else {
    auto is_null_dist = std::bernoulli_distribution(_null_fraction);
    std::generate(null_values.begin(), null_values.end(), [&]() { return is_null_dist(gen); });
  }
  return null_values;
}

template <typename T>
std::shared_ptr<ValueColumn<T>> ArithmeticColumnGenerator<T>::column_from_values(pmr_concurrent_vector<T> values) const {
  return std::allocate_shared<ValueColumn<T>>(_alloc, std::move(values));
}

template <typename T>
std::shared_ptr<ValueColumn<T>> ArithmeticColumnGenerator<T>::column_from_data(
    pmr_concurrent_vector<T> values,
    pmr_concurrent_vector<bool> null_values) const {
  return std::allocate_shared<ValueColumn<T>>(_alloc, std::move(values), std::move(null_values));
}

template class ArithmeticColumnGenerator<int32_t>;
template class ArithmeticColumnGenerator<int64_t>;
template class ArithmeticColumnGenerator<float>;
template class ArithmeticColumnGenerator<double>;

}  // namespace benchmark_utilities
