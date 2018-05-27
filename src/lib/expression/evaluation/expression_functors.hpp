#pragma once

#include "expression_result.hpp"

namespace opossum {

// clang-format off
bool to_bool(const bool value) { return value; }
bool to_bool(const int32_t value) { return value != 0; }
bool to_bool(const NullValue value) { return false; }

template<typename T> size_t size(const NullableValues<T>& nullable_values) { return nullable_values.first.size(); }
template<typename T> size_t size(const NonNullableValues<T>& non_nullable_values) { return non_nullable_values.size(); }

template<typename T> T value(const NullableValues<T>& nullable_values, const size_t idx) { return nullable_values.first[idx]; }
template<typename T> T value(const NonNullableValues<T>& non_nullable_values, const size_t idx) { return non_nullable_values[idx]; }

template<typename T> bool null(const NullableValues<T>& nullable_values, const size_t idx) { return nullable_values.second[idx]; }
template<typename T> bool null(const NonNullableValues<T>& non_nullable_values, const size_t idx) { return false; }
// clang-format on

struct TernaryOr {
  template<typename R, typename A, typename B> struct supports {
    static constexpr bool value = std::is_same_v<int32_t, R> &&
                                  (std::is_same_v<NullValue, A> || std::is_same_v<int32_t, A>) &&
                                  (std::is_same_v<NullValue, B> || std::is_same_v<int32_t, B>);
  };

  template<typename A, typename B> struct evaluate_as_series {
    static constexpr bool value = is_series<A>::value || is_series<B>::value;
  };

  static constexpr auto may_produce_value_from_null = true;

  template<typename R, typename A, typename B>
  void operator()(const ChunkOffset chunk_offset, R& result, const A& a, const B& b) {
    const auto a_is_true = !a.is_null() && to_bool(a.value());
    const auto b_is_true = !b.is_null() && to_bool(b.value());
    const auto result_is_true = a_is_true || b_is_true;
    const auto result_is_null = (a.is_null() || b.is_null()) && !result_is_true;

    set_expression_result(result, chunk_offset, result_is_true, result_is_null);
  };
};

struct In {
  template<typename R, typename A, typename B> struct supports {
    static constexpr bool value = std::is_same_v<int32_t, R> &&
                                  is_nullable_value_v<A> &&
                                  (is_nullable_values_v<B> || is_non_nullable_values_v<B>);
  };

  template<typename A, typename B> struct evaluate_as_series {
    static constexpr bool value = is_series<A>::value || is_series<B>::value;
  };

  static constexpr auto may_produce_value_from_null = true;

  template<typename R, typename A, typename B>
  void operator()(const ChunkOffset chunk_offset, R& result, const A& a, const B& b) {
    // "NULL IN (...)" is always NULL
    if (a.is_null()) {
      set_expression_result(result, chunk_offset, false, true);
    } else {
      const auto& a_value = a.value();
      const auto& array = b.value();

      static_assert(is_nullable_value_v<std::decay_t<decltype(a_value)>>);
      static_assert(is_nullable_values_v<std::decay_t<decltype(array)>> ||
                    is_non_nullable_values_v<std::decay_t<decltype(array)>>);

      for (auto element_idx = size_t{0}; element_idx < size(array); ++element_idx) {
        if (!null(array, element_idx) && a_value == value(array, element_idx)) {
          set_expression_result(result, chunk_offset, true, false);
          return;
        }
      }
      set_expression_result(result, chunk_offset, false, true);
    }
  };
};

}  // namespace opossum
