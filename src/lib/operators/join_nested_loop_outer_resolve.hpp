#pragma once

#include "join_nested_loop.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "type_comparison.hpp"

namespace opossum {

// inner join loop that joins two columns via their iterators
template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
void JoinNestedLoop::_join_two_columns(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                                       RightIterator right_begin, RightIterator right_end, const ChunkID chunk_id_left,
                                       const ChunkID chunk_id_right, std::vector<bool>& left_matches) {
  for (; left_it != left_end; ++left_it) {
    const auto left_value = *left_it;
    if (left_value.is_null()) continue;

    for (auto right_it = right_begin; right_it != right_end; ++right_it) {
      const auto right_value = *right_it;
      if (right_value.is_null()) continue;

      if (func(left_value.value(), right_value.value())) {
        _pos_list_left->emplace_back(RowID{chunk_id_left, left_value.chunk_offset()});
        _pos_list_right->emplace_back(RowID{chunk_id_right, right_value.chunk_offset()});

        if (_is_outer_join) {
          left_matches[left_value.chunk_offset()] = true;
        }

        if (_mode == JoinMode::Outer) {
          _right_matches.insert(RowID{chunk_id_right, right_value.chunk_offset()});
        }
      }
    }
  }
}

template<typename ColumnDataType, typename ColumnType>
void JoinNestedLoop::OuterResolve::operator()(ColumnDataType left_type, ColumnType& typed_left_column) const {
  resolve_data_and_column_type(right_data_type, column_right, [&](auto right_type, auto& typed_right_column) {
    using LeftType = typename decltype(left_type)::type;
    using RightType = typename decltype(right_type)::type;

    // make sure that we do not compile invalid versions of these lambdas
    constexpr auto left_is_string_column = (std::is_same<LeftType, std::string>{});
    constexpr auto right_is_string_column = (std::is_same<RightType, std::string>{});

    constexpr auto neither_is_string_column = !left_is_string_column && !right_is_string_column;
    constexpr auto both_are_string_columns = left_is_string_column && right_is_string_column;

    // clang-format off
    if constexpr (neither_is_string_column || both_are_string_columns) {
      auto iterable_left = create_iterable_from_column<LeftType>(typed_left_column);
      auto iterable_right = create_iterable_from_column<RightType>(typed_right_column);

      iterable_left.with_iterators([&](auto left_it, auto left_end) {
        iterable_right.with_iterators([&](auto right_it, auto right_end) {
          with_comparator(predicate_condition, [&](auto comparator) {
            join_nested_loop._join_two_columns(comparator, left_it, left_end, right_it, right_end, chunk_id_left,
                                               chunk_id_right, left_matches);
          });
        });
      });
    }
    // clang-format on
  });
}

}  // namespace opossum
