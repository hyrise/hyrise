#pragma once

#include <memory>
#include <vector>

#include "operator_join_predicate.hpp"
#include "resolve_type.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/table.hpp"
#include "type_comparison.hpp"
#include "types.hpp"

namespace opossum {

class BaseFieldComparator : public Noncopyable {
 public:
  virtual bool compare(const RowID& left, const RowID& right) const = 0;
  virtual ~BaseFieldComparator() = default;
};

template <typename CompareFunctor, typename L, typename R>
class FieldComparator : public BaseFieldComparator {
 public:
  FieldComparator(CompareFunctor compare_functor,
                  std::vector<std::unique_ptr<AbstractSegmentAccessor<L>>> left_accessors,
                  std::vector<std::unique_ptr<AbstractSegmentAccessor<R>>> right_accessors)
      : _compare{std::move(compare_functor)},
        _left_accessors{std::move(left_accessors)},
        _right_accessors{std::move(right_accessors)} {}

  bool compare(const RowID& left, const RowID& right) const override {
    const auto left_value = _left_accessors[left.chunk_id]->access(left.chunk_offset);
    const auto right_value = _right_accessors[right.chunk_id]->access(right.chunk_offset);

    // NULL value handling:
    // If either left or right value is NULL, the comparison will evaluate to false.
    if (!left_value || !right_value) {
      return false;
    } else {
      return _compare(*left_value, *right_value);
    }
  }

 private:
  const CompareFunctor _compare;
  const std::vector<std::unique_ptr<AbstractSegmentAccessor<L>>> _left_accessors;
  const std::vector<std::unique_ptr<AbstractSegmentAccessor<R>>> _right_accessors;
};

class MultiPredicateJoinEvaluator {
 public:
  MultiPredicateJoinEvaluator(const Table& left, const Table& right,
                              const std::vector<OperatorJoinPredicate>& join_predicates) {
    for (const auto& predicate : join_predicates) {
      resolve_data_type(left.column_data_type(predicate.column_ids.first), [&](auto left_type) {
        resolve_data_type(right.column_data_type(predicate.column_ids.second), [&](auto right_type) {
          using LeftColumnDataType = typename decltype(left_type)::type;
          using RightColumnDataType = typename decltype(right_type)::type;

          // This code has been copied from JoinNestedLoop::_join_two_untyped_segments
          constexpr auto LEFT_IS_STRING_COLUMN = (std::is_same<LeftColumnDataType, std::string>{});
          constexpr auto RIGHT_IS_STRING_COLUMN = (std::is_same<RightColumnDataType, std::string>{});

          constexpr auto NEITHER_IS_STRING_COLUMN = !LEFT_IS_STRING_COLUMN && !RIGHT_IS_STRING_COLUMN;
          constexpr auto BOTH_ARE_STRING_COLUMNS = LEFT_IS_STRING_COLUMN && RIGHT_IS_STRING_COLUMN;

          if constexpr (NEITHER_IS_STRING_COLUMN || BOTH_ARE_STRING_COLUMNS) {
            auto left_accessors = _create_accessors<LeftColumnDataType>(left, predicate.column_ids.first);
            auto right_accessors = _create_accessors<RightColumnDataType>(right, predicate.column_ids.second);

            // We need to do this assignment to work around an internal compiler error.
            // The compiler error would occur, if you tried to directly access _comparators within the following
            // lambda. This error is discussed at https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86740
            auto& comparators = _comparators;

            with_comparator(predicate.predicate_condition, [&](auto comparator) {
              comparators.emplace_back(
                  std::make_unique<FieldComparator<decltype(comparator), LeftColumnDataType, RightColumnDataType>>(
                      comparator, std::move(left_accessors), std::move(right_accessors)));
            });
          } else {
            Fail("Types of columns cannot be compared.");
          }
        });
      });
    }
  }

  MultiPredicateJoinEvaluator(const MultiPredicateJoinEvaluator&) = default;

  MultiPredicateJoinEvaluator(MultiPredicateJoinEvaluator&&) = default;

  bool fulfills_all_predicates(const RowID& left_row_id, const RowID& right_row_id) {
    for (const auto& comparator : _comparators) {
      if (!comparator->compare(left_row_id, right_row_id)) {
        return false;
      }
    }

    return true;
  }

 protected:
  std::vector<std::unique_ptr<BaseFieldComparator>> _comparators;

  template <typename T>
  static std::vector<std::unique_ptr<AbstractSegmentAccessor<T>>> _create_accessors(const Table& table,
                                                                                    const ColumnID column_id) {
    std::vector<std::unique_ptr<AbstractSegmentAccessor<T>>> accessors;
    accessors.resize(table.chunk_count());
    for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
      const auto& segment = table.get_chunk(chunk_id)->get_segment(column_id);
      accessors[chunk_id] = create_segment_accessor<T>(segment);
    }

    return accessors;
  }
};

}  // namespace opossum
