#pragma once

#include <memory>
#include <vector>

#include "resolve_type.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/table.hpp"
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
    const auto left_opt = _left_accessors[left.chunk_id]->access(left.chunk_offset);
    const auto right_opt = _right_accessors[right.chunk_id]->access(right.chunk_offset);

    // TODO(anyone): If opts have no value, an instance of 'std::bad_optional_access' would be thrown.
    return _compare(left_opt.value(), right_opt.value());
  }

 private:
  const CompareFunctor _compare;
  const std::vector<std::unique_ptr<AbstractSegmentAccessor<L>>> _left_accessors;
  const std::vector<std::unique_ptr<AbstractSegmentAccessor<R>>> _right_accessors;
};

class MultiPredicateJoinEvaluator {
 public:
  MultiPredicateJoinEvaluator(const Table& left, const Table& right,
                              const std::vector<JoinPredicate>& join_predicates) {
    for (const auto& pred : join_predicates) {
      resolve_data_type(left.column_data_type(pred.column_id_pair.first), [&](auto left_type) {
        using LeftColumnDataType = typename decltype(left_type)::type;
        resolve_data_type(right.column_data_type(pred.column_id_pair.second), [&](auto right_type) {
          using RightColumnDataType = typename decltype(right_type)::type;

          std::vector<std::unique_ptr<AbstractSegmentAccessor<LeftColumnDataType>>> left_accessors;
          std::vector<std::unique_ptr<AbstractSegmentAccessor<RightColumnDataType>>> right_accessors;

          left_accessors = _create_accessors<LeftColumnDataType>(left, pred.column_id_pair.first);
          right_accessors = _create_accessors<RightColumnDataType>(right, pred.column_id_pair.second);

          constexpr auto LEFT_IS_STRING_COLUMN = (std::is_same<LeftColumnDataType, std::string>{});
          constexpr auto RIGHT_IS_STRING_COLUMN = (std::is_same<RightColumnDataType, std::string>{});

          constexpr auto NEITHER_IS_STRING_COLUMN = !LEFT_IS_STRING_COLUMN && !RIGHT_IS_STRING_COLUMN;
          constexpr auto BOTH_ARE_STRING_COLUMNS = LEFT_IS_STRING_COLUMN && RIGHT_IS_STRING_COLUMN;

          if constexpr (NEITHER_IS_STRING_COLUMN || BOTH_ARE_STRING_COLUMNS) {
            switch (pred.predicate_condition) {
              case PredicateCondition::Equals:
                _comparators.emplace_back(
                    std::make_unique<FieldComparator<std::equal_to<void>, LeftColumnDataType, RightColumnDataType>>(
                        std::equal_to<void>{}, std::move(left_accessors), std::move(right_accessors)));
                break;
              case PredicateCondition::GreaterThan:
                _comparators.emplace_back(
                    std::make_unique<FieldComparator<std::greater<void>, LeftColumnDataType, RightColumnDataType>>(
                        std::greater<void>{}, std::move(left_accessors), std::move(right_accessors)));
                break;
              case PredicateCondition::GreaterThanEquals:
                _comparators.emplace_back(
                    std::make_unique<
                        FieldComparator<std::greater_equal<void>, LeftColumnDataType, RightColumnDataType>>(
                        std::greater_equal<void>{}, std::move(left_accessors), std::move(right_accessors)));
                break;
              case PredicateCondition::LessThan:
                _comparators.emplace_back(
                    std::make_unique<FieldComparator<std::less<void>, LeftColumnDataType, RightColumnDataType>>(
                        std::less<void>{}, std::move(left_accessors), std::move(right_accessors)));
                break;
              case PredicateCondition::LessThanEquals:
                _comparators.emplace_back(
                    std::make_unique<FieldComparator<std::less_equal<void>, LeftColumnDataType, RightColumnDataType>>(
                        std::less_equal<void>{}, std::move(left_accessors), std::move(right_accessors)));
                break;
              case PredicateCondition::NotEquals:
                _comparators.emplace_back(
                    std::make_unique<FieldComparator<std::not_equal_to<void>, LeftColumnDataType, RightColumnDataType>>(
                        std::not_equal_to<void>{}, std::move(left_accessors), std::move(right_accessors)));
                break;
              default:
                Fail("Predicate condition not supported!");
            }
          } else {
            Fail("Types of columns cannot be compared.");
          }
        });
      });
    }  // end for
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

  virtual ~MultiPredicateJoinEvaluator() = default;

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
