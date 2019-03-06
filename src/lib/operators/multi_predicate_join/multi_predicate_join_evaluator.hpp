#pragma once

#include <vector>

#include "operators/operator_join_predicate.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

enum class PredicateEvaluationResult { False, FalseRightNull, True };

class MultiPredicateJoinEvaluator {
 public:
  MultiPredicateJoinEvaluator(const Table& left, const Table& right,
                              const std::vector<OperatorJoinPredicate>& join_predicates);
  MultiPredicateJoinEvaluator(const MultiPredicateJoinEvaluator&) = delete;
  MultiPredicateJoinEvaluator(MultiPredicateJoinEvaluator&&) = delete;

  bool satisfies_all_predicates(const RowID& left_row_id, const RowID& right_row_id);
  PredicateEvaluationResult satisfies_all_predicates_detailed_result(const RowID& left_row_id,
                                                                     const RowID& right_row_id);

 protected:
  class BaseFieldComparator : public Noncopyable {
   public:
    virtual bool compare(const RowID& left, const RowID& right) const = 0;
    // For AntiDiscardNull joins we need to know whether the right value is null
    // to be able to decide if we retain of discard the tuple.
    // Therefore true or false as result is not sufficient.
    virtual PredicateEvaluationResult compare_detailed(const RowID& left, const RowID& right) const = 0;

    virtual ~BaseFieldComparator() = default;
  };

  template <typename CompareFunctor, typename L, typename R>
  class FieldComparator : public BaseFieldComparator {
   public:
    FieldComparator(CompareFunctor compare_functor,
                    std::vector<std::unique_ptr<AbstractSegmentAccessor<L>>> left_accessors,
                    std::vector<std::unique_ptr<AbstractSegmentAccessor<R>>> right_accessors)
        : _compare_functor{std::move(compare_functor)},
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
        return _compare_functor(*left_value, *right_value);
      }
    }

    PredicateEvaluationResult compare_detailed(const RowID& left, const RowID& right) const override {
      const auto left_value = _left_accessors[left.chunk_id]->access(left.chunk_offset);
      const auto right_value = _right_accessors[right.chunk_id]->access(right.chunk_offset);
      // NULL value handling:
      // If either left or right value is NULL, the comparison will evaluate to false.
      if (!left_value || !right_value) {
        return !right_value ? PredicateEvaluationResult::FalseRightNull : PredicateEvaluationResult::False;
      } else {
        return _compare_functor(*left_value, *right_value) ? PredicateEvaluationResult::True
                                                           : PredicateEvaluationResult::False;
      }
    }

   private:
    const CompareFunctor _compare_functor;
    const std::vector<std::unique_ptr<AbstractSegmentAccessor<L>>> _left_accessors;
    const std::vector<std::unique_ptr<AbstractSegmentAccessor<R>>> _right_accessors;
  };

  std::vector<std::unique_ptr<BaseFieldComparator>> _comparators;

  template <typename T>
  static std::vector<std::unique_ptr<AbstractSegmentAccessor<T>>> _create_accessors(const Table& table,
                                                                                    const ColumnID column_id);
};

}  // namespace opossum
