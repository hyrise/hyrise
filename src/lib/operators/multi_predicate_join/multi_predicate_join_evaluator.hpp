#pragma once

#include <vector>

#include "operators/operator_join_predicate.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

// This class is used to evaluate secondary join predicates. It is called by the join operators after the primary
// predicate has been handled by, e.g., probing the hash table.
// As accessors are not thread-safe, instances of this class should not be used in multiple threads.
class MultiPredicateJoinEvaluator {
 public:
  MultiPredicateJoinEvaluator(const Table& left, const Table& right, const JoinMode join_mode,
                              const std::vector<OperatorJoinPredicate>& join_predicates);

  bool satisfies_all_predicates(const RowID& left_row_id, const RowID& right_row_id);

 protected:
  class BaseFieldComparator : public Noncopyable {
   public:
    virtual bool compare(const RowID& left, const RowID& right) const = 0;
    virtual ~BaseFieldComparator() = default;
  };

  template <typename CompareFunctor, typename L, typename R>
  class FieldComparator : public BaseFieldComparator {
   public:
    FieldComparator(CompareFunctor compare_functor, JoinMode join_mode,
                    std::vector<std::unique_ptr<AbstractSegmentAccessor<L>>> left_accessors,
                    std::vector<std::unique_ptr<AbstractSegmentAccessor<R>>> right_accessors)
        : _compare_functor{std::move(compare_functor)},
          _join_mode{join_mode},
          _left_accessors{std::move(left_accessors)},
          _right_accessors{std::move(right_accessors)} {}

    /**
     * Tests the value behind the left and right RowID for equality.
     */
    bool compare(const RowID& left, const RowID& right) const override {
      const auto left_value = _left_accessors[left.chunk_id]->access(left.chunk_offset);
      const auto right_value = _right_accessors[right.chunk_id]->access(right.chunk_offset);
      // NULL value handling:
      // If either left or right value is NULL, the comparison will evaluate to TRUE for AntiNullAsTrue and to FALSE
      // for all other JoinModes.
      if (!left_value || !right_value) {
        return _join_mode == JoinMode::AntiNullAsTrue;
      } else {
        return _compare_functor(*left_value, *right_value);
      }
    }

   private:
    const CompareFunctor _compare_functor;
    const JoinMode _join_mode;
    const std::vector<std::unique_ptr<AbstractSegmentAccessor<L>>> _left_accessors;
    const std::vector<std::unique_ptr<AbstractSegmentAccessor<R>>> _right_accessors;
  };

  std::vector<std::unique_ptr<BaseFieldComparator>> _comparators;

  template <typename T>
  static std::vector<std::unique_ptr<AbstractSegmentAccessor<T>>> _create_accessors(const Table& table,
                                                                                    const ColumnID column_id);
};

}  // namespace opossum
