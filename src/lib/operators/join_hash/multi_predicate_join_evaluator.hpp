#pragma once

#include <memory>
#include <vector>

#include "resolve_type.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class BaseVoidComparator {
 public:
  BaseVoidComparator() = default;

  BaseVoidComparator(const BaseVoidComparator&) = default;

  BaseVoidComparator(BaseVoidComparator&&) = default;

  virtual bool compare(const void *a, const void *b) const = 0;

  virtual ~BaseVoidComparator() {};
};

template<typename T>
class EqVoidComparator : public BaseVoidComparator {
 public:
  bool compare(const void *a, const void *b) const override {
    return *static_cast<const T *>(a) == *static_cast<const T *>(b);
  }
};

template<typename T>
class LtEqVoidComparator : public BaseVoidComparator {
 public:
  bool compare(const void *a, const void *b) const override {
    return *static_cast<const T *>(a) <= *static_cast<const T *>(b);
  }
};

template<typename T>
class LtVoidComparator : public BaseVoidComparator {
 public:
  bool compare(const void *a, const void *b) const override {
    return *static_cast<const T *>(a) < *static_cast<const T *>(b);
  }
};

template<typename T>
class GtEqVoidComparator : public BaseVoidComparator {
 public:
  bool compare(const void *a, const void *b) const override {
    return *static_cast<const T *>(a) >= *static_cast<const T *>(b);
  }
};

template<typename T>
class GtVoidComparator : public BaseVoidComparator {
 public:
  bool compare(const void *a, const void *b) const override {
    return *static_cast<const T *>(a) > *static_cast<const T *>(b);
  }
};

class MultiPredicateJoinEvaluator {
 public:

  MultiPredicateJoinEvaluator(const Table& left, const Table& right,
                              const std::vector<JoinPredicate>& join_predicates)
      : _left{left}, _right{right}, _join_predicates{join_predicates}, _left_row_data{join_predicates.size(), nullptr},
        _right_row_data{join_predicates.size(), nullptr} {
    for (const auto& pred : _join_predicates) {
      resolve_data_type(_left.column_data_type(pred.column_id_pair.first), [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        switch (pred.predicate_condition) {
          case PredicateCondition::Equals:
            _comparators.emplace_back(std::make_unique<EqVoidComparator<ColumnDataType>>());
            break;
          default:
            throw std::runtime_error("Predicate condition not supported!");
        }
      });
    }

    _left_accessors = _create_accessors(_left);
    _right_accessors = _create_accessors(_right);
  }

  MultiPredicateJoinEvaluator(const MultiPredicateJoinEvaluator&) = default;

  MultiPredicateJoinEvaluator(MultiPredicateJoinEvaluator&&) = default;

  bool fulfills_all_predicates(const RowID& left_row_id, const RowID& right_row_id) {

    ColumnID col_id{0};
    for (const auto& comparator : _comparators) {
      const void *left_value = _left_accessors[col_id][left_row_id.chunk_id]->get_void_ptr(left_row_id.chunk_offset);
      const void *right_value = _right_accessors[col_id][right_row_id.chunk_offset]->get_void_ptr(
          right_row_id.chunk_offset);

      if (!comparator->compare(left_value, right_value)) {
        return false;
      }
      ++col_id;
    }

    return true;
  }


  virtual ~MultiPredicateJoinEvaluator() = default;

 protected:
  const Table& _left;
  const Table& _right;
  const std::vector<JoinPredicate>& _join_predicates;

  std::vector<const void *> _left_row_data;
  std::vector<const void *> _right_row_data;
  std::vector<std::unique_ptr<BaseVoidComparator>> _comparators;
  // column // chunk
  std::vector<std::vector<std::unique_ptr<BaseSegmentAccessor>>> _left_accessors;
  std::vector<std::vector<std::unique_ptr<BaseSegmentAccessor>>> _right_accessors;

  static std::vector<std::vector<std::unique_ptr<BaseSegmentAccessor>>> _create_accessors(const Table& table) {
    std::vector<std::vector<std::unique_ptr<BaseSegmentAccessor>>> accessors;
    accessors.resize(table.column_count());
    for (ColumnID col_id{0}; col_id < table.column_count(); ++col_id) {
      accessors[col_id].resize(table.chunk_count());
      for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
        const auto& segment = table.get_chunk(chunk_id)->get_segment(col_id);

        const auto ref_seg = std::dynamic_pointer_cast<ReferenceSegment>(segment);
        if (ref_seg != 0) {
          Assert(ref_seg->pos_list()->references_single_chunk(), "ref segment should only reference a single chunk");
        }

        accessors[col_id][chunk_id] = create_base_segment_accessor(segment);
      }
    }
    return accessors;
  }
};

} // namespace opossum