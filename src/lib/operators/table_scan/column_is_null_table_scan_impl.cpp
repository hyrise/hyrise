#include "column_is_null_table_scan_impl.hpp"

#include <memory>

#include "storage/base_value_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/value_segment/null_value_vector_iterable.hpp"

#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

ColumnIsNullTableScanImpl::ColumnIsNullTableScanImpl(const std::shared_ptr<const Table>& in_table,
                                                     const ColumnID column_id,
                                                     const PredicateCondition& predicate_condition)
    : _in_table(in_table), _column_id(column_id), _predicate_condition(predicate_condition) {
  DebugAssert(predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull,
              "Invalid PredicateCondition");
}

std::string ColumnIsNullTableScanImpl::description() const { return "IsNullScan"; }

std::shared_ptr<PosList> ColumnIsNullTableScanImpl::scan_chunk(const ChunkID chunk_id) const {
  const auto chunk = _in_table->get_chunk(chunk_id);
  const auto& segment = chunk->get_segment(_column_id);

  auto matches = std::make_shared<PosList>();

  if (const auto value_segment = std::dynamic_pointer_cast<BaseValueSegment>(segment)) {
    _scan_value_segment(*value_segment, chunk_id, *matches);
  } else {
    _scan_generic_segment(*segment, chunk_id, *matches);
  }

  return matches;
}

void ColumnIsNullTableScanImpl::_scan_generic_segment(const BaseSegment& segment, const ChunkID chunk_id,
                                                      PosList& matches) const {
  segment_with_iterators(segment, [&](auto it, [[maybe_unused]] const auto end) {
    // Don't instantiate this for this for ValueSegments to save compile time.
    // ValueSegments are handled in _scan_value_segment()
    if constexpr (!is_value_segment_iterable_v<typename decltype(it)::IterableType>) {
      const auto invert = _predicate_condition == PredicateCondition::IsNotNull;
      const auto functor = [&](const auto& value) { return invert ^ value.is_null(); };

      _scan_with_iterators<false>(functor, it, end, chunk_id, matches);
    } else {
      Fail("Dictionary and Reference segments have their own code paths and should be handled there()");
    }
  });
}

void ColumnIsNullTableScanImpl::_scan_value_segment(const BaseValueSegment& segment, const ChunkID chunk_id,
                                                    PosList& matches) const {
  if (_matches_all(segment)) {
    _add_all(chunk_id, matches, segment.size());
    return;
  }

  if (_matches_none(segment)) {
    return;
  }

  DebugAssert(segment.is_nullable(), "Columns that are not nullable should have been caught by edge case handling.");

  auto iterable = NullValueVectorIterable{segment.null_values()};

  const auto invert = _predicate_condition == PredicateCondition::IsNotNull;
  const auto functor = [&](const auto& value) { return invert ^ value.is_null(); };
  iterable.with_iterators([&](auto it, auto end) { _scan_with_iterators<false>(functor, it, end, chunk_id, matches); });
}

bool ColumnIsNullTableScanImpl::_matches_all(const BaseValueSegment& segment) const {
  switch (_predicate_condition) {
    case PredicateCondition::IsNull:
      return false;

    case PredicateCondition::IsNotNull:
      return !segment.is_nullable();

    default:
      Fail("Unsupported comparison type encountered");
  }
}

bool ColumnIsNullTableScanImpl::_matches_none(const BaseValueSegment& segment) const {
  switch (_predicate_condition) {
    case PredicateCondition::IsNull:
      return !segment.is_nullable();

    case PredicateCondition::IsNotNull:
      return false;

    default:
      Fail("Unsupported comparison type encountered");
  }
}

void ColumnIsNullTableScanImpl::_add_all(const ChunkID chunk_id, PosList& matches, const size_t segment_size) {
  const auto num_rows = segment_size;
  for (auto chunk_offset = 0u; chunk_offset < num_rows; ++chunk_offset) {
    matches.emplace_back(RowID{chunk_id, chunk_offset});
  }
}

}  // namespace opossum
