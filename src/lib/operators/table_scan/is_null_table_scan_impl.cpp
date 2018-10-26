#include "is_null_table_scan_impl.hpp"

#include <memory>

#include "storage/base_value_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/value_segment/null_value_vector_iterable.hpp"

#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

IsNullTableScanImpl::IsNullTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                                         const PredicateCondition& predicate_condition)
    : AbstractSingleColumnTableScanImpl{in_table, column_id, predicate_condition} {
  DebugAssert(predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull,
              "Invalid PredicateCondition");
}

std::string IsNullTableScanImpl::description() const { return "IsNullScan"; }

std::shared_ptr<PosList> IsNullTableScanImpl::scan_chunk(const ChunkID chunk_id) const {
  const auto& chunk = _in_table->get_chunk(chunk_id);
  const auto& segment = chunk->get_segment(_column_id);

  auto matches = std::make_shared<PosList>();

  _on_scan(*segment, chunk_id, *matches, nullptr);

  return matches;
}

void IsNullTableScanImpl::_on_scan(const BaseSegment& segment, const ChunkID chunk_id, PosList& results,
                                   const std::shared_ptr<const PosList>& position_filter) const {
  resolve_data_and_segment_type(segment, [&](const auto type, const auto& typed_segment) {
    _scan_segment(typed_segment, chunk_id, results, position_filter);
  });
}

void IsNullTableScanImpl::_scan_segment(const BaseSegment& segment, const ChunkID chunk_id, PosList& results,
                                        const std::shared_ptr<const PosList>& position_filter) const {
  resolve_data_and_segment_type(segment, [&](const auto type, const auto& typed_segment) {
    using Type = typename decltype(type)::type;
    auto iterable = create_iterable_from_segment<Type>(typed_segment);

    const auto invert = _predicate_condition == PredicateCondition::IsNotNull;
    const auto functor = [&](const auto& value) { return invert ^ value.is_null(); };

    if constexpr (std::is_same_v<decltype(typed_segment), const ReferenceSegment&>) {
      iterable.with_iterators([&](auto it, auto end) {
        // _scan_with_iterators should not check for null - we do that ourselves
        _scan_with_iterators<false>(functor, it, end, chunk_id, results, true);
      });
    } else {
      iterable.with_iterators(position_filter, [&](auto it, auto end) {
        _scan_with_iterators<false>(functor, it, end, chunk_id, results, true);
      });
    }
  });
}

void IsNullTableScanImpl::_scan_segment(const BaseValueSegment& segment, const ChunkID chunk_id, PosList& results,
                                        const std::shared_ptr<const PosList>& position_filter) const {
  if (_matches_all(segment)) {
    _add_all(chunk_id, results, position_filter, segment.size());
    return;
  }

  if (_matches_none(segment)) {
    return;
  }

  DebugAssert(segment.is_nullable(), "Columns that are not nullable should have been caught by edge case handling.");

  auto iterable = NullValueVectorIterable{segment.null_values()};

  const auto invert = _predicate_condition == PredicateCondition::IsNotNull;
  const auto functor = [&](const auto& value) { return invert ^ value.is_null(); };
  iterable.with_iterators(position_filter, [&](auto it, auto end) {
    _scan_with_iterators<false>(functor, it, end, chunk_id, results, true);
  });
}

bool IsNullTableScanImpl::_matches_all(const BaseValueSegment& segment) const {
  switch (_predicate_condition) {
    case PredicateCondition::IsNull:
      return false;

    case PredicateCondition::IsNotNull:
      return !segment.is_nullable();

    default:
      Fail("Unsupported comparison type encountered");
  }
}

bool IsNullTableScanImpl::_matches_none(const BaseValueSegment& segment) const {
  switch (_predicate_condition) {
    case PredicateCondition::IsNull:
      return !segment.is_nullable();

    case PredicateCondition::IsNotNull:
      return false;

    default:
      Fail("Unsupported comparison type encountered");
  }
}

void IsNullTableScanImpl::_add_all(const ChunkID chunk_id, PosList& results,
                                   const std::shared_ptr<const PosList>& position_filter,
                                   const size_t segment_size) const {
  const auto num_rows = position_filter ? position_filter->size() : segment_size;
  for (auto chunk_offset = 0u; chunk_offset < num_rows; ++chunk_offset) {
    results.emplace_back(RowID{chunk_id, chunk_offset});
  }
}

}  // namespace opossum
