#include "join_index.hpp"

#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "join_nested_loop.hpp"
#include "multi_predicate_join/multi_predicate_join_evaluator.hpp"
#include "resolve_type.hpp"
#include "storage/index/abstract_index.hpp"
#include "storage/segment_iterate.hpp"
#include "type_comparison.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

/*
 * This is an index join implementation. It expects to find an index on the index side column.
 * It can be used for all join modes except JoinMode::Cross.
 * For the remaining join types or if no index is found it falls back to a nested loop join.
 */

bool JoinIndex::supports(const JoinConfiguration config) {
  if (!config.left_table_type || !config.right_table_type || !config.index_side) {
    Fail("Table types and index side are required to make support decisions for the index join.");
  } else {
    TableType index_side_table_type;
    if (*config.index_side == IndexSide::Left) {
      index_side_table_type = *config.left_table_type;
    } else {
      index_side_table_type = *config.right_table_type;
    }
    if (index_side_table_type == TableType::References && config.join_mode != JoinMode::Inner) {
      return false;
    } else {
      return !config.secondary_predicates;
    }
  }
}

JoinIndex::JoinIndex(const std::shared_ptr<const AbstractOperator>& left,
                     const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                     const OperatorJoinPredicate& primary_predicate,
                     const std::vector<OperatorJoinPredicate>& secondary_predicates, const IndexSide index_side)
    : AbstractJoinOperator(OperatorType::JoinIndex, left, right, mode, primary_predicate, secondary_predicates,
                           std::make_unique<JoinIndex::PerformanceData>()),
      _index_side(index_side),
      _adjusted_primary_predicate(primary_predicate) {
  if (_index_side == IndexSide::Left) {
    _adjusted_primary_predicate.flip();
  }
}

const std::string& JoinIndex::name() const {
  static const auto name = std::string{"JoinIndex"};
  return name;
}

std::shared_ptr<AbstractOperator> JoinIndex::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<JoinIndex>(copied_input_left, copied_input_right, _mode, _primary_predicate,
                                     std::vector<OperatorJoinPredicate>{}, _index_side);
}

void JoinIndex::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> JoinIndex::_on_execute() {
  Assert(
      supports({_mode, _primary_predicate.predicate_condition,
                input_table_left()->column_data_type(_primary_predicate.column_ids.first),
                input_table_right()->column_data_type(_primary_predicate.column_ids.second),
                !_secondary_predicates.empty(), input_table_left()->type(), input_table_right()->type(), _index_side}),
      "JoinIndex doesn't support these parameters");

  if (_index_side == IndexSide::Left) {
    _probe_input_table = input_table_right();
    _index_input_table = input_table_left();
  } else {
    _probe_input_table = input_table_left();
    _index_input_table = input_table_right();
  }

  _index_matches.resize(_index_input_table->chunk_count());
  _probe_matches.resize(_probe_input_table->chunk_count());

  const auto is_semi_or_anti_join =
      _mode == JoinMode::Semi || _mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue;

  const auto track_probe_matches = _mode == JoinMode::FullOuter ||
                                   (_mode == JoinMode::Left && _index_side == IndexSide::Right) ||
                                   (_mode == JoinMode::Right && _index_side == IndexSide::Left) ||
                                   (is_semi_or_anti_join && _index_side == IndexSide::Right);
  const auto track_index_matches = _mode == JoinMode::FullOuter ||
                                   (_mode == JoinMode::Left && _index_side == IndexSide::Left) ||
                                   (_mode == JoinMode::Right && _index_side == IndexSide::Right) ||
                                   (is_semi_or_anti_join && _index_side == IndexSide::Left);

  if (track_probe_matches) {
    const auto chunk_count = _probe_input_table->chunk_count();
    for (ChunkID probe_chunk_id{0}; probe_chunk_id < chunk_count; ++probe_chunk_id) {
      const auto chunk = _probe_input_table->get_chunk(probe_chunk_id);
      Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

      _probe_matches[probe_chunk_id].resize(chunk->size());
    }
  }

  if (track_index_matches) {
    const auto chunk_count = _index_input_table->chunk_count();
    for (ChunkID index_chunk_id{0}; index_chunk_id < chunk_count; ++index_chunk_id) {
      const auto chunk = _index_input_table->get_chunk(index_chunk_id);
      Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

      _index_matches[index_chunk_id].resize(chunk->size());
    }
  }

  _probe_pos_list = std::make_shared<PosList>();
  _index_pos_list = std::make_shared<PosList>();

  auto pos_list_size_to_reserve =
      std::max(uint64_t{100}, std::min(_probe_input_table->row_count(), _index_input_table->row_count()));

  _probe_pos_list->reserve(pos_list_size_to_reserve);
  _index_pos_list->reserve(pos_list_size_to_reserve);

  auto& performance_data = static_cast<PerformanceData&>(*_performance_data);

  auto secondary_predicate_evaluator = MultiPredicateJoinEvaluator{*_probe_input_table, *_index_input_table, _mode, {}};

  if (_mode == JoinMode::Inner && _index_input_table->type() == TableType::References &&
      _secondary_predicates.empty()) {  // INNER REFERENCE JOIN
    // Scan all chunks for index input
    const auto chunk_count_index_input_table = _index_input_table->chunk_count();
    for (ChunkID index_chunk_id{0}; index_chunk_id < chunk_count_index_input_table; ++index_chunk_id) {
      const auto index_chunk = _index_input_table->get_chunk(index_chunk_id);
      Assert(index_chunk, "Did not expect deleted chunk here.");  // see #1686

      if (index_chunk->size() == 0) {
        continue;
      }

      const auto& reference_segment =
          std::dynamic_pointer_cast<ReferenceSegment>(index_chunk->get_segment(_primary_predicate.column_ids.second));
      Assert(reference_segment != nullptr,
             "Non-empty index input table (reference table) has to have only reference segments.");
      auto index_data_table = reference_segment->referenced_table();
      const std::vector<ColumnID> index_data_table_column_ids{reference_segment->referenced_column_id()};
      const auto& reference_segment_pos_list = reference_segment->pos_list();

      if (reference_segment_pos_list->empty()) {
        continue;
      }

      if (reference_segment_pos_list->references_single_chunk()) {
        const auto index_data_table_chunk = index_data_table->get_chunk((*reference_segment_pos_list)[0].chunk_id);
        Assert(index_data_table_chunk, "Did not expect deleted chunk here.");  // see #1686
        const auto& indexes = index_data_table_chunk->get_indexes(index_data_table_column_ids);

        if (!indexes.empty()) {
          // We assume the first index to be efficient for our join
          // as we do not want to spend time on evaluating the best index inside of this join loop
          const auto& index = indexes.front();

          // Scan all chunks from the probe side input
          const auto chunk_count_probe_input_table = _probe_input_table->chunk_count();
          for (ChunkID probe_chunk_id{0}; probe_chunk_id < chunk_count_probe_input_table; ++probe_chunk_id) {
            const auto chunk = _probe_input_table->get_chunk(probe_chunk_id);
            Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

            const auto& probe_segment = chunk->get_segment(_adjusted_primary_predicate.column_ids.first);
            segment_with_iterators(*probe_segment, [&](auto probe_iter, const auto probe_end) {
              _reference_join_two_segments_using_index(probe_iter, probe_end, probe_chunk_id, index_chunk_id, index,
                                                       reference_segment_pos_list);
            });
          }
          performance_data.chunks_scanned_with_index++;
        } else {
          _fallback_nested_loop(index_chunk_id, track_probe_matches, track_index_matches, is_semi_or_anti_join,
                                secondary_predicate_evaluator);
        }
      } else {
        _fallback_nested_loop(index_chunk_id, track_probe_matches, track_index_matches, is_semi_or_anti_join,
                              secondary_predicate_evaluator);
      }
    }
  } else {  // DATA JOIN since only inner joins are supported for a reference table on the index side
    // Scan all chunks for index input
    const auto chunk_count_index_input_table = _index_input_table->chunk_count();
    for (ChunkID index_chunk_id{0}; index_chunk_id < chunk_count_index_input_table; ++index_chunk_id) {
      const auto index_chunk = _index_input_table->get_chunk(index_chunk_id);
      Assert(index_chunk, "Did not expect deleted chunk here.");  // see #1686

      const auto& indexes =
          index_chunk->get_indexes(std::vector<ColumnID>{_adjusted_primary_predicate.column_ids.second});

      if (!indexes.empty()) {
        // We assume the first index to be efficient for our join
        // as we do not want to spend time on evaluating the best index inside of this join loop
        const auto& index = indexes.front();

        // Scan all chunks from the probe side input
        const auto chunk_count_probe_input_table = _probe_input_table->chunk_count();
        for (ChunkID probe_chunk_id{0}; probe_chunk_id < chunk_count_probe_input_table; ++probe_chunk_id) {
          const auto chunk = _probe_input_table->get_chunk(probe_chunk_id);
          Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

          const auto& probe_segment = chunk->get_segment(_adjusted_primary_predicate.column_ids.first);
          segment_with_iterators(*probe_segment, [&](auto probe_iter, const auto probe_end) {
            _data_join_two_segments_using_index(probe_iter, probe_end, probe_chunk_id, index_chunk_id, index);
          });
        }
        performance_data.chunks_scanned_with_index++;
      } else {
        _fallback_nested_loop(index_chunk_id, track_probe_matches, track_index_matches, is_semi_or_anti_join,
                              secondary_predicate_evaluator);
      }
    }

    _append_matches_non_inner(is_semi_or_anti_join);
  }

  // write output chunks
  Segments output_segments;

  if (_index_side == IndexSide::Left) {
    _write_output_segments(output_segments, _index_input_table, _index_pos_list);
  } else {
    _write_output_segments(output_segments, _probe_input_table, _probe_pos_list);
  }

  if (!is_semi_or_anti_join) {
    if (_index_side == IndexSide::Left) {
      _write_output_segments(output_segments, _probe_input_table, _probe_pos_list);
    } else {
      _write_output_segments(output_segments, _index_input_table, _index_pos_list);
    }
  }

  if (performance_data.chunks_scanned_with_index < performance_data.chunks_scanned_without_index) {
    PerformanceWarning(
        std::string("Only ") + std::to_string(performance_data.chunks_scanned_with_index) + " of " +
        std::to_string(performance_data.chunks_scanned_with_index + performance_data.chunks_scanned_without_index) +
        " chunks scanned using an index");
  }

  return _build_output_table({std::make_shared<Chunk>(output_segments)});
}

void JoinIndex::_fallback_nested_loop(const ChunkID index_chunk_id, const bool track_probe_matches,
                                      const bool track_index_matches, const bool is_semi_or_anti_join,
                                      MultiPredicateJoinEvaluator& secondary_predicate_evaluator) {
  PerformanceWarning("Fallback nested loop used.");
  auto& performance_data = static_cast<PerformanceData&>(*_performance_data);

  const auto index_chunk = _index_input_table->get_chunk(index_chunk_id);
  Assert(index_chunk, "Did not expect deleted chunk here.");  // see #1686

  const auto& index_segment = index_chunk->get_segment(_adjusted_primary_predicate.column_ids.second);
  const auto& index_pos_list_size_pre_fallback = _index_pos_list->size();

  const auto chunk_count = _probe_input_table->chunk_count();
  for (ChunkID probe_chunk_id{0}; probe_chunk_id < chunk_count; ++probe_chunk_id) {
    const auto chunk = _probe_input_table->get_chunk(probe_chunk_id);
    Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

    const auto& probe_segment = chunk->get_segment(_adjusted_primary_predicate.column_ids.first);
    JoinNestedLoop::JoinParams params{*_probe_pos_list,
                                      *_index_pos_list,
                                      _probe_matches[probe_chunk_id],
                                      _index_matches[index_chunk_id],
                                      track_probe_matches,
                                      track_index_matches,
                                      _mode,
                                      _adjusted_primary_predicate.predicate_condition,
                                      secondary_predicate_evaluator,
                                      !is_semi_or_anti_join};
    JoinNestedLoop::_join_two_untyped_segments(*probe_segment, *index_segment, probe_chunk_id, index_chunk_id, params);
  }
  const auto& index_pos_list_size_post_fallback = _index_pos_list->size();
  const auto& count_index_positions = index_pos_list_size_post_fallback - index_pos_list_size_pre_fallback;
  std::fill_n(std::back_inserter(_index_pos_dereferenced), count_index_positions, false);
  performance_data.chunks_scanned_without_index++;
}

// join loop that joins two segments of two columns using an iterator for the probe side,
// and an index for the index side
template <typename ProbeIterator>
void JoinIndex::_data_join_two_segments_using_index(ProbeIterator probe_iter, ProbeIterator probe_end,
                                                    const ChunkID probe_chunk_id, const ChunkID index_chunk_id,
                                                    const std::shared_ptr<AbstractIndex>& index) {
  for (; probe_iter != probe_end; ++probe_iter) {
    const auto probe_side_position = *probe_iter;
    const auto index_ranges = _index_ranges_for_value(probe_side_position, index);
    for (const auto& [index_begin, index_end] : index_ranges) {
      _append_matches(index_begin, index_end, probe_side_position.chunk_offset(), probe_chunk_id, index_chunk_id);
    }
  }
}

template <typename ProbeIterator>
void JoinIndex::_reference_join_two_segments_using_index(
    ProbeIterator probe_iter, ProbeIterator probe_end, const ChunkID probe_chunk_id, const ChunkID index_chunk_id,
    const std::shared_ptr<AbstractIndex>& index, const std::shared_ptr<const PosList>& reference_segment_pos_list) {
  for (; probe_iter != probe_end; ++probe_iter) {
    PosList index_scan_pos_list;
    const auto probe_side_position = *probe_iter;
    const auto index_ranges = _index_ranges_for_value(probe_side_position, index);
    for (const auto& [index_begin, index_end] : index_ranges) {
      std::transform(index_begin, index_end, std::back_inserter(index_scan_pos_list),
                     [index_chunk_id](ChunkOffset index_chunk_offset) {
                       return RowID{index_chunk_id, index_chunk_offset};
                     });
    }

    PosList mutable_ref_seg_pos_list(reference_segment_pos_list->size());
    std::copy(reference_segment_pos_list->begin(), reference_segment_pos_list->end(), mutable_ref_seg_pos_list.begin());
    std::sort(mutable_ref_seg_pos_list.begin(), mutable_ref_seg_pos_list.end());
    std::sort(index_scan_pos_list.begin(), index_scan_pos_list.end());

    PosList index_table_matches{};
    std::set_intersection(mutable_ref_seg_pos_list.begin(), mutable_ref_seg_pos_list.end(), index_scan_pos_list.begin(),
                          index_scan_pos_list.end(), std::back_inserter(index_table_matches));
    _append_matches_dereferenced(probe_chunk_id, probe_side_position.chunk_offset(), index_table_matches);
  }
}

template <typename SegmentPosition>
std::vector<IndexRange> JoinIndex::_index_ranges_for_value(const SegmentPosition probe_side_position,
                                                           const std::shared_ptr<AbstractIndex>& index) const {
  std::vector<IndexRange> index_ranges{};
  index_ranges.reserve(2);

  if (!probe_side_position.is_null()) {
    auto range_begin = AbstractIndex::Iterator{};
    auto range_end = AbstractIndex::Iterator{};

    switch (_adjusted_primary_predicate.predicate_condition) {
      case PredicateCondition::Equals: {
        range_begin = index->lower_bound({probe_side_position.value()});
        range_end = index->upper_bound({probe_side_position.value()});
        break;
      }
      case PredicateCondition::NotEquals: {
        // first, get all values less than the search value
        range_begin = index->cbegin();
        range_end = index->lower_bound({probe_side_position.value()});
        index_ranges.emplace_back(IndexRange{range_begin, range_end});

        // set range for second half to all values greater than the search value
        range_begin = index->upper_bound({probe_side_position.value()});
        range_end = index->cend();
        break;
      }
      case PredicateCondition::GreaterThan: {
        range_begin = index->cbegin();
        range_end = index->lower_bound({probe_side_position.value()});
        break;
      }
      case PredicateCondition::GreaterThanEquals: {
        range_begin = index->cbegin();
        range_end = index->upper_bound({probe_side_position.value()});
        break;
      }
      case PredicateCondition::LessThan: {
        range_begin = index->upper_bound({probe_side_position.value()});
        range_end = index->cend();
        break;
      }
      case PredicateCondition::LessThanEquals: {
        range_begin = index->lower_bound({probe_side_position.value()});
        range_end = index->cend();
        break;
      }
      default: {
        Fail("Unsupported comparison type encountered");
      }
    }
    index_ranges.emplace_back(IndexRange{range_begin, range_end});
  }
  return index_ranges;
}

void JoinIndex::_append_matches(const AbstractIndex::Iterator& range_begin, const AbstractIndex::Iterator& range_end,
                                const ChunkOffset probe_chunk_offset, const ChunkID probe_chunk_id,
                                const ChunkID index_chunk_id) {
  const auto num_index_matches = std::distance(range_begin, range_end);

  if (num_index_matches == 0) {
    return;
  }

  // Remember the matches for outer joins
  if ((_mode == JoinMode::Left && _index_side == IndexSide::Right) ||
      (_mode == JoinMode::Right && _index_side == IndexSide::Left) || _mode == JoinMode::FullOuter) {
    _probe_matches[probe_chunk_id][probe_chunk_offset] = true;
  }

  // we replicate the probe side value for each index side value
  std::fill_n(std::back_inserter(*_probe_pos_list), num_index_matches, RowID{probe_chunk_id, probe_chunk_offset});

  std::transform(range_begin, range_end, std::back_inserter(*_index_pos_list),
                 [index_chunk_id](ChunkOffset index_chunk_offset) {
                   return RowID{index_chunk_id, index_chunk_offset};
                 });

  if ((_mode == JoinMode::Left && _index_side == IndexSide::Left) ||
      (_mode == JoinMode::Right && _index_side == IndexSide::Right) || _mode == JoinMode::FullOuter) {
    std::for_each(range_begin, range_end, [this, index_chunk_id](ChunkOffset index_chunk_offset) {
      _index_matches[index_chunk_id][index_chunk_offset] = true;
    });
  }
}

void JoinIndex::_append_matches_dereferenced(const ChunkID& probe_chunk_id, const ChunkOffset& probe_chunk_offset,
                                             const PosList& index_table_matches) {
  for (const auto& index_side_row_id : index_table_matches) {
    _probe_pos_list->emplace_back(RowID{probe_chunk_id, probe_chunk_offset});
    _index_pos_list->emplace_back(index_side_row_id);
    _index_pos_dereferenced.emplace_back(true);
  }
}

void JoinIndex::_append_matches_non_inner(const bool is_semi_or_anti_join) {
  // For Full Outer and Left Join we need to add all unmatched rows for the probe side
  if ((_mode == JoinMode::Left && _index_side == IndexSide::Right) ||
      (_mode == JoinMode::Right && _index_side == IndexSide::Left) || _mode == JoinMode::FullOuter) {
    const auto chunk_count = _probe_input_table->chunk_count();
    for (ChunkID probe_chunk_id{0}; probe_chunk_id < chunk_count; ++probe_chunk_id) {
      for (ChunkOffset chunk_offset{0}; chunk_offset < static_cast<ChunkOffset>(_probe_matches[probe_chunk_id].size());
           ++chunk_offset) {
        if (!_probe_matches[probe_chunk_id][chunk_offset]) {
          _probe_pos_list->emplace_back(RowID{probe_chunk_id, chunk_offset});
          _index_pos_list->emplace_back(NULL_ROW_ID);
        }
      }
    }
  }

  // For Full Outer and Right Join we need to add all unmatched rows for the index side.
  if ((_mode == JoinMode::Left && _index_side == IndexSide::Left) ||
      (_mode == JoinMode::Right && _index_side == IndexSide::Right) || _mode == JoinMode::FullOuter) {
    const auto chunk_count = _index_matches.size();
    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
      for (ChunkOffset chunk_offset{0}; chunk_offset < static_cast<ChunkOffset>(_index_matches[chunk_id].size());
           ++chunk_offset) {
        if (!_index_matches[chunk_id][chunk_offset]) {
          _index_pos_list->emplace_back(RowID{chunk_id, chunk_offset});
          _probe_pos_list->emplace_back(NULL_ROW_ID);
        }
      }
    }
  }

  _probe_pos_list->shrink_to_fit();
  _index_pos_list->shrink_to_fit();

  // Write PosLists for Semi/Anti Joins, which so far haven't written any results to the PosLists
  // We use `_probe_matches` to determine whether a tuple from the probe side found a match.
  if (is_semi_or_anti_join) {
    const auto invert = _mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue;

    const auto chunk_count = _probe_input_table->chunk_count();
    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = _probe_input_table->get_chunk(chunk_id);
      Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

      const auto chunk_size = chunk->size();
      for (ChunkOffset chunk_offset{0}; chunk_offset < chunk_size; ++chunk_offset) {
        if (_probe_matches[chunk_id][chunk_offset] ^ invert) {
          _probe_pos_list->emplace_back(RowID{chunk_id, chunk_offset});
        }
      }
    }
  }
}

void JoinIndex::_write_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                                       const std::shared_ptr<PosList>& pos_list) {
  // Add segments from table to output chunk
  const auto column_count = input_table->column_count();
  for (ColumnID column_id{0}; column_id < column_count; ++column_id) {
    std::shared_ptr<BaseSegment> segment;

    if (input_table->type() == TableType::References) {
      if (input_table->chunk_count() > 0) {
        auto new_pos_list = std::make_shared<PosList>();

        ChunkID current_chunk_id{0};

        const auto first_chunk_input_table = input_table->get_chunk(ChunkID{0});
        Assert(first_chunk_input_table, "Did not expect deleted chunk here.");  // see #1686
        auto reference_segment =
            std::static_pointer_cast<const ReferenceSegment>(first_chunk_input_table->get_segment(column_id));

        // de-reference to the correct RowID so the output can be used in a Multi Join
        for (ChunkOffset pos_list_offset{0}; pos_list_offset < static_cast<ChunkOffset>(pos_list->size());
             ++pos_list_offset) {
          const auto& row = (*pos_list)[pos_list_offset];
          if (row.is_null()) {
            new_pos_list->push_back(NULL_ROW_ID);
            continue;
          }
          if (pos_list == _index_pos_list && _index_pos_dereferenced[pos_list_offset]) {
            new_pos_list->push_back(row);
          } else {
            if (row.chunk_id != current_chunk_id) {
              current_chunk_id = row.chunk_id;

              const auto chunk = input_table->get_chunk(current_chunk_id);
              Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

              reference_segment = std::dynamic_pointer_cast<const ReferenceSegment>(chunk->get_segment(column_id));
            }
            new_pos_list->push_back((*reference_segment->pos_list())[row.chunk_offset]);
          }
        }
        segment = std::make_shared<ReferenceSegment>(reference_segment->referenced_table(),
                                                     reference_segment->referenced_column_id(), new_pos_list);
      } else {
        // If there are no Chunks in the input_table, we can't deduce the Table that input_table is referencing to.
        // pos_list will contain only NULL_ROW_IDs anyway, so it doesn't matter which Table the ReferenceSegment that
        // we output is referencing. HACK, but works fine: we create a dummy table and let the ReferenceSegment ref
        // it.
        const auto dummy_table = Table::create_dummy_table(input_table->column_definitions());
        segment = std::make_shared<ReferenceSegment>(dummy_table, column_id, pos_list);
      }
    } else {
      segment = std::make_shared<ReferenceSegment>(input_table, column_id, pos_list);
    }

    output_segments.push_back(segment);
  }
}

void JoinIndex::_on_cleanup() {
  _output_table.reset();
  _probe_pos_list.reset();
  _index_pos_list.reset();
  _probe_matches.clear();
  _index_matches.clear();
}

void JoinIndex::PerformanceData::output_to_stream(std::ostream& stream, DescriptionMode description_mode) const {
  OperatorPerformanceData::output_to_stream(stream, description_mode);

  stream << (description_mode == DescriptionMode::SingleLine ? " / " : "\\n");
  stream << std::to_string(chunks_scanned_with_index) << " of "
         << std::to_string(chunks_scanned_with_index + chunks_scanned_without_index) << " chunks used an index";
}

}  // namespace opossum
