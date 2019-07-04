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
#include "operators/index_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "storage/index/base_index.hpp"
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

bool JoinIndex::supports(JoinMode join_mode, PredicateCondition predicate_condition, DataType left_data_type,
                         DataType right_data_type, bool secondary_predicates) {
  return !secondary_predicates;
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

const std::string JoinIndex::name() const { return "JoinIndex"; }

std::shared_ptr<AbstractOperator> JoinIndex::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<JoinIndex>(copied_input_left, copied_input_right, _mode, _primary_predicate,
                                     std::vector<OperatorJoinPredicate>{}, _index_side);
}

void JoinIndex::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> JoinIndex::_on_execute() {
  Assert(supports(_mode, _primary_predicate.predicate_condition,
                  input_table_left()->column_data_type(_primary_predicate.column_ids.first),
                  input_table_right()->column_data_type(_primary_predicate.column_ids.second),
                  !_secondary_predicates.empty()),
         "JoinHash doesn't support these parameters");

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

  const auto track_probe_matches = _mode == JoinMode::FullOuter || _mode == JoinMode::Left || is_semi_or_anti_join;
  const auto track_index_matches = _mode == JoinMode::FullOuter || _mode == JoinMode::Right;

  if (track_probe_matches) {
    for (ChunkID probe_chunk_id = ChunkID{0}; probe_chunk_id < _probe_input_table->chunk_count(); ++probe_chunk_id) {
      _probe_matches[probe_chunk_id].resize(_probe_input_table->get_chunk(probe_chunk_id)->size());
    }
  }

  if (track_index_matches) {
    for (ChunkID index_chunk_id = ChunkID{0}; index_chunk_id < _index_input_table->chunk_count(); ++index_chunk_id) {
      _index_matches[index_chunk_id].resize(_index_input_table->get_chunk(index_chunk_id)->size());
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

  // TODO(Marcel) implement this branch correctly (INDEX REF JOIN)
  // if (false && _mode == JoinMode::Inner &&
  //     ((_index_side == IndexSide::Right && input_table_right()->type() == TableType::References) ||
  //      (_index_side == IndexSide::Left && input_table_left()->type() == TableType::References)) &&
  //     _secondary_predicates.empty()) {  // INDEX REFERENCE JOIN
  //   std::vector<ColumnID> data_table_index_column_ids;

  //   // get referenced data table
  //   std::shared_ptr<const Table> referenced_data_table;
  //   if (!_index_input_table->chunks().empty() && !_index_input_table->chunks()[0]->segments().empty()) {
  //     const auto& first_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(
  //         _index_input_table->chunks()[0]->segments()[_primary_predicate.column_ids.second]);
  //     Assert(first_reference_segment != nullptr,
  //            "Non-empty index intput table (reference table) has to have only reference segments.");
  //     referenced_data_table = first_reference_segment->referenced_table();
  //     // Experiment assumption (this is not valid generally):
  //     // Each reference segment of the reference table references the same ColumnId
  //     data_table_index_column_ids.emplace_back(first_reference_segment->referenced_column_id());
  //   } else {
  //     return _build_output_table({std::make_shared<Chunk>(Segments{})});
  //   }
  //   Assert(referenced_data_table != nullptr, "ReferenceSegment has no reference table.");
  //   if (!referenced_data_table->indexes_statistics().empty()) {
  //     // Assumption: Original data table of the right input table has
  //     // an index for each segment that is evaluated for the join

  //     // build the position list for the index input table
  //     auto index_input_table_positions = PosList{};
  //     index_input_table_positions.reserve(_index_input_table->row_count());
  //     for (const auto& index_chunk : _index_input_table->chunks()) {
  //       if (!index_chunk->segments().empty()) {
  //         const auto& reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(index_chunk->segments()[0]);
  //         Assert(reference_segment != nullptr, "Segment of reference table is not of type ReferenceSegment.");
  //         const auto& reference_segment_pos_list = reference_segment->pos_list();
  //         index_input_table_positions.insert(index_input_table_positions.end(), reference_segment_pos_list->begin(),
  //                                            reference_segment_pos_list->end());
  //       }
  //     }

  //     std::sort(index_input_table_positions.begin(), index_input_table_positions.end());

  //     //  iterate over the probe join column
  //     //    for each value vl of that column:
  //     //      execute an index scan on the referenced_data_table
  //     //      get the global posList (index_data_table_matches) for the scan on the referenced_data_table
  //     //      sort index_input_table_positions
  //     //      sort index_data_table_matches
  //     //      index_table_matches = intersection(index_input_table_positions, index_data_table_matches)
  //     //      add the RowID of vl as often as the size of index_table_matches to _probe_pos_list
  //     //      add index_table_matches to _index_pos_list

  //     const auto& referenced_data_table_wrapper = std::make_shared<TableWrapper>(referenced_data_table);
  //     referenced_data_table_wrapper->execute();

  //     auto probe_chunk_id = ChunkID{0};
  //     for (const auto& probe_chunk : _probe_input_table->chunks()) {
  //       const auto& probe_segment = probe_chunk->get_segment(_primary_predicate.column_ids.first);
  //       //// new approach
  //       segment_with_iterators(*probe_segment, [&](auto probe_side_iter, auto end) {
  //         // for (; probe_side_iter != end; ++probe_side_iter) {
  //         //   const auto& probe_value = *probe_side_iter;
  //         //   const std::vector<AllTypeVariant> index_scan_values{AllTypeVariant{probe_value.value()}};
  //         //   // WARNING! The SegmentIndexType is hard coded for the benchmark experiment here.
  //         //   // TODO(Marcel) modify passing the SegmentIndexType
  //         //   // TODO(Marcel) avoid using the operator, see _fallback_nested_loop
  //         //   const auto& index_scan_on_data_table = std::make_shared<IndexScan>(
  //         //       referenced_data_table_wrapper, SegmentIndexType::GroupKey, data_table_index_column_ids,
  //         //       _primary_predicate.predicate_condition, index_scan_values);
  //         //   auto index_data_table_matches = index_scan_on_data_table->execute_matches_calculation();

  //         //   std::sort(index_data_table_matches->begin(), index_data_table_matches->end());
  //         //   auto index_table_matches = PosList{};

  //         //   std::set_intersection(index_input_table_positions.begin(), index_input_table_positions.end(),
  //         //                         index_data_table_matches->begin(), index_data_table_matches->end(),
  //         //                         std::back_inserter(index_table_matches));
  //         //   _append_matches(probe_chunk_id, probe_chunk_offset, index_table_matches);
  //         //   ++probe_chunk_offset;
  //         // }
  //       });
  //       ////
  //       ++probe_chunk_id;
  //     }
  //   } else {
  //     // fallback to nested loop join
  //     for (ChunkID index_chunk_id = ChunkID{0}; index_chunk_id < _index_input_table->chunk_count(); ++index_chunk_id) {
  //       const auto index_chunk = _index_input_table->get_chunk(index_chunk_id);
  //       _fallback_nested_loop(index_chunk_id, track_probe_matches, track_index_matches, is_semi_or_anti_join,
  //                             secondary_predicate_evaluator);
  //     }
  //   }
  // } else {  // INDEX DATA JOIN
    // Scan all chunks for index input
    for (ChunkID index_chunk_id = ChunkID{0}; index_chunk_id < _index_input_table->chunk_count(); ++index_chunk_id) {
      const auto index_chunk = _index_input_table->get_chunk(index_chunk_id);
      const auto indices =
          index_chunk->get_indices(std::vector<ColumnID>{_adjusted_primary_predicate.column_ids.second});
      std::shared_ptr<BaseIndex> index = nullptr;

      if (!indices.empty()) {
        // We assume the first index to be efficient for our join
        // as we do not want to spend time on evaluating the best index inside of this join loop
        index = indices.front();
      }

      // Scan all chunks from the pruning side input
      if (index) {
        for (ChunkID probe_chunk_id = ChunkID{0}; probe_chunk_id < _probe_input_table->chunk_count();
             ++probe_chunk_id) {
          const auto probe_segment =
              _probe_input_table->get_chunk(probe_chunk_id)->get_segment(_adjusted_primary_predicate.column_ids.first);
          segment_with_iterators(*probe_segment, [&](auto it, const auto end) {
            _join_two_segments_using_index(it, end, probe_chunk_id, index_chunk_id, index);
          });
        }
        performance_data.chunks_scanned_with_index++;
      } else {
        _fallback_nested_loop(index_chunk_id, track_probe_matches, track_index_matches, is_semi_or_anti_join,
                              secondary_predicate_evaluator);
      }
    }

    _append_matches_non_inner(is_semi_or_anti_join);
  // }

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
  auto& performance_data = static_cast<PerformanceData&>(*_performance_data);

  const auto index_segment =
      _index_input_table->get_chunk(index_chunk_id)->get_segment(_adjusted_primary_predicate.column_ids.second);
  for (ChunkID probe_chunk_id = ChunkID{0}; probe_chunk_id < _probe_input_table->chunk_count(); ++probe_chunk_id) {
    const auto probe_segment =
        _probe_input_table->get_chunk(probe_chunk_id)->get_segment(_adjusted_primary_predicate.column_ids.first);
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
  performance_data.chunks_scanned_without_index++;
}

// join loop that joins two segments of two columns using an iterator for the probe side,
// and an index for the index side
template <typename ProbeIterator>
void JoinIndex::_join_two_segments_using_index(ProbeIterator probe_iter, ProbeIterator probe_end,
                                               const ChunkID probe_chunk_id, const ChunkID index_chunk_id,
                                               const std::shared_ptr<BaseIndex>& index) {
  for (; probe_iter != probe_end; ++probe_iter) {
    const auto probe_side_position = *probe_iter;
    const auto index_ranges = _index_ranges_for_value(probe_side_position, index);
    for(const auto& index_range : index_ranges){
      _append_matches(std::get<0>(index_range), std::get<1>(index_range), probe_side_position.chunk_offset(), probe_chunk_id, index_chunk_id);
    }
  }
}

template <typename SegmentPosition>
const std::vector<IndexRange> JoinIndex::_index_ranges_for_value(SegmentPosition probe_side_position, const std::shared_ptr<BaseIndex>& index) const{
  std::vector<IndexRange> index_ranges{};
  index_ranges.reserve(2);

  if (!probe_side_position.is_null()){
  auto range_begin = BaseIndex::Iterator{};
  auto range_end = BaseIndex::Iterator{};

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

void JoinIndex::_append_matches(const BaseIndex::Iterator& range_begin, const BaseIndex::Iterator& range_end,
                                const ChunkOffset probe_chunk_offset, const ChunkID probe_chunk_id,
                                const ChunkID index_chunk_id) {
  const auto num_index_matches = std::distance(range_begin, range_end);

  if (num_index_matches == 0) {
    return;
  }

  // Remember the matches for outer joins
  if (_mode == JoinMode::Left || _mode == JoinMode::FullOuter) {
    _probe_matches[probe_chunk_id][probe_chunk_offset] = true;
  }

  // we replicate the probe side value for each index side value
  std::fill_n(std::back_inserter(*_probe_pos_list), num_index_matches, RowID{probe_chunk_id, probe_chunk_offset});

  std::transform(range_begin, range_end, std::back_inserter(*_index_pos_list),
                 [index_chunk_id](ChunkOffset index_chunk_offset) {
                   return RowID{index_chunk_id, index_chunk_offset};
                 });

  if (_mode == JoinMode::FullOuter || _mode == JoinMode::Right) {
    std::for_each(range_begin, range_end, [this, index_chunk_id](ChunkOffset index_chunk_offset) {
      _index_matches[index_chunk_id][index_chunk_offset] = true;
    });
  }
}

void JoinIndex::_append_matches(const ChunkID& probe_chunk_id, const ChunkOffset& probe_chunk_offset,
                                const PosList& index_table_matches) {
  for (const auto& index_side_row_id : index_table_matches) {
    _probe_pos_list->emplace_back(RowID{probe_chunk_id, probe_chunk_offset});
    _index_pos_list->emplace_back((index_side_row_id));
  }
}

void JoinIndex::_append_matches_non_inner(const bool is_semi_or_anti_join) {
  // For Full Outer and Left Join we need to add all unmatched rows for the probe side
  if (_mode == JoinMode::Left || _mode == JoinMode::FullOuter) {
    for (ChunkID probe_chunk_id = ChunkID{0}; probe_chunk_id < _probe_input_table->chunk_count(); ++probe_chunk_id) {
      for (ChunkOffset chunk_offset{0}; chunk_offset < _probe_matches[probe_chunk_id].size(); ++chunk_offset) {
        if (!_probe_matches[probe_chunk_id][chunk_offset]) {
          _probe_pos_list->emplace_back(RowID{probe_chunk_id, chunk_offset});
          _index_pos_list->emplace_back(NULL_ROW_ID);
        }
      }
    }
  }

  // For Full Outer and Right Join we need to add all unmatched rows for the index side.
  if (_mode == JoinMode::FullOuter || _mode == JoinMode::Right) {
    for (ChunkID chunk_id{0}; chunk_id < _index_matches.size(); ++chunk_id) {
      for (ChunkOffset chunk_offset{0}; chunk_offset < _index_matches[chunk_id].size(); ++chunk_offset) {
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
  // We use `probe_matches_by_chunk` to determine whether a tuple from the probe side found a match.
  if (is_semi_or_anti_join) {
    const auto invert = _mode == JoinMode::AntiNullAsFalse || _mode == JoinMode::AntiNullAsTrue;

    for (auto chunk_id = ChunkID{0}; chunk_id < _probe_input_table->chunk_count(); ++chunk_id) {
      const auto chunk_size = _probe_input_table->get_chunk(chunk_id)->size();
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
        if (_probe_matches[chunk_id][chunk_offset] ^ invert) {
          _probe_pos_list->emplace_back(chunk_id, chunk_offset);
        }
      }
    }
  }
}

void JoinIndex::_write_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                                       std::shared_ptr<PosList> pos_list) {
  // Add segments from table to output chunk
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    std::shared_ptr<BaseSegment> segment;

    if (input_table->type() == TableType::References) {
      if (input_table->chunk_count() > 0) {
        auto new_pos_list = std::make_shared<PosList>();

        ChunkID current_chunk_id{0};

        auto reference_segment = std::static_pointer_cast<const ReferenceSegment>(
            input_table->get_chunk(ChunkID{0})->get_segment(column_id));

        // de-reference to the correct RowID so the output can be used in a Multi Join
        for (const auto& row : *pos_list) {
          if (row.is_null()) {
            new_pos_list->push_back(NULL_ROW_ID);
            continue;
          }
          if (row.chunk_id != current_chunk_id) {
            current_chunk_id = row.chunk_id;

            reference_segment = std::dynamic_pointer_cast<const ReferenceSegment>(
                input_table->get_chunk(current_chunk_id)->get_segment(column_id));
          }
          new_pos_list->push_back((*reference_segment->pos_list())[row.chunk_offset]);
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
