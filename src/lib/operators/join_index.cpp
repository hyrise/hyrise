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
// #include "operators/index_scan.hpp"
// #include "operators/table_wrapper.hpp"
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
  if (_index_side == IndexSide::Left) {
    _probe_input_table = input_table_right();
    _index_input_table = input_table_left();
  } else {
    _probe_input_table = input_table_left();
    _index_input_table = input_table_right();
  }

  _index_matches.resize(_index_input_table->chunk_count());
  _probe_matches.resize(_probe_input_table->chunk_count());

  if (_mode == JoinMode::Inner &&
      ((_index_side == IndexSide::Right && input_table_right()->type() == TableType::References) ||
       (_index_side == IndexSide::Left && input_table_left()->type() == TableType::References)) &&
      _secondary_predicates.empty()) {
    return _perform_reference_join();
  } else {
    return _perform_data_join();
  }
}

std::shared_ptr<const Table> JoinIndex::_perform_data_join() {
  Assert(supports(_mode, _primary_predicate.predicate_condition,
                  input_table_left()->column_data_type(_primary_predicate.column_ids.first),
                  input_table_right()->column_data_type(_primary_predicate.column_ids.second),
                  !_secondary_predicates.empty()),
         "JoinHash doesn't support these parameters");

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

  // Scan all chunks for index input
  for (ChunkID index_chunk_id = ChunkID{0}; index_chunk_id < _index_input_table->chunk_count(); ++index_chunk_id) {
    const auto index_chunk = _index_input_table->get_chunk(index_chunk_id);
    const auto indices = index_chunk->get_indices(std::vector<ColumnID>{_adjusted_primary_predicate.column_ids.second});
    std::shared_ptr<BaseIndex> index = nullptr;

    if (!indices.empty()) {
      // We assume the first index to be efficient for our join
      // as we do not want to spend time on evaluating the best index inside of this join loop
      index = indices.front();
    }

    // Scan all chunks from the pruning side input
    if (index) {
      for (ChunkID probe_chunk_id = ChunkID{0}; probe_chunk_id < _probe_input_table->chunk_count(); ++probe_chunk_id) {
        const auto probe_segment =
            _probe_input_table->get_chunk(probe_chunk_id)->get_segment(_adjusted_primary_predicate.column_ids.first);
        segment_with_iterators(*probe_segment, [&](auto it, const auto end) {
          _join_two_segments_using_index(it, end, probe_chunk_id, index_chunk_id, index);
        });
      }
      performance_data.chunks_scanned_with_index++;
    } else {
      // Fall back to NestedLoopJoin
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
        JoinNestedLoop::_join_two_untyped_segments(*probe_segment, *index_segment, probe_chunk_id, index_chunk_id,
                                                   params);
      }
      performance_data.chunks_scanned_without_index++;
    }
  }

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

std::shared_ptr<const Table> JoinIndex::_perform_reference_join() {
  // std::cout << "REF INDEX JOIN"
  //           << "\n";
  // std::vector<ColumnID> data_table_index_column_ids;

  // // get referenced data table
  // std::shared_ptr<const Table> referenced_data_table;
  // if (!input_table_right()->chunks().empty() && !input_table_right()->chunks()[0]->segments().empty()) {
  //   const auto& first_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(
  //       input_table_right()->chunks()[0]->segments()[_primary_predicate.column_ids.second]);
  //   if (first_reference_segment != nullptr) {
  //     referenced_data_table = first_reference_segment->referenced_table();
  //     // Experiment assumption (this is not valid generally):
  //     // Each reference segment of the reference table references the same ColumnId
  //     data_table_index_column_ids.emplace_back(first_reference_segment->referenced_column_id());
  //   }
  // }
  // // use _perform_join if the referenced data table has no index
  // Assert(referenced_data_table != nullptr, "ReferenceSegment has no reference table.");
  // if (!referenced_data_table->indexes_statistics().empty()) {
  //   // Assumption: Original data table of the right input table has
  //   // an index for each segment that is evaluated for the join

  //   // build the position list for the right input table
  //   auto input_right_table_positions = PosList{};
  //   input_right_table_positions.reserve(input_table_right()->row_count());
  //   for (const auto& chunk : input_table_right()->chunks()) {
  //     if (!chunk->segments().empty()) {
  //       const auto& reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(chunk->segments()[0]);
  //       Assert(reference_segment != nullptr, "Segment of reference table is not of type ReferenceSegment.");
  //       const auto& reference_segment_pos_list = reference_segment->pos_list();
  //       input_right_table_positions.insert(input_right_table_positions.end(), reference_segment_pos_list->begin(),
  //                                          reference_segment_pos_list->end());
  //     }
  //   }

  //   // std::cout << "input_right_table_positions sorted: ";
  //   // std::cout << std::is_sorted(input_right_table_positions.begin(), input_right_table_positions.end()) << "\n";

  //   std::sort(input_right_table_positions.begin(), input_right_table_positions.end());

  //   //    const auto first_row_id = input_right_table_positions.front();
  //   //    const auto last_row_id = input_right_table_positions.back();

  //   _pos_list_left = std::make_shared<PosList>();
  //   _pos_list_right = std::make_shared<PosList>();

  //   _pos_list_left->reserve(input_right_table_positions.size());
  //   _pos_list_right->reserve(input_right_table_positions.size());

  //   //  iterate over the left join column
  //   //    for each value vl of that column:
  //   //      execute an index scan on the referenced_data_table
  //   //      get the global posList (right_data_table_matches) for the scan on the referenced_data_table
  //   //      sort input_right_table_positions
  //   //      sort right_data_table_matches
  //   //      input_right_table_matches = intersection(input_right_table_positions, right_data_table_matches)
  //   //      add the RowID of vl as often as the size of input_right_table_matches to _pos_list_left
  //   //      add input_right_table_matches to _pos_list_right

  //   const auto& referenced_data_table_wrapper = std::make_shared<TableWrapper>(referenced_data_table);
  //   referenced_data_table_wrapper->execute();

  //   auto left_chunk_id = ChunkID{0};
  //   for (const auto& chunk : input_table_left()->chunks()) {
  //     const auto& left_segment = chunk->get_segment(_primary_predicate.column_ids.first);

  //     resolve_data_and_segment_type(*left_segment, [&](auto type, auto& typed_segment) {
  //       using Type = typename decltype(type)::type;
  //       auto iterable = create_iterable_from_segment<Type>(typed_segment);

  //       auto left_chunk_offset = ChunkOffset{0};
  //       iterable.with_iterators([&](auto iterator, auto end) {
  //         for (; iterator != end; ++iterator) {
  //           const auto& value = *iterator;
  //           const std::vector<AllTypeVariant> right_values{AllTypeVariant{value.value()}};
  //           // WARNING! The SegmentIndexType is hard coded for the benchmark experiment here.
  //           // TODO(anyone) modify passing the SegmentIndexType
  //           const auto& index_scan_on_data_table = std::make_shared<IndexScan>(
  //               referenced_data_table_wrapper, SegmentIndexType::GroupKey, data_table_index_column_ids,
  //               _primary_predicate.predicate_condition, right_values);
  //           auto right_data_table_matches = index_scan_on_data_table->execute_matches_calculation();

  //           std::sort(right_data_table_matches->begin(), right_data_table_matches->end());
  //           auto input_right_table_matches = PosList{};

  //           std::set_intersection(input_right_table_positions.begin(), input_right_table_positions.end(),
  //                                 right_data_table_matches->begin(), right_data_table_matches->end(),
  //                                 std::back_inserter(input_right_table_matches));
  //           _append_matches(left_chunk_id, left_chunk_offset, input_right_table_matches);
  //           ++left_chunk_offset;
  //         }
  //       });
  //     });
  //     ++left_chunk_id;
  //   }

  //   // write output chunks
  //   Segments output_segments;

  //   _write_output_segments(output_segments, input_table_left(), _pos_list_left);
  //   _write_output_segments(output_segments, input_table_right(), _pos_list_right);
  //   return _build_output_table({std::make_shared<Chunk>(output_segments)});
  // }
  // use the fallback solution (nested loop)
  return _perform_data_join();
}

void JoinIndex::_append_matches(const ChunkID& probe_chunk_id, const ChunkOffset& probe_chunk_offset,
                                const PosList& index_table_matches) {
  for (const auto& index_side_row_id : index_table_matches) {
    _probe_pos_list->emplace_back(RowID{probe_chunk_id, probe_chunk_offset});
    _index_pos_list->emplace_back((index_side_row_id));
  }
}

// join loop that joins two segments of two columns using an iterator for the probe side,
// and an index for the index side
template <typename ProbeIterator>
void JoinIndex::_join_two_segments_using_index(ProbeIterator probe_iter, ProbeIterator probe_end,
                                               const ChunkID probe_chunk_id, const ChunkID index_chunk_id,
                                               const std::shared_ptr<BaseIndex>& index) {
  for (; probe_iter != probe_end; ++probe_iter) {
    const auto probe_side_value = *probe_iter;
    if (probe_side_value.is_null()) continue;

    auto range_begin = BaseIndex::Iterator{};
    auto range_end = BaseIndex::Iterator{};

    switch (_adjusted_primary_predicate.predicate_condition) {
      case PredicateCondition::Equals: {
        range_begin = index->lower_bound({probe_side_value.value()});
        range_end = index->upper_bound({probe_side_value.value()});
        break;
      }
      case PredicateCondition::NotEquals: {
        // first, get all values less than the search value
        range_begin = index->cbegin();
        range_end = index->lower_bound({probe_side_value.value()});

        _append_matches(range_begin, range_end, probe_side_value.chunk_offset(), probe_chunk_id, index_chunk_id);

        // set range for second half to all values greater than the search value
        range_begin = index->upper_bound({probe_side_value.value()});
        range_end = index->cend();
        break;
      }
      case PredicateCondition::GreaterThan: {
        range_begin = index->cbegin();
        range_end = index->lower_bound({probe_side_value.value()});
        break;
      }
      case PredicateCondition::GreaterThanEquals: {
        range_begin = index->cbegin();
        range_end = index->upper_bound({probe_side_value.value()});
        break;
      }
      case PredicateCondition::LessThan: {
        range_begin = index->upper_bound({probe_side_value.value()});
        range_end = index->cend();
        break;
      }
      case PredicateCondition::LessThanEquals: {
        range_begin = index->lower_bound({probe_side_value.value()});
        range_end = index->cend();
        break;
      }
      default:
        Fail("Unsupported comparison type encountered");
    }

    _append_matches(range_begin, range_end, probe_side_value.chunk_offset(), probe_chunk_id, index_chunk_id);
  }
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
