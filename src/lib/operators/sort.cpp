#include "sort.hpp"

#include <mutex>

#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

Sort::Sort(const std::shared_ptr<const AbstractOperator>& in, const std::vector<SortColumnDefinition>& sort_definitions,
           const size_t output_chunk_size)
    : AbstractReadOnlyOperator(OperatorType::Sort, in),
      _sort_definitions(sort_definitions),
      _output_chunk_size(output_chunk_size) {}

const std::vector<SortColumnDefinition>& Sort::sort_definitions() const { return _sort_definitions; }

const std::string& Sort::name() const {
  static const auto name = std::string{"Sort"};
  return name;
}

std::shared_ptr<AbstractOperator> Sort::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Sort>(copied_input_left, _sort_definitions, _output_chunk_size);
}

void Sort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Sort::_on_execute() {
  _sort_definition_data_types.resize(_sort_definitions.size());
  for (auto column_id = ColumnID{0}; column_id < _sort_definitions.size(); ++column_id) {
    auto sort_definition = _sort_definitions[column_id];
    _sort_definition_data_types[column_id] = input_table_left()->column_data_type(sort_definition.column);
  }
  _validate_sort_definitions();

  const auto in_table = input_table_left();

  // TODO(anyone): Replace this naive parallel implementation by something more optimized
  // 1. In parallel, sort each of the input table's chunks (by all to-be-sorted columns) in one job task
  std::mutex output_mutex;
  const auto chunk_count = in_table->chunk_count();

  auto output_pos_lists = std::vector<std::shared_ptr<PosList>>{};
  output_pos_lists.reserve(chunk_count);

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(chunk_count);

  for (ChunkID chunk_id{0u}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk_in = in_table->get_chunk(chunk_id);
    // chunk_in – Copy by value since copy by reference is not possible due to the limited scope of the for-iteration.
    auto job_task =
        std::make_shared<JobTask>([this, chunk_id, chunk_in, &in_table, &output_mutex, &output_pos_lists]() {
          std::shared_ptr<PosList> previously_sorted_pos_list = std::shared_ptr<PosList>(nullptr);
          for (auto column_id = _sort_definitions.size(); column_id-- != 0;) {
            auto sort_definition = _sort_definitions[column_id];
            DataType data_type = _sort_definition_data_types[column_id];

            resolve_data_type(data_type, [&](auto type) {
              using ColumnDataType = typename decltype(type)::type;
              auto sort_single_col_impl = SortImpl<ColumnDataType>(in_table, sort_definition.column,
                                                                   sort_definition.order_by_mode, _output_chunk_size);
              previously_sorted_pos_list =
                  sort_single_col_impl.sort_one_column_of_chunk(chunk_id, previously_sorted_pos_list);
            });
          }
          std::lock_guard<std::mutex> lock(output_mutex);
          output_pos_lists.emplace_back(previously_sorted_pos_list);
        });
    jobs.push_back(job_task);
    job_task->schedule();
  }

  Hyrise::get().scheduler()->wait_for_tasks(jobs);

  // 2. Then, merge the resulting PosLists and sort the complete table based on this pre-sorted concatenated PosLists
  std::shared_ptr<PosList> merged_pos_list = std::make_shared<PosList>();
  if (chunk_count > 1) {
    // Concatenate the sorted partial PosLists
    for (const auto& pos_list : output_pos_lists) {
      merged_pos_list->reserve(merged_pos_list->size() + pos_list->size());
      merged_pos_list->insert(merged_pos_list->end(), pos_list->begin(), pos_list->end());
    }

    // Sort the concatenated PosList by the given sort definitions
    for (auto column_id = _sort_definitions.size(); column_id-- != 0;) {
      auto sort_definition = _sort_definitions[column_id];
      DataType data_type = _sort_definition_data_types[column_id];

      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        auto sort_single_col_impl = SortImpl<ColumnDataType>(input_table_left(), sort_definition.column,
                                                             sort_definition.order_by_mode, _output_chunk_size);
        merged_pos_list = sort_single_col_impl.sort_one_column(merged_pos_list);
      });
    }
  } else {
    merged_pos_list = output_pos_lists[0];
  }

  return _get_materialized_output(merged_pos_list);
}

std::shared_ptr<const Table> Sort::_get_materialized_output(const std::shared_ptr<PosList>& pos_list) {
  // First we create a new table as the output
  auto output =
      std::make_shared<Table>(_input_left->get_output()->column_definitions(), TableType::Data, _output_chunk_size);

  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408

  // After we created the output table and initialized the column structure, we can start adding values. Because the
  // values are not ordered by input chunks anymore, we can't process them chunk by chunk. Instead the values are
  // copied column by column for each output row. For each column in a row we visit the input segment with a reference
  // to the output segment. This enables for the SortImplMaterializeOutput class to ignore the column types during the
  // copying of the values.
  const auto row_count_out = pos_list->size();

  // Ceiling of integer division
  const auto div_ceil = [](auto x, auto y) { return (x + y - 1u) / y; };

  const auto chunk_count_out = div_ceil(row_count_out, _output_chunk_size);

  // Vector of segments for each chunk
  std::vector<Segments> output_segments_by_chunk(chunk_count_out);

  // Materialize segment-wise
  for (ColumnID column_id{0u}; column_id < output->column_count(); ++column_id) {
    const auto column_data_type = output->column_data_type(column_id);

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto chunk_it = output_segments_by_chunk.begin();
      auto chunk_offset_out = 0u;

      auto value_segment_value_vector = pmr_concurrent_vector<ColumnDataType>();
      auto value_segment_null_vector = pmr_concurrent_vector<bool>();

      value_segment_value_vector.reserve(row_count_out);
      value_segment_null_vector.reserve(row_count_out);

      auto segment_ptr_and_accessor_by_chunk_id =
          std::unordered_map<ChunkID, std::pair<std::shared_ptr<const BaseSegment>,
                                                std::shared_ptr<AbstractSegmentAccessor<ColumnDataType>>>>();
      segment_ptr_and_accessor_by_chunk_id.reserve(row_count_out);

      for (auto row_index = 0u; row_index < row_count_out; ++row_index) {
        const auto [chunk_id, chunk_offset] = pos_list->operator[](row_index);

        auto& segment_ptr_and_typed_ptr_pair = segment_ptr_and_accessor_by_chunk_id[chunk_id];
        auto& base_segment = segment_ptr_and_typed_ptr_pair.first;
        auto& accessor = segment_ptr_and_typed_ptr_pair.second;

        if (!base_segment) {
          base_segment = _input_left->get_output()->get_chunk(chunk_id)->get_segment(column_id);
          accessor = create_segment_accessor<ColumnDataType>(base_segment);
        }

        // If the input segment is not a ReferenceSegment, we can take a fast(er) path
        if (accessor) {
          const auto typed_value = accessor->access(chunk_offset);
          const auto is_null = !typed_value;
          value_segment_value_vector.push_back(is_null ? ColumnDataType{} : typed_value.value());
          value_segment_null_vector.push_back(is_null);
        } else {
          const auto value = (*base_segment)[chunk_offset];
          const auto is_null = variant_is_null(value);
          value_segment_value_vector.push_back(is_null ? ColumnDataType{} : boost::get<ColumnDataType>(value));
          value_segment_null_vector.push_back(is_null);
        }

        ++chunk_offset_out;

        // Check if value segment is full
        if (chunk_offset_out >= _output_chunk_size) {
          chunk_offset_out = 0u;
          auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                              std::move(value_segment_null_vector));
          chunk_it->push_back(value_segment);
          value_segment_value_vector = pmr_concurrent_vector<ColumnDataType>();
          value_segment_null_vector = pmr_concurrent_vector<bool>();
          ++chunk_it;
        }
      }

      // Last segment has not been added
      if (chunk_offset_out > 0u) {
        auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                            std::move(value_segment_null_vector));
        chunk_it->push_back(value_segment);
      }
    });
  }

  for (auto& segments : output_segments_by_chunk) {
    output->append_chunk(segments);
  }

  return output;
}

void Sort::_on_cleanup() {
  // TODO(anyone): Implement, if necessary
}

/**
 * Asserts that all column definitions are valid.
 */
void Sort::_validate_sort_definitions() const {
  const auto input_table = input_table_left();
  for (const auto& column_sort_definition : _sort_definitions) {
    Assert(column_sort_definition.column != INVALID_COLUMN_ID, "Sort: Invalid column in sort definition");
  }
}

template <typename SortColumnType>
class Sort::SortImpl {
 public:
  using RowIDValuePair = std::pair<RowID, SortColumnType>;

  SortImpl(const std::shared_ptr<const Table>& table_in, const ColumnID column_id,
           const OrderByMode order_by_mode = OrderByMode::Ascending, const size_t output_chunk_size = 0)
      : _table_in(table_in),
        _column_id(column_id),
        _order_by_mode(order_by_mode),
        _output_chunk_size(output_chunk_size) {
    // initialize a structure which can be sorted by std::sort
    _row_id_value_vector = std::make_shared<std::vector<RowIDValuePair>>();
    _null_value_rows = std::make_shared<std::vector<RowIDValuePair>>();
  }

  // TODO(anyone): Reduce this method's code duplication with sort_one_column by making parameter chunk_id optional
  std::shared_ptr<PosList> sort_one_column_of_chunk(ChunkID chunk_id, std::shared_ptr<PosList> pos_list = nullptr) {
    // 1. Prepare Sort: Creating rowid-value-Structure
    _materialize_sort_column_of_chunk(chunk_id, pos_list);

    // 2. After we got our ValueRowID Map we sort the map by the value of the pair
    if (_order_by_mode == OrderByMode::Ascending || _order_by_mode == OrderByMode::AscendingNullsLast) {
      _sort_with_operator<std::less<>>();
    } else {
      _sort_with_operator<std::greater<>>();
    }

    // 2b. Insert null rows if necessary
    if (!_null_value_rows->empty()) {
      if (_order_by_mode == OrderByMode::AscendingNullsLast || _order_by_mode == OrderByMode::DescendingNullsLast) {
        // NULLs last
        _row_id_value_vector->insert(_row_id_value_vector->end(), _null_value_rows->begin(), _null_value_rows->end());
      } else {
        // NULLs first (default behavior)
        _row_id_value_vector->insert(_row_id_value_vector->begin(), _null_value_rows->begin(), _null_value_rows->end());
      }
    }

    pos_list.reset();
    pos_list = std::make_shared<PosList>();

    for (auto& row_id_value : *_row_id_value_vector) {
      pos_list->emplace_back(row_id_value.first);
    }

    return pos_list;
  }

  std::shared_ptr<PosList> sort_one_column(std::shared_ptr<PosList> pos_list = nullptr) {
    // 1. Prepare Sort: Creating rowid-value-Structure
    _materialize_sort_column(pos_list);

    // 2. After we got our ValueRowID Map we sort the map by the value of the pair
    if (_order_by_mode == OrderByMode::Ascending || _order_by_mode == OrderByMode::AscendingNullsLast) {
      _sort_with_operator<std::less<>>();
    } else {
      _sort_with_operator<std::greater<>>();
    }

    // 2b. Insert null rows if necessary
    if (!_null_value_rows->empty()) {
      if (_order_by_mode == OrderByMode::AscendingNullsLast || _order_by_mode == OrderByMode::DescendingNullsLast) {
        // NULLs last
        _row_id_value_vector->insert(_row_id_value_vector->end(), _null_value_rows->begin(), _null_value_rows->end());
      } else {
        // NULLs first (default behavior)
        _row_id_value_vector->insert(_row_id_value_vector->begin(), _null_value_rows->begin(), _null_value_rows->end());
      }
    }

    pos_list.reset();
    pos_list = std::make_shared<PosList>();

    for (auto& row_id_value : *_row_id_value_vector) {
      pos_list->emplace_back(row_id_value.first);
    }

    return pos_list;
  }

 protected:
  // TODO(anyone): Reduce this method's code duplication with _materialize_sort_column
  // completely materializes the sort column to create a vector of RowID-Value pairs
  void _materialize_sort_column_of_chunk(ChunkID chunk_id, std::shared_ptr<PosList> pos_list = nullptr) {
    auto& row_id_value_vector = *_row_id_value_vector;
    row_id_value_vector.reserve(_table_in->get_chunk(chunk_id)->size());

    auto& null_value_rows = *_null_value_rows;

    if (pos_list) {
      // TODO(anyone): Optimize this since it only works on one chunk now
      auto segment_ptr_and_accessor_by_chunk_id =
          std::unordered_map<ChunkID, std::pair<std::shared_ptr<const BaseSegment>,
                                                std::shared_ptr<AbstractSegmentAccessor<SortColumnType>>>>();
      segment_ptr_and_accessor_by_chunk_id.reserve(_table_in->chunk_count());

      // When there was a preceding sorting run, we materialize according to the passed PosList.
      for (RowID row_id : *pos_list) {
        const auto [chunk_id, chunk_offset] = row_id;

        auto& segment_ptr_and_typed_ptr_pair = segment_ptr_and_accessor_by_chunk_id[chunk_id];
        auto& base_segment = segment_ptr_and_typed_ptr_pair.first;
        auto& accessor = segment_ptr_and_typed_ptr_pair.second;

        if (!base_segment) {
          base_segment = _table_in->get_chunk(chunk_id)->get_segment(_column_id);
          accessor = create_segment_accessor<SortColumnType>(base_segment);
        }

        // If the input segment is not a ReferenceSegment, we can take a fast(er) path
        if (accessor) {
          const auto typed_value = accessor->access(chunk_offset);
          if (!typed_value) {
            null_value_rows.emplace_back(row_id, SortColumnType{});
          } else {
            row_id_value_vector.emplace_back(row_id, typed_value.value());
          }
        } else {
          const auto value = (*base_segment)[chunk_offset];
          if (variant_is_null(value)) {
            null_value_rows.emplace_back(row_id, SortColumnType{});
          } else {
            row_id_value_vector.emplace_back(row_id, boost::get<SortColumnType>(value));
          }
        }
      }
    } else {
      const auto chunk = _table_in->get_chunk(chunk_id);
      Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

      auto base_segment = chunk->get_segment(_column_id);

      segment_iterate<SortColumnType>(*base_segment, [&](const auto& position) {
        if (position.is_null()) {
          null_value_rows.emplace_back(RowID{chunk_id, position.chunk_offset()}, SortColumnType{});
        } else {
          row_id_value_vector.emplace_back(RowID{chunk_id, position.chunk_offset()}, position.value());
        }
      });
    }
  }

  // completely materializes the sort column to create a vector of RowID-Value pairs
  void _materialize_sort_column(std::shared_ptr<PosList> pos_list = nullptr) {
    auto& row_id_value_vector = *_row_id_value_vector;
    row_id_value_vector.reserve(_table_in->row_count());

    auto& null_value_rows = *_null_value_rows;

    if (pos_list) {
      auto segment_ptr_and_accessor_by_chunk_id =
          std::unordered_map<ChunkID, std::pair<std::shared_ptr<const BaseSegment>,
                                                std::shared_ptr<AbstractSegmentAccessor<SortColumnType>>>>();
      segment_ptr_and_accessor_by_chunk_id.reserve(_table_in->chunk_count());

      // When there was a preceding sorting run, we materialize according to the passed PosList.
      for (RowID row_id : *pos_list) {
        const auto [chunk_id, chunk_offset] = row_id;

        auto& segment_ptr_and_typed_ptr_pair = segment_ptr_and_accessor_by_chunk_id[chunk_id];
        auto& base_segment = segment_ptr_and_typed_ptr_pair.first;
        auto& accessor = segment_ptr_and_typed_ptr_pair.second;

        if (!base_segment) {
          base_segment = _table_in->get_chunk(chunk_id)->get_segment(_column_id);
          accessor = create_segment_accessor<SortColumnType>(base_segment);
        }

        // If the input segment is not a ReferenceSegment, we can take a fast(er) path
        if (accessor) {
          const auto typed_value = accessor->access(chunk_offset);
          if (!typed_value) {
            null_value_rows.emplace_back(row_id, SortColumnType{});
          } else {
            row_id_value_vector.emplace_back(row_id, typed_value.value());
          }
        } else {
          const auto value = (*base_segment)[chunk_offset];
          if (variant_is_null(value)) {
            null_value_rows.emplace_back(row_id, SortColumnType{});
          } else {
            row_id_value_vector.emplace_back(row_id, boost::get<SortColumnType>(value));
          }
        }
      }
    } else {
      const auto chunk_count = _table_in->chunk_count();
      for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = _table_in->get_chunk(chunk_id);
        Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

        auto base_segment = chunk->get_segment(_column_id);

        segment_iterate<SortColumnType>(*base_segment, [&](const auto& position) {
          if (position.is_null()) {
            null_value_rows.emplace_back(RowID{chunk_id, position.chunk_offset()}, SortColumnType{});
          } else {
            row_id_value_vector.emplace_back(RowID{chunk_id, position.chunk_offset()}, position.value());
          }
        });
      }
    }
  }

  template <typename Comparator>
  void _sort_with_operator() {
    Comparator comparator;
    std::stable_sort(_row_id_value_vector->begin(), _row_id_value_vector->end(),
                     [comparator](RowIDValuePair a, RowIDValuePair b) { return comparator(a.second, b.second); });
  }

  const std::shared_ptr<const Table> _table_in;

  // column to sort by
  const ColumnID _column_id;
  const OrderByMode _order_by_mode;
  // chunk size of the materialized output
  const size_t _output_chunk_size;

  std::shared_ptr<std::vector<RowIDValuePair>> _row_id_value_vector;
  std::shared_ptr<std::vector<RowIDValuePair>> _null_value_rows;
};

}  // namespace opossum
