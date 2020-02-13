#include "sort.hpp"

#include <mutex>

#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/segment_iterate.hpp"
#include "utils/pos_list_utils.hpp"

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

  std::shared_ptr<const Table> output = input_table_left();
  std::shared_ptr<PosList> merged_pos_list = nullptr;

  const auto in_table = input_table_left();
  const auto job_count = in_table->chunk_count();

  for (auto column_id = _sort_definitions.size(); column_id-- != 0;) {
    const bool is_last_sorting_run = (column_id == 0);

    auto sort_definition = _sort_definitions[column_id];
    DataType data_type = _sort_definition_data_types[column_id];

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      using RowIDValuePair = std::pair<RowID, ColumnDataType>;

      std::mutex output_mutex;

      auto output_value_vectors = std::vector<std::shared_ptr<std::vector<RowIDValuePair>>>{};
      auto output_null_vectors = std::vector<std::shared_ptr<std::vector<RowIDValuePair>>>{};
      output_value_vectors.resize(job_count);
      output_null_vectors.resize(job_count);

      auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
      jobs.reserve(job_count);

      // Initialize to nullptr to be able to check with boolean existence operator below
      auto pos_lists = std::shared_ptr<std::vector<std::shared_ptr<PosList>>>{};

      if (merged_pos_list) {
        pos_lists = split_pos_list(merged_pos_list, job_count);
      }

      for (ChunkID job_id{0u}; job_id < job_count; ++job_id) {
        auto job_task = std::make_shared<JobTask>([job_id, &in_table, pos_lists, sort_definition, &output_mutex,
                                                   &output_value_vectors, &output_null_vectors]() {
          std::shared_ptr<std::vector<RowIDValuePair>> chunk_column_sorted_value_vector;
          std::shared_ptr<std::vector<RowIDValuePair>> chunk_column_sorted_null_vector;

          auto sort_single_col_impl =
              SortImpl<ColumnDataType>(in_table, sort_definition.column, sort_definition.order_by_mode);

          if (pos_lists) {
            const auto chunk_column_sorted = sort_single_col_impl.sort_one_column(std::nullopt, (*pos_lists)[job_id]);
            chunk_column_sorted_value_vector = chunk_column_sorted.first;
            chunk_column_sorted_null_vector = chunk_column_sorted.second;
          } else {
            const auto chunk_column_sorted = sort_single_col_impl.sort_one_column(std::optional<ChunkID>{job_id});
            chunk_column_sorted_value_vector = chunk_column_sorted.first;
            chunk_column_sorted_null_vector = chunk_column_sorted.second;
          }

          std::lock_guard<std::mutex> lock(output_mutex);
          output_value_vectors[job_id] = chunk_column_sorted_value_vector;
          output_null_vectors[job_id] = chunk_column_sorted_null_vector;
        });
        jobs.push_back(job_task);
        job_task->schedule();
      }

      Hyrise::get().scheduler()->wait_for_tasks(jobs);

      auto merge_impl = MergeImpl<ColumnDataType>(in_table, sort_definition.column, sort_definition.order_by_mode);

      const auto merged_row_id_value_vector =
          merge_impl.merge_value_and_null_vectors(output_value_vectors, output_null_vectors);

      // 3. Materialization of the result: We take the sorted ValueRowID Vector, create chunks fill them until they are
      // full and create the next one. Each chunk is filled row by row.
      if (is_last_sorting_run) {
        auto materialization = std::make_shared<SortImplMaterializeOutput<ColumnDataType>>(
            input_table_left(), merged_row_id_value_vector, _output_chunk_size);

        std::shared_ptr<Table> output_mutable = materialization->execute();

        // Set the ordered_by attribute of the output's chunks to the column the chunk was sorted by last (because this
        // is the column that the chunk is ordered by in any case; whereas it is not necessarily sorted by the other
        // columns -- on their own -- as well).
        const auto chunk_count = output_mutable->chunk_count();
        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          output_mutable->get_chunk(chunk_id)->set_ordered_by(
              std::make_pair(sort_definition.column, sort_definition.order_by_mode));
        }
        output = output_mutable;
      } else {
        merged_pos_list.reset();
        merged_pos_list = std::make_shared<PosList>();

        for (auto& row_id_value : *merged_row_id_value_vector) {
          merged_pos_list->emplace_back(row_id_value.first);
        }
      }
    });
  }

  return output;
}

void Sort::_on_cleanup() {}

/**
 * Asserts that all column definitions are valid.
 */
void Sort::_validate_sort_definitions() const {
  const auto input_table = input_table_left();
  for (const auto& column_sort_definition : _sort_definitions) {
    Assert(column_sort_definition.column != INVALID_COLUMN_ID, "Sort: Invalid column in sort definition");
  }
}

template <typename MergeColumnType>
class Sort::MergeImpl {
 public:
  using RowIDValuePair = std::pair<RowID, MergeColumnType>;

  MergeImpl(const std::shared_ptr<const Table>& table_in, const ColumnID column_id,
            const OrderByMode order_by_mode = OrderByMode::Ascending)
      : _table_in(table_in), _column_id(column_id), _order_by_mode(order_by_mode) {}

  std::shared_ptr<std::vector<RowIDValuePair>> merge_value_and_null_vectors(
      std::vector<std::shared_ptr<std::vector<RowIDValuePair>>> value_vectors,
      std::vector<std::shared_ptr<std::vector<RowIDValuePair>>> null_vectors) {
    std::shared_ptr<std::vector<RowIDValuePair>> merged_vector = std::make_shared<std::vector<RowIDValuePair>>();
    merged_vector->reserve(_table_in->row_count());

    auto value_iterators = std::vector<std::pair<ChunkID, typename std::vector<RowIDValuePair>::iterator>>{};
    value_iterators.reserve(value_vectors.size());

    for (ChunkID chunk_id{0u}; chunk_id < value_vectors.size(); ++chunk_id) {
      const auto& value_vector = value_vectors[chunk_id];
      if (value_vector->size() > 0) {
        value_iterators.emplace_back(chunk_id, value_vectors[chunk_id]->begin());
      }
    }

    bool nulls_last =
        _order_by_mode == OrderByMode::AscendingNullsLast || _order_by_mode == OrderByMode::DescendingNullsLast;

    // (Step 0: Merge NULL values)
    if (!nulls_last) {
      for (ChunkID chunk_id{0u}; chunk_id < null_vectors.size(); ++chunk_id) {
        const auto null_vector = *null_vectors[chunk_id];
        std::transform(std::begin(null_vector), std::end(null_vector), std::back_inserter(*merged_vector));
      }
    }

    // Step 1: Merge non-NULL values
    while (value_iterators.size() != 0) {
      RowIDValuePair compare_result = *(value_iterators[0].second);
      uint32_t pos_list_id_of_compare_result_iterator = 0;
      ChunkID chunk_id_of_compare_result = value_iterators[0].first;

      if (value_iterators.size() > 1) {
        for (uint32_t pos_list_id{1u}; pos_list_id < value_iterators.size(); ++pos_list_id) {
          RowIDValuePair row_id_value_pair = *(value_iterators[pos_list_id].second);

          if (_order_by_mode == OrderByMode::Ascending || _order_by_mode == OrderByMode::AscendingNullsLast) {
            if (row_id_value_pair.second < compare_result.second) {
              compare_result = *(value_iterators[pos_list_id].second);
              pos_list_id_of_compare_result_iterator = pos_list_id;
              chunk_id_of_compare_result = value_iterators[pos_list_id].first;
            }
          } else {
            if (row_id_value_pair.second > compare_result.second) {
              compare_result = *(value_iterators[pos_list_id].second);
              pos_list_id_of_compare_result_iterator = pos_list_id;
              chunk_id_of_compare_result = value_iterators[pos_list_id].first;
            }
          }
        }
      }

      merged_vector->emplace_back(compare_result);

      auto compare_result_iterator = value_iterators[pos_list_id_of_compare_result_iterator].second;
      if ((compare_result_iterator != value_vectors[chunk_id_of_compare_result]->end()) &&
          (std::next(compare_result_iterator) == value_vectors[chunk_id_of_compare_result]->end())) {
        value_iterators.erase(value_iterators.begin() + pos_list_id_of_compare_result_iterator);
      } else {
        value_iterators[pos_list_id_of_compare_result_iterator].second = std::next(compare_result_iterator);
      }
    }

    // (Step 2: Merge NULL values)
    if (nulls_last) {
      for (ChunkID chunk_id{0u}; chunk_id < null_vectors.size(); ++chunk_id) {
        const auto null_vector = *null_vectors[chunk_id];
        std::transform(std::begin(null_vector), std::end(null_vector), std::back_inserter(*merged_vector));
      }
    }

    return merged_vector;
  }

  const std::shared_ptr<const Table> _table_in;

  // column to sort by
  const ColumnID _column_id;
  const OrderByMode _order_by_mode;
};

// This class fulfills only the materialization task for a sorted row_id_value_vector.
template <typename SortColumnType>
class Sort::SortImplMaterializeOutput {
 public:
  // creates a new table with reference segments
  SortImplMaterializeOutput(const std::shared_ptr<const Table>& in,
                            const std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>>& id_value_map,
                            const size_t output_chunk_size)
      : _table_in(in), _output_chunk_size(output_chunk_size), _row_id_value_vector(id_value_map) {}

  std::shared_ptr<Table> execute() {
    // First we create a new table as the output
    auto output = std::make_shared<Table>(_table_in->column_definitions(), TableType::Data, _output_chunk_size);

    // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408

    // After we created the output table and initialized the column structure, we can start adding values. Because the
    // values are not ordered by input chunks anymore, we can't process them chunk by chunk. Instead the values are
    // copied column by column for each output row. For each column in a row we visit the input segment with a reference
    // to the output segment. This enables for the SortImplMaterializeOutput class to ignore the column types during the
    // copying of the values.
    const auto row_count_out = _row_id_value_vector->size();

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

        auto value_segment_value_vector = pmr_vector<ColumnDataType>();
        auto value_segment_null_vector = pmr_vector<bool>();

        value_segment_value_vector.reserve(row_count_out);
        value_segment_null_vector.reserve(row_count_out);

        auto segment_ptr_and_accessor_by_chunk_id =
            std::unordered_map<ChunkID, std::pair<std::shared_ptr<const BaseSegment>,
                                                  std::shared_ptr<AbstractSegmentAccessor<ColumnDataType>>>>();
        segment_ptr_and_accessor_by_chunk_id.reserve(row_count_out);

        for (auto row_index = 0u; row_index < row_count_out; ++row_index) {
          const auto [chunk_id, chunk_offset] = _row_id_value_vector->at(row_index).first;

          auto& segment_ptr_and_typed_ptr_pair = segment_ptr_and_accessor_by_chunk_id[chunk_id];
          auto& base_segment = segment_ptr_and_typed_ptr_pair.first;
          auto& accessor = segment_ptr_and_typed_ptr_pair.second;

          if (!base_segment) {
            base_segment = _table_in->get_chunk(chunk_id)->get_segment(column_id);
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
            value_segment_value_vector = pmr_vector<ColumnDataType>();
            value_segment_null_vector = pmr_vector<bool>();
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

 protected:
  const std::shared_ptr<const Table> _table_in;
  const size_t _output_chunk_size;
  const std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> _row_id_value_vector;
};

template <typename SortColumnType>
class Sort::SortImpl {
 public:
  using RowIDValuePair = std::pair<RowID, SortColumnType>;

  SortImpl(const std::shared_ptr<const Table>& table_in, const ColumnID column_id,
           const OrderByMode order_by_mode = OrderByMode::Ascending, const size_t output_chunk_size = 0)
      : _table_in(table_in), _column_id(column_id), _order_by_mode(order_by_mode) {
    // initialize a structure which can be sorted by std::sort
    _row_id_value_vector = std::make_shared<std::vector<RowIDValuePair>>();
    _null_value_rows = std::make_shared<std::vector<RowIDValuePair>>();
  }

  std::pair<std::shared_ptr<std::vector<RowIDValuePair>>, std::shared_ptr<std::vector<RowIDValuePair>>> sort_one_column(
      std::optional<ChunkID> chunk_id = std::nullopt, const std::shared_ptr<PosList>& pos_list = nullptr) {
    // 1. Prepare Sort: Creating rowid-value-Structure
    _materialize_sort_column(chunk_id, pos_list);

    // 2. After we got our ValueRowID Map we sort the map by the value of the pair
    if (_order_by_mode == OrderByMode::Ascending || _order_by_mode == OrderByMode::AscendingNullsLast) {
      _sort_with_operator<std::less<>>();
    } else {
      _sort_with_operator<std::greater<>>();
    }

    return {_row_id_value_vector, _null_value_rows};
  }

 protected:
  // Completely materializes the sort column to create a vector of RowID-Value pairs.
  void _materialize_sort_column(std::optional<ChunkID> chunk_id = std::nullopt,
                                std::shared_ptr<PosList> pos_list = nullptr) {
    auto& row_id_value_vector = *_row_id_value_vector;

    if (chunk_id.has_value()) {
      row_id_value_vector.reserve(_table_in->get_chunk(chunk_id.value())->size());
    } else {
      row_id_value_vector.reserve(_table_in->row_count());
    }

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

        // If the input segment is not a ReferenceSegment, we can take a fast(er) path.
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
      std::function<void(const std::shared_ptr<const opossum::Chunk>)> materialize_chunk = [&](const auto& chunk) {
        Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

        auto base_segment = chunk->get_segment(_column_id);

        segment_iterate<SortColumnType>(*base_segment, [&](const auto& position) {
          if (position.is_null()) {
            null_value_rows.emplace_back(RowID{chunk_id.value(), position.chunk_offset()}, SortColumnType{});
          } else {
            row_id_value_vector.emplace_back(RowID{chunk_id.value(), position.chunk_offset()}, position.value());
          }
        });
      };

      if (chunk_id.has_value()) {
        const auto chunk = _table_in->get_chunk(chunk_id.value());
        materialize_chunk(chunk);
      } else {
        const auto chunk_count = _table_in->chunk_count();
        for (ChunkID chunk_idx{0}; chunk_idx < chunk_count; ++chunk_idx) {
          const auto chunk = _table_in->get_chunk(chunk_idx);
          materialize_chunk(chunk);
        }
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

  std::shared_ptr<std::vector<RowIDValuePair>> _row_id_value_vector;
  std::shared_ptr<std::vector<RowIDValuePair>> _null_value_rows;
};

}  // namespace opossum
