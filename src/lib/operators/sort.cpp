#include "sort.hpp"

#include "storage/segment_iterate.hpp"
#include "utils/timer.hpp"

namespace {

using namespace opossum;  // NOLINT

// Given an unsorted_table and a pos_list that defines the output order, this materializes all columns in the table,
// creating chunks of output_chunk_size rows at maximum.
std::shared_ptr<Table> write_materialized_output_table(const std::shared_ptr<const Table>& unsorted_table,
                                                       RowIDPosList pos_list, const ChunkOffset output_chunk_size) {
  // First, we create a new table as the output
  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408
  auto output = std::make_shared<Table>(unsorted_table->column_definitions(), TableType::Data, output_chunk_size);

  // After we created the output table and initialized the column structure, we can start adding values. Because the
  // values are not sorted by input chunks anymore, we can't process them chunk by chunk. Instead the values are copied
  // column by column for each output row.

  // Ceiling of integer division
  const auto div_ceil = [](auto x, auto y) { return (x + y - 1u) / y; };
  const auto output_chunk_count = div_ceil(pos_list.size(), output_chunk_size);
  Assert(pos_list.size() == unsorted_table->row_count(), "Mismatching size of input table and PosList");

  // Vector of segments for each chunk
  std::vector<Segments> output_segments_by_chunk(output_chunk_count);

  // Materialize column by column, starting a new ValueSegment whenever output_chunk_size is reached
  const auto input_chunk_count = unsorted_table->chunk_count();
  const auto row_count = unsorted_table->row_count();
  for (ColumnID column_id{0u}; column_id < output->column_count(); ++column_id) {
    const auto column_data_type = output->column_data_type(column_id);
    const auto column_is_nullable = unsorted_table->column_is_nullable(column_id);

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto chunk_it = output_segments_by_chunk.begin();
      auto current_segment_size = 0u;

      auto value_segment_value_vector = pmr_vector<ColumnDataType>();
      auto value_segment_null_vector = pmr_vector<bool>();

      {
        const auto next_chunk_size = std::min(static_cast<size_t>(output_chunk_size), static_cast<size_t>(row_count));
        value_segment_value_vector.reserve(next_chunk_size);
        if (column_is_nullable) value_segment_null_vector.reserve(next_chunk_size);
      }

      auto accessor_by_chunk_id =
          std::vector<std::unique_ptr<AbstractSegmentAccessor<ColumnDataType>>>(unsorted_table->chunk_count());
      for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        const auto& abstract_segment = unsorted_table->get_chunk(input_chunk_id)->get_segment(column_id);
        accessor_by_chunk_id[input_chunk_id] = create_segment_accessor<ColumnDataType>(abstract_segment);
      }

      for (auto row_index = size_t{0}; row_index < row_count; ++row_index) {
        const auto [chunk_id, chunk_offset] = pos_list[row_index];

        auto& accessor = accessor_by_chunk_id[chunk_id];
        const auto typed_value = accessor->access(chunk_offset);
        const auto is_null = !typed_value;
        value_segment_value_vector.push_back(is_null ? ColumnDataType{} : typed_value.value());
        if (column_is_nullable) value_segment_null_vector.push_back(is_null);

        ++current_segment_size;

        // Check if value segment is full
        if (current_segment_size >= output_chunk_size) {
          current_segment_size = 0u;

          std::shared_ptr<ValueSegment<ColumnDataType>> value_segment;
          if (column_is_nullable) {
            value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                           std::move(value_segment_null_vector));
          } else {
            value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector));
          }

          chunk_it->push_back(value_segment);
          value_segment_value_vector = pmr_vector<ColumnDataType>();
          value_segment_null_vector = pmr_vector<bool>();

          const auto next_chunk_size =
              std::min(static_cast<size_t>(output_chunk_size), static_cast<size_t>(row_count - row_index));
          value_segment_value_vector.reserve(next_chunk_size);
          if (column_is_nullable) value_segment_null_vector.reserve(next_chunk_size);

          ++chunk_it;
        }
      }

      // Last segment has not been added
      if (current_segment_size > 0u) {
        std::shared_ptr<ValueSegment<ColumnDataType>> value_segment;
        if (column_is_nullable) {
          value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                         std::move(value_segment_null_vector));
        } else {
          value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector));
        }
        chunk_it->push_back(value_segment);
      }
    });
  }

  for (auto& segments : output_segments_by_chunk) {
    output->append_chunk(segments);
  }

  return output;
}

// Given an unsorted_table and an input_pos_list that defines the output order, this writes the output table as a
// reference table. This is usually faster, but can only be done if a single column in the input table does not
// reference multiple tables. An example where this restriction applies is the sorted result of a union between two
// tables. The restriction is needed because a ReferenceSegment can only reference a single table. It does, however,
// not necessarily apply to joined tables, so two tables referenced in different columns is fine.
//
// If unsorted_table is of TableType::Data, this is trivial and the input_pos_list is used to create the output
// reference table. If the input is already a reference table, the double indirection needs to be resolved.
std::shared_ptr<Table> write_reference_output_table(const std::shared_ptr<const Table>& unsorted_table,
                                                    RowIDPosList input_pos_list, const ChunkOffset output_chunk_size) {
  // First we create a new table as the output
  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408
  auto output_table = std::make_shared<Table>(unsorted_table->column_definitions(), TableType::References);

  const auto resolve_indirection = unsorted_table->type() == TableType::References;
  const auto column_count = output_table->column_count();

  // Ceiling of integer division
  const auto div_ceil = [](auto x, auto y) { return (x + y - 1u) / y; };
  const auto output_chunk_count = div_ceil(input_pos_list.size(), output_chunk_size);
  Assert(input_pos_list.size() == unsorted_table->row_count(), "Mismatching size of input table and PosList");

  // Vector of segments for each chunk
  auto output_segments_by_chunk = std::vector<Segments>(output_chunk_count, Segments(column_count));

  if (!resolve_indirection && input_pos_list.size() <= output_chunk_size) {
    // Shortcut: No need to copy RowIDs if input_pos_list is small enough and we do not need to resolve the indirection.
    const auto output_pos_list = std::make_shared<RowIDPosList>(std::move(input_pos_list));
    auto& output_segments = output_segments_by_chunk.at(0);
    for (auto column_id = ColumnID{0u}; column_id < column_count; ++column_id) {
      output_segments[column_id] = std::make_shared<ReferenceSegment>(unsorted_table, column_id, output_pos_list);
    }
  } else {
    for (ColumnID column_id{0u}; column_id < column_count; ++column_id) {
      // To keep the implementation simple, we write the output ReferenceSegments column by column. This means that even
      // if input ReferenceSegments share a PosList, the output will contain independent PosLists. While this is
      // slightly more expensive to generate and slightly less efficient for following operators, we assume that the
      // lion's share of the work has been done before the Sort operator is executed and that the relative cost of this
      // is acceptable. In the future, this could be improved.
      auto output_pos_list = std::make_shared<RowIDPosList>();
      output_pos_list->reserve(output_chunk_size);

      // Collect all input segments for the current column
      const auto input_chunk_count = unsorted_table->chunk_count();
      auto input_segments = std::vector<std::shared_ptr<AbstractSegment>>(input_chunk_count);
      for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        input_segments[input_chunk_id] = unsorted_table->get_chunk(input_chunk_id)->get_segment(column_id);
      }

      const auto first_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(input_segments.at(0));
      const auto referenced_table = resolve_indirection ? first_reference_segment->referenced_table() : unsorted_table;
      const auto referenced_column_id =
          resolve_indirection ? first_reference_segment->referenced_column_id() : column_id;

      // write_output_pos_list creates an output reference segment for a given ChunkID, ColumnID and PosList.
      auto output_chunk_id = ChunkID{0};
      const auto write_output_pos_list = [&] {
        DebugAssert(!output_pos_list->empty(), "Asked to write empty output_pos_list");
        output_segments_by_chunk.at(output_chunk_id)[column_id] =
            std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, output_pos_list);
        ++output_chunk_id;

        output_pos_list = std::make_shared<RowIDPosList>();
        if (output_chunk_id < output_chunk_count) {
          output_pos_list->reserve(output_chunk_size);
        }
      };

      // Iterate over rows in sorted input pos list, dereference them if necessary, and write a chunk every
      // `output_chunk_size` rows.
      for (auto input_pos_list_offset = size_t{0}; input_pos_list_offset < input_pos_list.size();
           ++input_pos_list_offset) {
        const auto& row_id = input_pos_list[input_pos_list_offset];
        if (resolve_indirection) {
          const auto& input_reference_segment = static_cast<ReferenceSegment&>(*input_segments[row_id.chunk_id]);
          DebugAssert(input_reference_segment.referenced_table() == referenced_table,
                      "Input column references more than one table");
          DebugAssert(input_reference_segment.referenced_column_id() == referenced_column_id,
                      "Input column references more than one column");
          const auto& input_reference_pos_list = input_reference_segment.pos_list();
          output_pos_list->emplace_back((*input_reference_pos_list)[row_id.chunk_offset]);
        } else {
          output_pos_list->emplace_back(row_id);
        }

        if (output_pos_list->size() == output_chunk_size) {
          write_output_pos_list();
        }
      }
      if (!output_pos_list->empty()) {
        write_output_pos_list();
      }
    }
  }

  for (auto& segments : output_segments_by_chunk) {
    output_table->append_chunk(segments);
  }

  return output_table;
}

}  // namespace

namespace opossum {

Sort::Sort(const std::shared_ptr<const AbstractOperator>& in, const std::vector<SortColumnDefinition>& sort_definitions,
           const ChunkOffset output_chunk_size, const ForceMaterialization force_materialization)
    : AbstractReadOnlyOperator(OperatorType::Sort, in, nullptr,
                               std::make_unique<OperatorPerformanceData<OperatorSteps>>()),
      _sort_definitions(sort_definitions),
      _output_chunk_size(output_chunk_size),
      _force_materialization(force_materialization) {
  DebugAssert(!_sort_definitions.empty(), "Expected at least one sort criterion");
}

const std::vector<SortColumnDefinition>& Sort::sort_definitions() const { return _sort_definitions; }

const std::string& Sort::name() const {
  static const auto name = std::string{"Sort"};
  return name;
}

std::shared_ptr<AbstractOperator> Sort::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input) const {
  return std::make_shared<Sort>(copied_left_input, _sort_definitions, _output_chunk_size, _force_materialization);
}

void Sort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Sort::_on_execute() {
  Timer timer;
  const auto& input_table = left_input_table();

  for (const auto& column_sort_definition : _sort_definitions) {
    Assert(column_sort_definition.column != INVALID_COLUMN_ID, "Sort: Invalid column in sort definition");
    Assert(column_sort_definition.column < input_table->column_count(),
           "Sort: Column ID is greater than table's column count");
  }

  if (input_table->row_count() == 0) {
    if (_force_materialization == ForceMaterialization::Yes && input_table->type() == TableType::References) {
      return Table::create_dummy_table(input_table->column_definitions());
    } else {
      return input_table;
    }
  }

  std::shared_ptr<Table> sorted_table;

  // After the first (least significant) sort operation has been completed, this holds the order of the table as it has
  // been determined so far. This is not a completely proper PosList on the input table as it might point to
  // ReferenceSegments.
  auto previously_sorted_pos_list = std::optional<RowIDPosList>{};

  for (auto sort_step = static_cast<int64_t>(_sort_definitions.size() - 1); sort_step >= 0; --sort_step) {
    const auto& sort_definition = _sort_definitions[sort_step];
    const auto data_type = input_table->column_data_type(sort_definition.column);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto sort_impl = SortImpl<ColumnDataType>(input_table, sort_definition.column, sort_definition.sort_mode);
      previously_sorted_pos_list = sort_impl.sort(previously_sorted_pos_list);
    });
  }

  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  step_performance_data.set_step_runtime(OperatorSteps::Sort, timer.lap());

  // We have to materialize the output (i.e., write ValueSegments) if
  //  (a) it is requested by the user,
  //  (b) a column in the table references multiple tables (see write_reference_output_table for details), or
  //  (c) a column in the table references multiple columns in the same table (which is an unlikely edge case).
  // Cases (b) and (c) can only occur if there is more than one ReferenceSegment in an input chunk.
  auto must_materialize = _force_materialization == ForceMaterialization::Yes;
  const auto input_chunk_count = input_table->chunk_count();
  if (!must_materialize && input_table->type() == TableType::References && input_chunk_count > 1) {
    const auto input_column_count = input_table->column_count();

    for (auto input_column_id = ColumnID{0}; input_column_id < input_column_count; ++input_column_id) {
      const auto& first_segment = input_table->get_chunk(ChunkID{0})->get_segment(input_column_id);
      const auto& first_reference_segment = static_cast<ReferenceSegment&>(*first_segment);

      const auto& common_referenced_table = first_reference_segment.referenced_table();
      const auto& common_referenced_column_id = first_reference_segment.referenced_column_id();

      for (auto input_chunk_id = ChunkID{1}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        const auto& segment = input_table->get_chunk(input_chunk_id)->get_segment(input_column_id);
        const auto& referenced_table = static_cast<ReferenceSegment&>(*segment).referenced_table();
        const auto& referenced_column_id = static_cast<ReferenceSegment&>(*segment).referenced_column_id();

        if (common_referenced_table != referenced_table || common_referenced_column_id != referenced_column_id) {
          must_materialize = true;
          break;
        }
      }
      if (must_materialize) break;
    }
  }

  if (must_materialize) {
    sorted_table =
        write_materialized_output_table(input_table, std::move(*previously_sorted_pos_list), _output_chunk_size);
  } else {
    sorted_table =
        write_reference_output_table(input_table, std::move(*previously_sorted_pos_list), _output_chunk_size);
  }

  const auto& final_sort_definition = _sort_definitions[0];
  // Set the sorted_by attribute of the output's chunks according to the most significant sort operation, which is the
  // column the table was sorted by last.
  const auto output_chunk_count = sorted_table->chunk_count();
  for (auto output_chunk_id = ChunkID{0}; output_chunk_id < output_chunk_count; ++output_chunk_id) {
    const auto& output_chunk = sorted_table->get_chunk(output_chunk_id);
    output_chunk->finalize();
    output_chunk->set_individually_sorted_by(final_sort_definition);
  }

  step_performance_data.set_step_runtime(OperatorSteps::WriteOutput, timer.lap());
  return sorted_table;
}

template <typename SortColumnType>
class Sort::SortImpl {
 public:
  using RowIDValuePair = std::pair<RowID, SortColumnType>;

  SortImpl(const std::shared_ptr<const Table>& table_in, const ColumnID column_id,
           const SortMode sort_mode = SortMode::Ascending)
      : _table_in(table_in), _column_id(column_id), _sort_mode(sort_mode) {
    const auto row_count = _table_in->row_count();
    _row_id_value_vector.reserve(row_count);
    _null_value_rows.reserve(row_count);
  }

  // Sorts table_in, potentially taking the pre-existing order of previously_sorted_pos_list into account.
  // Returns a PosList, which can either be used as an input to the next call of sort or for materializing the
  // output table.
  RowIDPosList sort(const std::optional<RowIDPosList>& previously_sorted_pos_list) {
    // 1. Prepare Sort: Creating RowID-value-Structure
    _materialize_sort_column(previously_sorted_pos_list);

    // 2. After we got our ValueRowID Map we sort the map by the value of the pair
    const auto sort_with_comparator = [&](auto comparator) {
      std::stable_sort(_row_id_value_vector.begin(), _row_id_value_vector.end(),
                       [comparator](RowIDValuePair a, RowIDValuePair b) { return comparator(a.second, b.second); });
    };
    if (_sort_mode == SortMode::Ascending) {
      sort_with_comparator(std::less<>{});
    } else {
      sort_with_comparator(std::greater<>{});
    }

    // 2b. Insert null rows in front of all non-NULL rows
    if (!_null_value_rows.empty()) {
      // NULLs come before all values. The SQL standard allows for this to be implementation-defined. We used to have
      // a NULLS LAST mode, but never used it over multiple years. Different databases have different behaviors, and
      // storing NULLs first even for descending orders is somewhat uncommon:
      //   https://docs.mendix.com/refguide/null-ordering-behavior
      // For Hyrise, we found that storing NULLs first is the method that requires the least amount of code.
      _row_id_value_vector.insert(_row_id_value_vector.begin(), _null_value_rows.begin(), _null_value_rows.end());
    }

    RowIDPosList pos_list{};
    pos_list.reserve(_row_id_value_vector.size());
    for (const auto& [row_id, _] : _row_id_value_vector) {
      pos_list.emplace_back(row_id);
    }
    return pos_list;
  }

 protected:
  // completely materializes the sort column to create a vector of RowID-Value pairs
  void _materialize_sort_column(const std::optional<RowIDPosList>& previously_sorted_pos_list) {
    // If there was no PosList passed, this is the first sorting run and we simply fill our values and nulls data
    // structures from our input table. Otherwise we will materialize according to the PosList which is the result of
    // the last run.
    if (previously_sorted_pos_list) {
      _materialize_column_from_pos_list(*previously_sorted_pos_list);
    } else {
      const auto chunk_count = _table_in->chunk_count();
      for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = _table_in->get_chunk(chunk_id);
        Assert(chunk, "Did not expect deleted chunk here.");  // see https://github.com/hyrise/hyrise/issues/1686

        auto abstract_segment = chunk->get_segment(_column_id);

        segment_iterate<SortColumnType>(*abstract_segment, [&](const auto& position) {
          if (position.is_null()) {
            _null_value_rows.emplace_back(RowID{chunk_id, position.chunk_offset()}, SortColumnType{});
          } else {
            _row_id_value_vector.emplace_back(RowID{chunk_id, position.chunk_offset()}, position.value());
          }
        });
      }
    }
  }

  // When there was a preceding sorting run, we materialize by retaining the order of the values in the passed PosList.
  void _materialize_column_from_pos_list(const RowIDPosList& pos_list) {
    const auto input_chunk_count = _table_in->chunk_count();
    auto accessor_by_chunk_id =
        std::vector<std::unique_ptr<AbstractSegmentAccessor<SortColumnType>>>(input_chunk_count);
    for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
      const auto& abstract_segment = _table_in->get_chunk(input_chunk_id)->get_segment(_column_id);
      accessor_by_chunk_id[input_chunk_id] = create_segment_accessor<SortColumnType>(abstract_segment);
    }

    for (auto row_id : pos_list) {
      const auto [chunk_id, chunk_offset] = row_id;

      auto& accessor = accessor_by_chunk_id[chunk_id];
      const auto typed_value = accessor->access(chunk_offset);
      if (!typed_value) {
        _null_value_rows.emplace_back(row_id, SortColumnType{});
      } else {
        _row_id_value_vector.emplace_back(row_id, typed_value.value());
      }
    }
  }

  const std::shared_ptr<const Table> _table_in;

  // column to sort by
  const ColumnID _column_id;
  const SortMode _sort_mode;

  std::vector<RowIDValuePair> _row_id_value_vector;

  // Stored as RowIDValuePair for better type compatibility even if value is unused
  std::vector<RowIDValuePair> _null_value_rows;
};

}  // namespace opossum
