#include "sort.hpp"

#include "storage/segment_iterate.hpp"

namespace {

using namespace opossum;  // NOLINT

// Given an unsorted_table and a pos_list that defines the output order, this materializes all columns in the table,
// creating chunks of output_chunk_size rows at maximum.
std::shared_ptr<Table> materialize_output_table(const std::shared_ptr<const Table>& unsorted_table,
                                                const RowIDPosList& pos_list, const ChunkOffset output_chunk_size) {
  // First we create a new table as the output
  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408
  auto output = std::make_shared<Table>(unsorted_table->column_definitions(), TableType::Data, output_chunk_size);

  // After we created the output table and initialized the column structure, we can start adding values. Because the
  // values are not ordered by input chunks anymore, we can't process them chunk by chunk. Instead the values are
  // copied column by column for each output row.

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

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto chunk_it = output_segments_by_chunk.begin();
      auto current_segment_size = 0u;

      auto value_segment_value_vector = pmr_vector<ColumnDataType>();
      auto value_segment_null_vector = pmr_vector<bool>();

      value_segment_value_vector.reserve(output_chunk_size);
      value_segment_null_vector.reserve(output_chunk_size);

      auto accessor_by_chunk_id =
          std::vector<std::unique_ptr<AbstractSegmentAccessor<ColumnDataType>>>(unsorted_table->chunk_count());
      for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        const auto& base_segment = unsorted_table->get_chunk(input_chunk_id)->get_segment(column_id);
        accessor_by_chunk_id[input_chunk_id] = create_segment_accessor<ColumnDataType>(base_segment);
      }

      for (auto row_index = 0u; row_index < row_count; ++row_index) {
        const auto [chunk_id, chunk_offset] = pos_list[row_index];

        auto& accessor = accessor_by_chunk_id[chunk_id];
        const auto typed_value = accessor->access(chunk_offset);
        const auto is_null = !typed_value;
        value_segment_value_vector.push_back(is_null ? ColumnDataType{} : typed_value.value());
        value_segment_null_vector.push_back(is_null);

        ++current_segment_size;

        // Check if value segment is full
        if (current_segment_size >= output_chunk_size) {
          current_segment_size = 0u;
          auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                              std::move(value_segment_null_vector));
          chunk_it->push_back(value_segment);
          value_segment_value_vector = pmr_vector<ColumnDataType>();
          value_segment_null_vector = pmr_vector<bool>();

          value_segment_value_vector.reserve(output_chunk_size);
          value_segment_null_vector.reserve(output_chunk_size);

          ++chunk_it;
        }
      }

      // Last segment has not been added
      if (current_segment_size > 0u) {
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

}  // namespace

namespace opossum {

Sort::Sort(const std::shared_ptr<const AbstractOperator>& in, const std::vector<SortColumnDefinition>& sort_definitions,
           const ChunkOffset output_chunk_size)
    : AbstractReadOnlyOperator(OperatorType::Sort, in),
      _sort_definitions(sort_definitions),
      _output_chunk_size(output_chunk_size) {
  DebugAssert(!_sort_definitions.empty(), "Expected at least one sort criterion");
}

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
  const auto& input_table = input_table_left();
  for (const auto& column_sort_definition : _sort_definitions) {
    Assert(column_sort_definition.column != INVALID_COLUMN_ID, "Sort: Invalid column in sort definition");
    Assert(column_sort_definition.column < input_table->column_count(),
           "Sort: Column ID is greater than table's column count");
  }

  std::shared_ptr<Table> sorted_table;

  // After the first (least significant) sort operation has been completed, this holds the order of the table as it has
  // been determined so far. This is not a completely proper PosList on the input table as it might point to
  // ReferenceSegments.
  auto previously_sorted_pos_list = std::optional<RowIDPosList>{};

  for (auto sort_step = static_cast<int64_t>(_sort_definitions.size() - 1); sort_step >= 0; --sort_step) {
    const bool is_last_sorting_step = (sort_step == 0);

    const auto& sort_definition = _sort_definitions[sort_step];
    const auto data_type = input_table->column_data_type(sort_definition.column);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto sort_impl = SortImpl<ColumnDataType>(input_table, sort_definition.column, sort_definition.order_by_mode);
      previously_sorted_pos_list = sort_impl.sort(previously_sorted_pos_list);

      if (is_last_sorting_step) {
        // This is inside the for loop so that we do not have to resolve the type again
        sorted_table = materialize_output_table(input_table, *previously_sorted_pos_list, _output_chunk_size);
      }
    });
  }

  auto final_sort_definition = _sort_definitions[0];
  // Set the ordered_by attribute of the output's chunks according to the most significant sort operation, which is the
  // column the table was sorted by last.
  const auto chunk_count = sorted_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = sorted_table->get_chunk(chunk_id);
    chunk->finalize();
    chunk->set_ordered_by(std::make_pair(final_sort_definition.column, final_sort_definition.order_by_mode));
  }
  return sorted_table;
}

template <typename SortColumnType>
class Sort::SortImpl {
 public:
  using RowIDValuePair = std::pair<RowID, SortColumnType>;

  SortImpl(const std::shared_ptr<const Table>& table_in, const ColumnID column_id,
           const OrderByMode order_by_mode = OrderByMode::Ascending)
      : _table_in(table_in), _column_id(column_id), _order_by_mode(order_by_mode) {
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
    if (_order_by_mode == OrderByMode::Ascending || _order_by_mode == OrderByMode::AscendingNullsLast) {
      sort_with_comparator(std::less<>{});
    } else {
      sort_with_comparator(std::greater<>{});
    }

    // 2b. Insert null rows if necessary
    if (!_null_value_rows.empty()) {
      if (_order_by_mode == OrderByMode::AscendingNullsLast || _order_by_mode == OrderByMode::DescendingNullsLast) {
        // NULLs last
        _row_id_value_vector.insert(_row_id_value_vector.end(), _null_value_rows.begin(), _null_value_rows.end());
      } else {
        // NULLs first (default behavior)
        _row_id_value_vector.insert(_row_id_value_vector.begin(), _null_value_rows.begin(), _null_value_rows.end());
      }
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

        auto base_segment = chunk->get_segment(_column_id);

        segment_iterate<SortColumnType>(*base_segment, [&](const auto& position) {
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
      const auto& base_segment = _table_in->get_chunk(input_chunk_id)->get_segment(_column_id);
      accessor_by_chunk_id[input_chunk_id] = create_segment_accessor<SortColumnType>(base_segment);
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
  const OrderByMode _order_by_mode;

  std::vector<RowIDValuePair> _row_id_value_vector;

  // Stored as RowIDValuePair for better type compatibility even if value is unused
  std::vector<RowIDValuePair> _null_value_rows;
};

}  // namespace opossum
