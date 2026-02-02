#include "sort_for_aggregate.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "boost/sort/pdqsort/pdqsort.hpp"
#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/operator_performance_data.hpp"
#include "operators/sort_algorithms/parallel_merge_sorter.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/base_segment_accessor.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/normalization.hpp"
#include "utils/timer.hpp"
#include "expression/window_function_expression.hpp"

namespace {

using StringSize = unsigned char;

using namespace hyrise;  // NOLINT

// Ceiling of integer division.
size_t div_ceil(const size_t lhs, const ChunkOffset rhs) {
  DebugAssert(rhs > 0, "Divisor must be larger than 0.");
  return (lhs + rhs - 1u) / rhs;
}

bool is_descending(const SortMode& mode) {
  return mode == SortMode::DescendingNullsFirst || mode == SortMode::DescendingNullsLast;
}

// Given an unsorted_table and a pos_list that defines the output order, this materializes all columns in the table,
// creating chunks of output_chunk_size rows at maximum.
std::shared_ptr<Table> write_materialized_output_table(const std::shared_ptr<const Table>& unsorted_table,
                                                       RowIDPosList pos_list, const ChunkOffset output_chunk_size) {
  // First, we create a new table as the output.
  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408.
  auto output = std::make_shared<Table>(unsorted_table->column_definitions(), TableType::Data, output_chunk_size);

  // After we created the output table and initialized the column structure, we can start adding values. Because the
  // values are not sorted by input chunks anymore, we can't process them chunk by chunk. Instead the values are copied
  // column by column for each output row.

  const auto output_chunk_count = div_ceil(pos_list.size(), output_chunk_size);
  Assert(pos_list.size() == unsorted_table->row_count(), "Mismatching size of input table and PosList");

  // Materialize column by column, starting a new ValueSegment whenever output_chunk_size is reached.
  const auto input_chunk_count = unsorted_table->chunk_count();
  const auto output_column_count = unsorted_table->column_count();
  const auto row_count = unsorted_table->row_count();

  // Vector of segments for each chunk.
  auto output_segments_by_chunk = std::vector<Segments>(output_chunk_count, Segments(output_column_count));

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(output_chunk_count * output_column_count);

  for (auto column_id = ColumnID{0}; column_id < output_column_count; ++column_id) {
    const auto column_data_type = output->column_data_type(column_id);
    const auto column_is_nullable = unsorted_table->column_is_nullable(column_id);

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto accessor_by_chunk_id =
          std::vector<std::shared_ptr<AbstractSegmentAccessor<ColumnDataType>>>(unsorted_table->chunk_count());
      for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        const auto& abstract_segment = unsorted_table->get_chunk(input_chunk_id)->get_segment(column_id);
        accessor_by_chunk_id[input_chunk_id] = create_segment_accessor<ColumnDataType>(abstract_segment);
      }

      for (auto output_chunk_id = ChunkID{0}; output_chunk_id < output_chunk_count; ++output_chunk_id) {
        jobs.emplace_back(
            std::make_shared<JobTask>([&, output_chunk_id, column_id, column_is_nullable, accessor_by_chunk_id]() {
              auto value_segment_value_vector = pmr_vector<ColumnDataType>{};
              auto value_segment_null_vector = pmr_vector<bool>{};

              const auto chunk_size = std::min(
                  output_chunk_size,
                  static_cast<ChunkOffset>(row_count - (static_cast<uint64_t>(output_chunk_id * output_chunk_size))));

              value_segment_value_vector.reserve(chunk_size);
              if (column_is_nullable) {
                value_segment_null_vector.reserve(chunk_size);
              }

              const auto pos_base = output_chunk_size * output_chunk_id;
              for (auto row_index = size_t{0}; row_index < chunk_size; ++row_index) {
                const auto [chunk_id, chunk_offset] = pos_list[pos_base + row_index];

                auto& accessor = accessor_by_chunk_id[chunk_id];
                const auto typed_value = accessor->access(chunk_offset);
                const auto is_null = !typed_value;
                value_segment_value_vector.push_back(is_null ? ColumnDataType{} : typed_value.value());
                if (column_is_nullable) {
                  value_segment_null_vector.push_back(is_null);
                }
              }

              std::shared_ptr<ValueSegment<ColumnDataType>> value_segment;
              if (column_is_nullable) {
                value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                               std::move(value_segment_null_vector));
              } else {
                value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector));
              }

              output_segments_by_chunk[output_chunk_id][column_id] = value_segment;
            }));
      }
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

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
  // First we create a new table as the output.
  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408.
  auto output_table = std::make_shared<Table>(unsorted_table->column_definitions(), TableType::References);

  const auto resolve_indirection = unsorted_table->type() == TableType::References;
  const auto column_count = output_table->column_count();

  const auto output_chunk_count = div_ceil(input_pos_list.size(), output_chunk_size);
  const auto row_count = unsorted_table->row_count();
  Assert(input_pos_list.size() == row_count, "Mismatching size of input table and PosList");

  // Vector of segments for each chunk.
  auto output_segments_by_chunk = std::vector<Segments>(output_chunk_count, Segments(column_count));

  if (!resolve_indirection && input_pos_list.size() <= output_chunk_size) {
    // Shortcut: No need to copy RowIDs if input_pos_list is small enough and we do not need to resolve the indirection.
    const auto output_pos_list = std::make_shared<RowIDPosList>(std::move(input_pos_list));
    auto& output_segments = output_segments_by_chunk.at(0);
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      output_segments[column_id] = std::make_shared<ReferenceSegment>(unsorted_table, column_id, output_pos_list);
    }
  } else {
    const auto input_chunk_count = unsorted_table->chunk_count();

    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(static_cast<size_t>(column_count));

    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      // Collect all input segments for the current column.
      auto input_segments = std::vector<std::shared_ptr<AbstractSegment>>(input_chunk_count);
      for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        input_segments[input_chunk_id] = unsorted_table->get_chunk(input_chunk_id)->get_segment(column_id);
      }

      const auto first_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(input_segments.at(0));
      const auto referenced_table = resolve_indirection ? first_reference_segment->referenced_table() : unsorted_table;
      const auto referenced_column_id =
          resolve_indirection ? first_reference_segment->referenced_column_id() : column_id;

      for (auto output_chunk_id = ChunkID{0}; output_chunk_id < output_chunk_count; ++output_chunk_id) {
        jobs.emplace_back(std::make_shared<JobTask>([&, output_chunk_id, column_id, input_segments, referenced_table,
                                                     referenced_column_id]() {
          const auto chunk_size = std::min(
              output_chunk_size,
              static_cast<ChunkOffset>(row_count - (static_cast<uint64_t>(output_chunk_id * output_chunk_size))));

          // To keep the implementation simple, we write the output ReferenceSegments column by column.
          // This means that even if input ReferenceSegments share a PosList,
          //  the output will contain independent PosLists. While this is
          // slightly more expensive to generate and slightly less efficient for following operators,
          // we assume that the lion's share of the work has been done
          // before the Sort operator is executed and that the relative cost of this
          // is acceptable. In the future, this could be improved.
          auto output_pos_list = std::make_shared<RowIDPosList>();
          output_pos_list->reserve(chunk_size);

          const auto pos_base = output_chunk_size * output_chunk_id;
          for (auto row_index = size_t{0}; row_index < chunk_size; ++row_index) {
            const auto& row_id = input_pos_list[pos_base + row_index];

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
          }

          DebugAssert(!output_pos_list->empty(), "Asked to write empty output_pos_list");
          output_segments_by_chunk.at(output_chunk_id)[column_id] =
              std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, output_pos_list);
        }));
        jobs.back()->schedule();  // Schedule job immediately.
      }
    }

    Hyrise::get().scheduler()->wait_for_tasks(jobs);
  }

  for (auto& segments : output_segments_by_chunk) {
    output_table->append_chunk(segments);
  }

  return output_table;
}

}  // namespace

namespace hyrise {

SortForAggregate::SortForAggregate(const std::shared_ptr<const AbstractOperator>& input_operator,
           const std::vector<SortColumnDefinition>& sort_definitions, 
           const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
           const ChunkOffset output_chunk_size,
           const ForceMaterialization force_materialization)
    : AbstractReadOnlyOperator(OperatorType::Sort, input_operator, nullptr,
                               std::make_unique<OperatorPerformanceData<OperatorSteps>>()),
      _sort_definitions(sort_definitions),
      _output_chunk_size(output_chunk_size),
      _force_materialization(force_materialization),
      _rowid_sorter(std::make_unique<ParallelMergeSorter<std::function<bool(const RowID&, const RowID&)>>>()),
      _agg_sorter(std::make_unique<ParallelAggregateSorter<std::function<bool(const AggEntry&, const AggEntry&)>>>()),
      _aggregates(aggregates) {
  DebugAssert(!_sort_definitions.empty(), "Expected at least one sort criterion");
}

const std::vector<SortColumnDefinition>& SortForAggregate::sort_definitions() const {
  return _sort_definitions;
}

const std::vector<std::shared_ptr<WindowFunctionExpression>>& SortForAggregate::aggregates() const {
  return _aggregates;
}

const std::string& SortForAggregate::name() const {
  static const auto name = std::string{"Sort"};
  return name;
}

std::shared_ptr<AbstractOperator> SortForAggregate::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  // FIX: Create SortForAggregate, not Sort
  return std::make_shared<SortForAggregate>(copied_left_input, _sort_definitions, _aggregates, _output_chunk_size, _force_materialization);
}

void SortForAggregate::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> SortForAggregate::_on_execute() {
  auto timer = Timer{};
  const auto& input_table = left_input_table();


  // Validate sort definitions
  for (const auto& column_sort_definition : _sort_definitions) {
    Assert(column_sort_definition.column != INVALID_COLUMN_ID, "Sort: Invalid column in sort definition");
    Assert(column_sort_definition.column < input_table->column_count(),
           "Sort: Column ID is greater than table's column count");
  }

  if (input_table->row_count() == 0) {
    if (_force_materialization == ForceMaterialization::Yes && input_table->type() == TableType::References) {
      return Table::create_dummy_table(input_table->column_definitions());
    }
    return input_table;
  }


  const auto chunk_count = input_table->chunk_count();
  const auto row_count = input_table->row_count();
  const auto sort_definitions_size = _sort_definitions.size();

  // --- Key preparation and row_ids ---
  auto field_width = std::vector<size_t>();
  field_width.reserve(sort_definitions_size);

  for (const auto& def : _sort_definitions) {
    const auto sort_col = def.column;
    resolve_data_type(input_table->column_data_type(sort_col), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        std::vector<size_t> max_lengths(chunk_count, 0);
        std::vector<std::shared_ptr<AbstractTask>> jobs;
        jobs.reserve(chunk_count);
        for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
          jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
            const auto& seg = input_table->get_chunk(chunk_id)->get_segment(sort_col);
            size_t local_max = 0;
            segment_iterate<ColumnDataType>(*seg, [&](const auto& val) {
              if (!val.is_null()) local_max = std::max(local_max, val.value().size());
            });
            max_lengths[chunk_id] = local_max;
          }));
        }
        Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
        field_width.emplace_back(*std::max_element(max_lengths.begin(), max_lengths.end()) + STRING_SIZE_LENGTH);
      } else {
        field_width.emplace_back(sizeof(ColumnDataType));
      }
    });
  }


  auto key_width = std::reduce(field_width.begin(), field_width.end(), size_t{0}) + field_width.size();
  auto key_offsets = std::vector<size_t>(sort_definitions_size);
  std::transform_exclusive_scan(field_width.begin(), field_width.end(), key_offsets.begin(), size_t{0}, std::plus<>{},
                                [](auto value) { return value + 1; });

  std::vector<uint8_t> key_buffer;
  key_buffer.reserve(row_count * key_width);

  RowIDPosList row_ids(row_count);
  std::vector<size_t> row_id_offsets(chunk_count, 0);

  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = input_table->get_chunk(ChunkID{chunk_id});
    const auto chunk_size = chunk->size();
    if (chunk_id > 0) row_id_offsets[chunk_id] = row_id_offsets[chunk_id - 1] + input_table->get_chunk(ChunkID{chunk_id - 1})->size();
    for (ChunkOffset row{0}; row < chunk_size; ++row) row_ids[row_id_offsets[chunk_id] + row] = {chunk_id, row};
  }

  const auto preparation_time = timer.lap();


  // --- Key generation ---
  std::vector<std::shared_ptr<AbstractTask>> keygen_jobs;
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    for (size_t index = 0; index < sort_definitions_size; ++index) {
      keygen_jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id, index]() {
        const auto sort_col = _sort_definitions[index].column;
        const auto nulls_first = (_sort_definitions[index].sort_mode == SortMode::AscendingNullsFirst ||
                                  _sort_definitions[index].sort_mode == SortMode::DescendingNullsFirst);
        const auto descending = is_descending(_sort_definitions[index].sort_mode);
        const auto& seg = input_table->get_chunk(ChunkID{chunk_id})->get_segment(sort_col);

        resolve_data_type(input_table->column_data_type(sort_col), [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;
          auto accessor = create_segment_accessor<ColumnDataType>(seg);

          segment_iterate<ColumnDataType>(*seg, [&](const auto& val) {
            const auto row = val.chunk_offset();
            auto* dest = reinterpret_cast<std::byte*>(&key_buffer[(row_id_offsets[chunk_id] + row) * key_width + key_offsets[index]]);
            const bool not_null = !val.is_null();
            const ColumnDataType value = not_null ? val.value() : ColumnDataType{};
            const auto data_len = field_width[index];

            dest[0] = not_null ? std::byte{0} : std::byte{255};
            if (nulls_first) dest[0] = ~dest[0];

            if constexpr (std::is_same_v<ColumnDataType, pmr_string>) hyrise::encode_string(dest + 1, data_len, value);
            else if constexpr (std::is_same_v<ColumnDataType, double>) hyrise::encode_double(dest, value);
            else if constexpr (std::is_same_v<ColumnDataType, float>) hyrise::encode_float(dest, value);
            else if constexpr (std::is_integral<ColumnDataType>::value && std::is_signed_v<ColumnDataType>) hyrise::encode_integer<ColumnDataType>(dest, value);
            else Assert(false, "Unsupported type for sorting");

            if (descending) for (size_t i = 1; i <= data_len; ++i) dest[i] = ~dest[i];
          });
        });
      }));
    }
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(keygen_jobs);
  const auto key_generation_time = timer.lap();

    // --- Sort ---
  if (_aggregates.empty()) {
    auto compare_rows = [&](RowID a, RowID b) {
      auto* key_a = &key_buffer[(row_id_offsets[a.chunk_id] + a.chunk_offset) * key_width];
      auto* key_b = &key_buffer[(row_id_offsets[b.chunk_id] + b.chunk_offset) * key_width];
      return memcmp(key_a, key_b, key_width) < 0;
    };
    _rowid_sorter->sort(row_ids, compare_rows);
  } else {
    std::vector<ColumnID> non_sort_columns;
    for (ColumnID col{0}; col < input_table->column_count(); ++col) {
      bool is_sort_col = false;
      for (const auto& sort_def : _sort_definitions) {
        if (sort_def.column == col) {
          is_sort_col = true;
          break;
        }
      }
      if (!is_sort_col) non_sort_columns.push_back(col);
    }
    // Aggregates exist → use ParallelMergeSorter
    std::vector<AggEntry> agg_entries;
    agg_entries.reserve(row_ids.size());

    for (const auto& row_id : row_ids) {
      std::vector<double> sums;
      sums.reserve(non_sort_columns.size());

    for (const auto col_id : non_sort_columns) {
    double value = 0.0;

    resolve_data_type(input_table->column_data_type(col_id), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      const auto& segment = input_table->get_chunk(row_id.chunk_id)->get_segment(col_id);
      auto accessor = create_segment_accessor<ColumnDataType>(segment);

      if constexpr (std::is_arithmetic_v<ColumnDataType>) {
        const auto opt_value = accessor->access(row_id.chunk_offset);
        if (opt_value) value = static_cast<double>(*opt_value);
      } else {
        value = 0.0; // treat strings / unsupported types as 0 for SUM
      }
    });

    sums.push_back(value);
    }

    agg_entries.push_back({row_id, std::move(sums)});
    }

    auto compare_agg = [&](const AggEntry& a, const AggEntry& b) {
    const auto* key_a =
      &key_buffer[(row_id_offsets[a.row_id.chunk_id] + a.row_id.chunk_offset) * key_width];
    const auto* key_b =
      &key_buffer[(row_id_offsets[b.row_id.chunk_id] + b.row_id.chunk_offset) * key_width];

    return std::memcmp(key_a, key_b, key_width) < 0;
    };

  // --- Sort using parallel merge sorter ---
  _agg_sorter->sort_with_agg(agg_entries, std::function<bool(const AggEntry&, const AggEntry&)>(compare_agg));

  // --- Extract sorted row_ids back ---
  for (size_t i = 0; i < row_ids.size(); ++i) {
    row_ids[i] = agg_entries[i].row_id;
  }
}

  const auto merge_sort_time = timer.lap();

  auto& step_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  step_data.set_step_runtime(OperatorSteps::Preparation, preparation_time);
  step_data.set_step_runtime(OperatorSteps::MaterializeSortColumns, key_generation_time);
  step_data.set_step_runtime(OperatorSteps::Sort, merge_sort_time);

  std::shared_ptr<Table> sorted_table;

  auto must_materialize = _force_materialization == ForceMaterialization::Yes;
  if (!must_materialize && input_table->type() == TableType::References && chunk_count > 1) {
    must_materialize = true;  // Simplified for now
  }
  sorted_table = must_materialize
                     ? write_materialized_output_table(input_table, std::move(row_ids), _output_chunk_size)
                     : write_reference_output_table(input_table, std::move(row_ids), _output_chunk_size);
                 
  // --- Set sorted_by ---
  if (_aggregates.empty()) {
    const auto& final_sort_def = _sort_definitions[0];
    for (ChunkID chunk_id{0}; chunk_id < sorted_table->chunk_count(); ++chunk_id) {
      auto chunk = sorted_table->get_chunk(ChunkID{chunk_id});
      chunk->set_immutable();
      chunk->set_individually_sorted_by(final_sort_def);
    }
  } else {
  // Aggregated sort: output order is not guaranteed to match segment sort semantics
    for (ChunkID chunk_id{0}; chunk_id < sorted_table->chunk_count(); ++chunk_id) {
      sorted_table->get_chunk(chunk_id)->set_immutable();
    }
  }

  const auto write_output_time = timer.lap();
  step_data.set_step_runtime(OperatorSteps::WriteOutput, write_output_time);

  return sorted_table;
}
}  // namespace hyrise