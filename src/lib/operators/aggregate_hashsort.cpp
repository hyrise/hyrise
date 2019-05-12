#include "aggregate_hashsort.hpp"

#include "storage/segment_iterate.hpp"

namespace {

using namespace opossum;  // NOLINT

class BaseRunSegment {
 public:
  virtual ~BaseRunSegment() = default;
};

template<typename T>
class GroupByRunSegment : public BaseRunSegment {
 public:
  GroupByRunSegment(std::vector<T> &&values, std::vector<bool> &&null_values) :
  values(std::move(values)),
  null_values(std::move(null_values)) {}

  std::vector<T> values;
  std::vector<bool> null_values;
};

template<typename T>
class AggregateRunSegment : public BaseRunSegment {
 public:
  AggregateRunSegment(std::vector<T> &&values): values(std::move(values)) {}

  std::vector<T> values;
};

struct Run {
  std::vector<size_t> hashes;
  std::vector<std::unique_ptr<BaseRunSegment>> group_by_segments;
  std::vector<std::unique_ptr<BaseRunSegment>> aggregate_segments;
  bool is_aggregated{false};
};

size_t determine_run_length() {
  return 100; // TODO(moritz)
}

struct ColumnRange {
  const Table& table;
  const ColumnID column_id;
  const size_t row_count;
  const ChunkID first_chunk_id;
  const ChunkOffset first_chunk_begin_offset;
  const ChunkID last_chunk_id;
  const ChunkOffset last_chunk_end_offset;

  template<typename T, typename F>
  void for_each(const F& f) const {
    auto offset = size_t{0};
    for (auto chunk_id = first_chunk_id; chunk_id <= last_chunk_id; ++chunk_id) {
      const auto &chunk = *table.chunks()[chunk_id];
      const auto begin_offset = chunk_id == first_chunk_id ? first_chunk_begin_offset : ChunkOffset{0};
      const auto end_offset = chunk_id == last_chunk_id ? last_chunk_end_offset : chunk.size();
      const auto &segment = *chunk.get_segment(column_id);

      segment_with_iterators<T>(segment, [&](auto begin, auto end) {
        end = begin;
        begin.advance(begin_offset);
        end.advance(end_offset);

        std::for_each(begin, end, [&](const auto& segment_position) {
          f(segment_position, offset);
          ++offset;
        });
      });
    }
  }
};

template<typename T>
std::unique_ptr<GroupByRunSegment<T>> materialize_group_by_run_segment(const ColumnRange& range) {

  const auto is_nullable = range.table.column_is_nullable(range.column_id);

  auto values = std::vector<T>(range.row_count);
  auto null_values = std::vector<bool>(is_nullable ? range.row_count : 0);

  auto values_iter = values.begin();
  auto null_values_iter = null_values.begin();

  range.for_each<T>([&](const auto &segment_position, const auto offset) {
    if (is_nullable) {
      *null_values_iter = segment_position.is_null();
      ++null_values_iter;
    }

    *values_iter = segment_position.value();
    ++values_iter;
  });

  return std::make_unique<GroupByRunSegment<T>>(std::move(values), std::move(null_values));
}

template<typename T>
std::unique_ptr<BaseRunSegment> materialize_aggregate_run_segment(const ColumnRange& range, const AggregateFunction aggregate_function) {
  switch (aggregate_function) {
    case AggregateFunction::Min:
    case AggregateFunction::Max: {
      auto values = std::vector<std::optional<T>>(range.row_count);
      range.for_each<T>([&](const auto &segment_position, const auto offset) {
        if (!segment_position.is_null()) {
          values[offset] = segment_position.value();
        }
      });

      return std::make_unique<AggregateRunSegment<std::optional<T>>>(std::move(values));
    } break;

    case AggregateFunction::Sum: {
      auto values = std::vector<T>(range.row_count);
      range.for_each<T>([&](const auto &segment_position, const auto offset) {
        values[offset] = segment_position.is_null() ? T{0} : segment_position.value();
      });

      return std::make_unique<AggregateRunSegment<T>>(std::move(values));
    } break;

    case AggregateFunction::Avg: {
      auto values = std::vector<std::vector<T>>(range.row_count);
      range.for_each<T>([&](const auto &segment_position, const auto offset) {
        if (!segment_position.is_null()) {
          values[offset].emplace_back(segment_position.value());
        }
      });

      return std::make_unique<AggregateRunSegment<std::vector<T>>>(std::move(values));
    } break;

    case AggregateFunction::Count: {
    } break;

    case AggregateFunction::CountDistinct: {
      auto values = std::vector<std::unordered_set<T>>(range.row_count);
      range.for_each<T>([&](const auto &segment_position, const auto offset) {
        if (!segment_position.is_null()) {
          values[offset].emplace(segment_position.value());
        }
      });

      return std::make_unique<AggregateRunSegment<std::unordered_set<T>>>(std::move(values));
    } break;
  }

  return {};
}

std::vector<Run> aggregate(std::vector<Run>&& runs, const size_t level) {
  // run_matrix[partition_idx][run_idx]
  std::vector<std::vector<Run>> run_matrix;



  for (auto& run : runs) {

  }
}

}  // namespace

namespace opossum {

AggregateHashSort::AggregateHashSort(const std::shared_ptr<AbstractOperator> &in,
                                     const std::vector<AggregateColumnDefinition> &aggregates,
                                     const std::vector<ColumnID> &groupby_column_ids)
: AbstractAggregateOperator(in, aggregates, groupby_column_ids) {}

const std::string AggregateHashSort::name() const { return "AggregateHashSort"; }

std::shared_ptr<AbstractOperator> AggregateHashSort::_on_deep_copy(
const std::shared_ptr<AbstractOperator> &copied_input_left,
const std::shared_ptr<AbstractOperator> &copied_input_right) const {
  return std::make_shared<AggregateHashSort>(copied_input_left, _aggregates, _groupby_column_ids);
}

void AggregateHashSort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant> &parameters) {}

void AggregateHashSort::_on_cleanup() {}

std::shared_ptr<const Table> AggregateHashSort::_on_execute() {
  auto &input_table = *input_table_left();

  const auto run_length = determine_run_length();

  auto run_count = input_table.row_count() / run_length;
  auto last_run_length = input_table.row_count() % run_length;
  if (last_run_length == 0) {
    last_run_length = run_length;
  } else {
    ++run_count;
  }

  /**
   * Initialize Runs
   */
  auto current_run_first_chunk_id = ChunkID{0};
  auto current_run_first_chunk_begin_offset = ChunkOffset{0};

  auto runs = std::vector<Run>{run_count};
  for (auto run_idx = size_t{0}; run_idx < runs.size(); ++run_idx) {
    const auto current_run_length = run_idx + 1 != runs.size() ? run_length : last_run_length;

    // Determine source range for current run
    auto current_run_last_chunk_id = current_run_first_chunk_id;
    auto current_run_last_chunk_end_offset = current_run_first_chunk_begin_offset;
    auto current_run_remaining_rows = current_run_length;
    while (true) {
      const auto current_chunk_begin_offset =
      current_run_last_chunk_id == current_run_first_chunk_id ? current_run_first_chunk_begin_offset : ChunkOffset{0};
      const auto current_chunk_remaining_rows =
      input_table.get_chunk(current_run_last_chunk_id)->size() - current_chunk_begin_offset;
      const auto current_chunk_take_rows = std::min<size_t>(current_chunk_remaining_rows, current_run_remaining_rows);

      current_run_last_chunk_end_offset += current_chunk_take_rows;
      current_run_remaining_rows -= current_chunk_take_rows;

      if (current_run_remaining_rows == 0) {
        current_run_last_chunk_end_offset = current_chunk_begin_offset + current_chunk_take_rows;
        break;
      }

      ++current_run_last_chunk_id;
    }

    std::cout << "Run " << run_idx << " from (" << current_run_first_chunk_id << ", "
              << current_run_first_chunk_begin_offset << ") -> " <<
              "(" << current_run_last_chunk_id << ", " << current_run_last_chunk_end_offset << ")" << std::endl;

    // ...

    auto &run = runs[run_idx];
    run.hashes.resize(current_run_length);

    // Initialize and materialize group by columns
    run.group_by_segments.resize(_groupby_column_ids.size());
    for (auto output_group_by_column_id = ColumnID{0};
         output_group_by_column_id < _groupby_column_ids.size(); ++output_group_by_column_id) {
      const auto input_group_by_column_id = _groupby_column_ids[output_group_by_column_id];

      resolve_data_type(input_table.column_data_type(input_group_by_column_id), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        run.group_by_segments[output_group_by_column_id] = materialize_group_by_run_segment<ColumnDataType>({
        input_table, input_group_by_column_id, current_run_length, current_run_first_chunk_id,
        current_run_first_chunk_begin_offset,
        current_run_last_chunk_id, current_run_last_chunk_end_offset});
      });
    }

    // Initialize and materialize aggregate columns
    run.aggregate_segments.resize(_aggregates.size());
    for (auto output_aggregate_column_id = ColumnID{0};
         output_aggregate_column_id < _aggregates.size(); ++output_aggregate_column_id) {
      const auto &aggregate = _aggregates[output_aggregate_column_id];

      if (aggregate.function == AggregateFunction::Count && !aggregate.column) {
      } else {
        resolve_data_type(input_table.column_data_type(*aggregate.column), [&](const auto data_type_t) {
          using ColumnDataType = typename decltype(data_type_t)::type;
          run.aggregate_segments[output_aggregate_column_id] = materialize_aggregate_run_segment<ColumnDataType>(
          {input_table, *aggregate.column, current_run_length, current_run_first_chunk_id,
          current_run_first_chunk_begin_offset,
          current_run_last_chunk_id, current_run_last_chunk_end_offset}, aggregate.function);
        });
      }
    }

    current_run_first_chunk_id = current_run_last_chunk_id;
    current_run_first_chunk_begin_offset = current_run_last_chunk_end_offset;
  }

  return nullptr;
}


}  // namespace opossum
