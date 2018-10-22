#include "aggregate.hpp"

#include <boost/container/pmr/monotonic_buffer_resource.hpp>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "aggregate/aggregate_traits.hpp"
#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "type_comparison.hpp"
#include "utils/aligned_size.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

Aggregate::Aggregate(const std::shared_ptr<AbstractOperator>& in,
                     const std::vector<AggregateColumnDefinition>& aggregates,
                     const std::vector<ColumnID>& groupby_column_ids)
    : AbstractReadOnlyOperator(OperatorType::Aggregate, in),
      _aggregates(aggregates),
      _groupby_column_ids(groupby_column_ids) {
  Assert(!(aggregates.empty() && groupby_column_ids.empty()),
         "Neither aggregate nor groupby columns have been specified");
}

const std::vector<AggregateColumnDefinition>& Aggregate::aggregates() const { return _aggregates; }

const std::vector<ColumnID>& Aggregate::groupby_column_ids() const { return _groupby_column_ids; }

const std::string Aggregate::name() const { return "Aggregate"; }

const std::string Aggregate::description(DescriptionMode description_mode) const {
  std::stringstream desc;
  desc << "[Aggregate] GroupBy ColumnIDs: ";
  for (size_t groupby_column_idx = 0; groupby_column_idx < _groupby_column_ids.size(); ++groupby_column_idx) {
    desc << _groupby_column_ids[groupby_column_idx];

    if (groupby_column_idx + 1 < _groupby_column_ids.size()) {
      desc << ", ";
    }
  }

  desc << " Aggregates: ";
  for (size_t expression_idx = 0; expression_idx < _aggregates.size(); ++expression_idx) {
    const auto& aggregate = _aggregates[expression_idx];
    desc << aggregate_function_to_string.left.at(aggregate.function);

    if (aggregate.column) {
      desc << "(Column #" << *aggregate.column << ")";
    } else {
      desc << "(*)";
    }

    if (expression_idx + 1 < _aggregates.size()) desc << ", ";
  }
  return desc.str();
}

std::shared_ptr<AbstractOperator> Aggregate::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Aggregate>(copied_input_left, _aggregates, _groupby_column_ids);
}

void Aggregate::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void Aggregate::_on_cleanup() { _contexts_per_column.clear(); }

/*
Visitor context for the partitioning/grouping visitor
*/
template <typename AggregateKey>
struct GroupByContext : SegmentVisitorContext {
  GroupByContext(const std::shared_ptr<const Table>& t, ChunkID chunk, ColumnID column,
                 const std::shared_ptr<AggregateKeys<AggregateKey>>& keys)
      : table_in(t), chunk_id(chunk), column_id(column), hash_keys(keys) {}

  GroupByContext(const std::shared_ptr<BaseSegment>&, const std::shared_ptr<const Table>& referenced_table,
                 const std::shared_ptr<SegmentVisitorContext>& base_context, ChunkID chunk_id,
                 const std::shared_ptr<std::vector<ChunkOffset>>& chunk_offsets)
      : table_in(referenced_table),
        chunk_id(chunk_id),
        column_id(std::static_pointer_cast<GroupByContext>(base_context)->column_id),
        hash_keys(std::static_pointer_cast<GroupByContext>(base_context)->hash_keys),
        chunk_offsets_in(chunk_offsets) {}

  std::shared_ptr<const Table> table_in;
  ChunkID chunk_id;
  const ColumnID column_id;
  std::shared_ptr<AggregateKeys<AggregateKey>> hash_keys;
  std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets_in;
};

/*
Visitor context for the AggregateVisitor.
*/
template <typename ColumnType, typename AggregateType, typename AggregateKey>
struct AggregateContext : SegmentVisitorContext {
  AggregateContext() = default;
  explicit AggregateContext(const std::shared_ptr<GroupByContext<AggregateKey>>& base_context)
      : groupby_context(base_context) {}

  AggregateContext(const std::shared_ptr<BaseSegment>&, const std::shared_ptr<const Table>&,
                   const std::shared_ptr<SegmentVisitorContext>& base_context, ChunkID chunk_id,
                   const std::shared_ptr<std::vector<ChunkOffset>>& chunk_offsets)
      : groupby_context(std::static_pointer_cast<AggregateContext>(base_context)->groupby_context),
        results(std::static_pointer_cast<AggregateContext>(base_context)->results) {
    groupby_context->chunk_id = chunk_id;
    groupby_context->chunk_offsets_in = chunk_offsets;
  }

  std::shared_ptr<GroupByContext<AggregateKey>> groupby_context;
  std::shared_ptr<std::unordered_map<AggregateKey, AggregateResult<AggregateType, ColumnType>, std::hash<AggregateKey>>>
      results;
};

/*
The AggregateFunctionBuilder is used to create the lambda function that will be used by
the AggregateVisitor. It is a separate class because methods cannot be partially specialized.
Therefore, we partially specialize the whole class and define the get_aggregate_function anew every time.
*/
template <typename ColumnType, typename AggregateType>
using AggregateFunctor = std::function<void(const ColumnType&, std::optional<AggregateType>&)>;

template <typename ColumnType, typename AggregateType, AggregateFunction function>
struct AggregateFunctionBuilder {
  AggregateFunctor<ColumnType, AggregateType> get_aggregate_function() { Fail("Invalid aggregate function"); }
};

template <typename ColumnType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnType, AggregateType, AggregateFunction::Min> {
  AggregateFunctor<ColumnType, AggregateType> get_aggregate_function() {
    return [](const ColumnType& new_value, std::optional<AggregateType>& current_aggregate) {
      if (!current_aggregate || value_smaller(new_value, *current_aggregate)) {
        // New minimum found
        current_aggregate = new_value;
      }
      return *current_aggregate;
    };
  }
};

template <typename ColumnType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnType, AggregateType, AggregateFunction::Max> {
  AggregateFunctor<ColumnType, AggregateType> get_aggregate_function() {
    return [](const ColumnType& new_value, std::optional<AggregateType>& current_aggregate) {
      if (!current_aggregate || value_greater(new_value, *current_aggregate)) {
        // New maximum found
        current_aggregate = new_value;
      }
      return *current_aggregate;
    };
  }
};

template <typename ColumnType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnType, AggregateType, AggregateFunction::Sum> {
  AggregateFunctor<ColumnType, AggregateType> get_aggregate_function() {
    return [](const ColumnType& new_value, std::optional<AggregateType>& current_aggregate) {
      // add new value to sum
      if (current_aggregate) {
        *current_aggregate += new_value;
      } else {
        current_aggregate = new_value;
      }
    };
  }
};

template <typename ColumnType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnType, AggregateType, AggregateFunction::Avg> {
  AggregateFunctor<ColumnType, AggregateType> get_aggregate_function() {
    // We reuse Sum here and use it together with aggregate_count to calculate the average
    return AggregateFunctionBuilder<ColumnType, AggregateType, AggregateFunction::Sum>{}.get_aggregate_function();
  }
};

template <typename ColumnType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnType, AggregateType, AggregateFunction::Count> {
  AggregateFunctor<ColumnType, AggregateType> get_aggregate_function() {
    return [](const ColumnType&, std::optional<AggregateType>& current_aggregate) { return std::nullopt; };
  }
};

template <typename ColumnType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnType, AggregateType, AggregateFunction::CountDistinct> {
  AggregateFunctor<ColumnType, AggregateType> get_aggregate_function() {
    return [](const ColumnType&, std::optional<AggregateType>& current_aggregate) { return std::nullopt; };
  }
};

template <typename ColumnDataType, AggregateFunction function, typename AggregateKey>
void Aggregate::_aggregate_segment(ChunkID chunk_id, ColumnID column_index, const BaseSegment& base_segment,
                                   const KeysPerChunk<AggregateKey>& keys_per_chunk) {
  using AggregateType = typename AggregateTraits<ColumnDataType, function>::AggregateType;

  auto aggregator = AggregateFunctionBuilder<ColumnDataType, AggregateType, function>().get_aggregate_function();

  auto& context = *std::static_pointer_cast<AggregateContext<ColumnDataType, AggregateType, AggregateKey>>(
      _contexts_per_column[column_index]);

  auto& results = *context.results;
  const auto& hash_keys = keys_per_chunk[chunk_id];

  // clang-format off
  resolve_segment_type<ColumnDataType>(
      // clang-format on
      base_segment, [&results, &hash_keys, chunk_id, aggregator](const auto& typed_segment) {
        auto iterable = create_iterable_from_segment<ColumnDataType>(typed_segment);

        ChunkOffset chunk_offset{0};

        // Now that all relevant types have been resolved, we can iterate over the segment and build the aggregations.
        iterable.for_each([&, chunk_id, aggregator](const auto& value) {
          auto& hash_entry = results[hash_keys[chunk_offset]];
          hash_entry.row_id = RowID(chunk_id, chunk_offset);

          /**
          * If the value is NULL, the current aggregate value does not change.
          */
          if (!value.is_null()) {
            // If we have a value, use the aggregator lambda to update the current aggregate value for this group
            aggregator(value.value(), hash_entry.current_aggregate);

            // increase value counter
            ++hash_entry.aggregate_count;

            if constexpr (function == AggregateFunction::CountDistinct) {  // NOLINT
              // clang-tidy error: https://bugs.llvm.org/show_bug.cgi?id=35824
              // for the case of CountDistinct, insert this value into the set to keep track of distinct values
              hash_entry.distinct_values.insert(value.value());
            }
          }

          ++chunk_offset;
        });
      });
}

template <typename AggregateKey>
void Aggregate::_aggregate() {
  // We use monotonic_buffer_resource for the vector of vectors that hold the aggregate keys. That is so that we can
  // save time when allocating and we can throw away everything in this temporary structure at once (once the resource
  // gets deleted). Also, we use the scoped_allocator_adaptor to propagate the allocator to all inner vectors.
  // This is suitable here because the amount of memory needed is known from the start. In other places with frequent
  // reallocations, this might make less sense.
  // We use boost over std because libc++ does not yet (July 2018) support monotonic_buffer_resource:
  // https://libcxx.llvm.org/ts1z_status.html
  using AggregateKeysAllocator =
      boost::container::scoped_allocator_adaptor<PolymorphicAllocator<AggregateKeys<AggregateKey>>>;

  auto input_table = input_table_left();

  for ([[maybe_unused]] const auto groupby_column_id : _groupby_column_ids) {
    DebugAssert(groupby_column_id < input_table->column_count(), "GroupBy column index out of bounds");
  }

  // check for invalid aggregates
  for (const auto& aggregate : _aggregates) {
    if (!aggregate.column) {
      if (aggregate.function != AggregateFunction::Count) {
        Fail("Aggregate: Asterisk is only valid with COUNT");
      }
    } else {
      DebugAssert(*aggregate.column < input_table->column_count(), "Aggregate column index out of bounds");
      if (input_table->column_data_type(*aggregate.column) == DataType::String &&
          (aggregate.function == AggregateFunction::Sum || aggregate.function == AggregateFunction::Avg)) {
        Fail("Aggregate: Cannot calculate SUM or AVG on string column");
      }
    }
  }

  /*
  PARTITIONING PHASE
  First we partition the input chunks by the given group key(s).
  This is done by creating a vector that contains the AggregateKey for each row.
  It is gradually built by visitors, one for each group segment.
  */

  KeysPerChunk<AggregateKey> keys_per_chunk;

  {
    // Allocate a temporary memory buffer, for more details see aggregate.hpp
    // This calculation assumes that we use pmr_vector<AggregateKeyEntry> - other data structures use less space, but
    // that is fine
    size_t needed_size_per_aggregate_key =
        aligned_size<AggregateKey>() + _groupby_column_ids.size() * aligned_size<AggregateKeyEntry>();
    size_t needed_size = aligned_size<KeysPerChunk<AggregateKey>>() +
                         input_table->chunk_count() * aligned_size<AggregateKeys<AggregateKey>>() +
                         input_table->row_count() * needed_size_per_aggregate_key;
    needed_size *= 1.1;  // Give it a little bit more, just in case

    auto temp_buffer = boost::container::pmr::monotonic_buffer_resource(needed_size);
    auto allocator = AggregateKeysAllocator{PolymorphicAllocator<AggregateKeys<AggregateKey>>{&temp_buffer}};
    allocator.allocate(1);  // Make sure that the buffer is initialized
    const auto start_next_buffer_size = temp_buffer.next_buffer_size();

    // Create the actual data structure
    keys_per_chunk = KeysPerChunk<AggregateKey>{allocator};
    keys_per_chunk.reserve(input_table->chunk_count());
    for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
      if constexpr (std::is_same_v<AggregateKey, pmr_vector<AggregateKeyEntry>>) {
        keys_per_chunk.emplace_back(input_table->get_chunk(chunk_id)->size(), AggregateKey(_groupby_column_ids.size()));
      } else {
        keys_per_chunk.emplace_back(input_table->get_chunk(chunk_id)->size(), AggregateKey{});
      }
    }

    // Make sure that we did not have to allocate more memory than originally computed
    if (temp_buffer.next_buffer_size() != start_next_buffer_size) {
      // The buffer sizes are increasing when the current buffer is full. We can use this to make sure that we allocated
      // enough space from the beginning on. It would be more intuitive to compare current_buffer(), but this seems to
      // be broken in boost: https://svn.boost.org/trac10/ticket/13639#comment:1
      PerformanceWarning(std::string("needed_size ") + std::to_string(needed_size) +
                         " was not enough and a second buffer was needed");
    }
  }

  // Now that we have the data structures in place, we can start the actual work
  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(_groupby_column_ids.size());

  for (size_t group_column_index = 0; group_column_index < _groupby_column_ids.size(); ++group_column_index) {
    jobs.emplace_back(std::make_shared<JobTask>([&input_table, group_column_index, &keys_per_chunk, this]() {
      const auto column_id = _groupby_column_ids.at(group_column_index);
      const auto data_type = input_table->column_data_type(column_id);

      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        /*
        Store unique IDs for equal values in the groupby column (similar to dictionary encoding).
        The ID 0 is reserved for NULL values. The combined IDs build an AggregateKey for each row.
        */

        // This time, we have no idea how much space we need, so we take some memory and then rely on the automatic
        // resizing. The size is quite random, but since single memory allocations do not cost too much, we rather
        // allocate a bit too much.
        auto temp_buffer = boost::container::pmr::monotonic_buffer_resource(1'000'000);
        auto allocator = PolymorphicAllocator<std::pair<const ColumnDataType, AggregateKeyEntry>>{&temp_buffer};

        auto id_map = std::unordered_map<ColumnDataType, AggregateKeyEntry, std::hash<ColumnDataType>,
                                         std::equal_to<ColumnDataType>, decltype(allocator)>(allocator);
        AggregateKeyEntry id_counter = 1u;

        for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
          const auto chunk_in = input_table->get_chunk(chunk_id);
          const auto base_segment = chunk_in->get_segment(column_id);

          resolve_segment_type<ColumnDataType>(*base_segment, [&](auto& typed_segment) {
            auto iterable = create_iterable_from_segment<ColumnDataType>(typed_segment);

            ChunkOffset chunk_offset{0};
            iterable.for_each([&](const auto& value) {
              if (value.is_null()) {
                if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                  keys_per_chunk[chunk_id][chunk_offset] = 0u;
                } else {
                  keys_per_chunk[chunk_id][chunk_offset][group_column_index] = 0u;
                }
              } else {
                auto inserted = id_map.try_emplace(value.value(), id_counter);
                // store either the current id_counter or the existing ID of the value
                if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                  keys_per_chunk[chunk_id][chunk_offset] = inserted.first->second;
                } else {
                  keys_per_chunk[chunk_id][chunk_offset][group_column_index] = inserted.first->second;
                }

                // if the id_map didn't have the value as a key and a new element was inserted
                if (inserted.second) ++id_counter;
              }

              ++chunk_offset;
            });
          });
        }
      });
    }));
    jobs.back()->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  /*
  AGGREGATION PHASE
  */
  _contexts_per_column = std::vector<std::shared_ptr<SegmentVisitorContext>>(_aggregates.size());

  if (_aggregates.empty()) {
    /*
    Insert a dummy context for the DISTINCT implementation.
    That way, _contexts_per_column will always have at least one context with results.
    This is important later on when we write the group keys into the table.

    We choose int8_t for column type and aggregate type because it's small.
    */
    auto context = std::make_shared<AggregateContext<DistinctColumnType, DistinctAggregateType, AggregateKey>>();
    context->results =
        std::make_shared<std::unordered_map<AggregateKey, AggregateResult<DistinctAggregateType, DistinctColumnType>,
                                            std::hash<AggregateKey>>>();

    _contexts_per_column.push_back(context);
  }

  /**
   * Create an AggregateContext for each column in the input table that a normal (i.e. non-DISTINCT) aggregate is
   * created on. We do this here, and not in the per-chunk-loop below, because there might be no Chunks in the input
   * and _write_aggregate_output() needs these contexts anyway.
   */
  for (ColumnID column_id{0}; column_id < _aggregates.size(); ++column_id) {
    const auto& aggregate = _aggregates[column_id];
    if (!aggregate.column && aggregate.function == AggregateFunction::Count) {
      // SELECT COUNT(*) - we know the template arguments, so we don't need a visitor
      auto context = std::make_shared<AggregateContext<CountColumnType, CountAggregateType, AggregateKey>>();
      context->results =
          std::make_shared<std::unordered_map<AggregateKey, AggregateResult<CountAggregateType, CountColumnType>,
                                              std::hash<AggregateKey>>>();
      _contexts_per_column[column_id] = context;
      continue;
    }
    auto data_type = input_table->column_data_type(*aggregate.column);
    _contexts_per_column[column_id] = _create_aggregate_context<AggregateKey>(data_type, aggregate.function);
  }

  // Process Chunks and perform aggregations
  for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
    auto chunk_in = input_table->get_chunk(chunk_id);

    const auto& hash_keys = keys_per_chunk[chunk_id];

    // Sometimes, gcc is really bad at accessing loop conditions only once, so we cache that here.
    const auto input_chunk_size = chunk_in->size();

    if (_aggregates.empty()) {
      /**
       * DISTINCT implementation
       *
       * In Opossum we handle the SQL keyword DISTINCT by grouping without aggregation.
       *
       * For a query like "SELECT DISTINCT * FROM A;"
       * we would assume that all columns from A are part of 'groupby_columns',
       * respectively any columns that were specified in the projection.
       * The optimizer is responsible to take care of passing in the correct columns.
       *
       * How does this operation work?
       * Distinct rows are retrieved by grouping by vectors of values. Similar as for the usual aggregation
       * these vectors are used as keys in the 'column_results' map.
       *
       * At this point we've got all the different keys from the chunks and accumulate them in 'column_results'.
       * In order to reuse the aggregation implementation, we add a dummy AggregateResult.
       * One could optimize here in the future.
       *
       * Obviously this implementation is also used for plain GroupBy's.
       */

      auto context =
          std::static_pointer_cast<AggregateContext<DistinctColumnType, DistinctAggregateType, AggregateKey>>(
              _contexts_per_column[0]);
      auto& results = *context->results;

      for (ChunkOffset chunk_offset{0}; chunk_offset < input_chunk_size; chunk_offset++) {
        results[hash_keys[chunk_offset]].row_id = RowID(chunk_id, chunk_offset);
      }
    } else {
      ColumnID column_index{0};
      for (const auto& aggregate : _aggregates) {
        /**
         * Special COUNT(*) implementation.
         * Because COUNT(*) does not have a specific target column, we use the maximum ColumnID.
         * We then go through the keys_per_chunk map and count the occurrences of each group key.
         * The results are saved in the regular aggregate_count variable so that we don't need a
         * specific output logic for COUNT(*).
         */
        if (!aggregate.column && aggregate.function == AggregateFunction::Count) {
          auto context = std::static_pointer_cast<AggregateContext<CountColumnType, CountAggregateType, AggregateKey>>(
              _contexts_per_column[column_index]);

          auto& results = *context->results;

          // count occurrences for each group key
          for (ChunkOffset chunk_offset{0}; chunk_offset < input_chunk_size; chunk_offset++) {
            auto& hash_entry = results[hash_keys[chunk_offset]];
            hash_entry.row_id = RowID(chunk_id, chunk_offset);
            ++hash_entry.aggregate_count;
          }

          ++column_index;
          continue;
        }

        auto base_segment = chunk_in->get_segment(*aggregate.column);
        auto data_type = input_table->column_data_type(*aggregate.column);

        /*
        Invoke correct aggregator for each segment
        */

        resolve_data_type(data_type, [&, aggregate](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          switch (aggregate.function) {
            case AggregateFunction::Min:
              _aggregate_segment<ColumnDataType, AggregateFunction::Min, AggregateKey>(chunk_id, column_index,
                                                                                       *base_segment, keys_per_chunk);
              break;
            case AggregateFunction::Max:
              _aggregate_segment<ColumnDataType, AggregateFunction::Max, AggregateKey>(chunk_id, column_index,
                                                                                       *base_segment, keys_per_chunk);
              break;
            case AggregateFunction::Sum:
              _aggregate_segment<ColumnDataType, AggregateFunction::Sum, AggregateKey>(chunk_id, column_index,
                                                                                       *base_segment, keys_per_chunk);
              break;
            case AggregateFunction::Avg:
              _aggregate_segment<ColumnDataType, AggregateFunction::Avg, AggregateKey>(chunk_id, column_index,
                                                                                       *base_segment, keys_per_chunk);
              break;
            case AggregateFunction::Count:
              _aggregate_segment<ColumnDataType, AggregateFunction::Count, AggregateKey>(chunk_id, column_index,
                                                                                         *base_segment, keys_per_chunk);
              break;
            case AggregateFunction::CountDistinct:
              _aggregate_segment<ColumnDataType, AggregateFunction::CountDistinct, AggregateKey>(
                  chunk_id, column_index, *base_segment, keys_per_chunk);
              break;
          }
        });

        ++column_index;
      }
    }
  }

  // add group by columns
  for (const auto column_id : _groupby_column_ids) {
    _output_column_definitions.emplace_back(input_table->column_name(column_id),
                                            input_table->column_data_type(column_id));

    auto groupby_segment =
        make_shared_by_data_type<BaseSegment, ValueSegment>(input_table->column_data_type(column_id), true);
    _groupby_segments.push_back(std::static_pointer_cast<BaseValueSegment>(groupby_segment));
    _output_segments.push_back(groupby_segment);
  }
  /**
   * Write group-by columns.
   *
   * 'results_per_column' always contains at least one element, since there are either GroupBy or Aggregate columns.
   * However, we need to look only at the first element, because the keys for all columns are the same.
   *
   * The following loop is used for both, actual GroupBy columns and DISTINCT columns.
   **/
  if (_aggregates.empty()) {
    auto context = std::static_pointer_cast<AggregateContext<DistinctColumnType, DistinctAggregateType, AggregateKey>>(
        _contexts_per_column[0]);
    auto pos_list = PosList();
    pos_list.reserve(context->results->size());
    for (auto& map : *context->results) {
      pos_list.push_back(map.second.row_id);
    }
    _write_groupby_output(pos_list);
  }

  /*
  Write the aggregated columns to the output
  */
  ColumnID column_index{0};
  for (const auto& aggregate : _aggregates) {
    const auto column = aggregate.column;

    // Output column for COUNT(*). int is chosen arbitrarily.
    const auto data_type = !column ? DataType::Int : input_table->column_data_type(*column);

    resolve_data_type(data_type, [&, column_index](auto type) {
      _write_aggregate_output<AggregateKey>(type, column_index, aggregate.function);
    });

    ++column_index;
  }
}

std::shared_ptr<const Table> Aggregate::_on_execute() {
  // We do not want the overhead of a vector with heap storage when we have a limited number of aggregate columns.
  // The reason we only have specializations up to 2 is because every specialization increases the compile time.
  // Also, we need to make sure that there are tests for at least the first case, one array case, and the fallback.
  switch (_groupby_column_ids.size()) {
    case 0:
    case 1:
      // No need for a complex data structure if we only have one entry
      _aggregate<AggregateKeyEntry>();
      break;
    case 2:
      // We need to explicitly list all array sizes that we want to support
      _aggregate<std::array<AggregateKeyEntry, 2>>();
      break;
    default:
      PerformanceWarning("No std::array implementation initialized - falling back to vector");
      _aggregate<pmr_vector<AggregateKeyEntry>>();
      break;
  }

  // Write the output
  auto output = std::make_shared<Table>(_output_column_definitions, TableType::Data);
  output->append_chunk(_output_segments);

  return output;
}

/*
The following template functions write the aggregated values for the different aggregate functions.
They are separate and templated to avoid compiler errors for invalid type/function combinations.
*/
// MIN, MAX, SUM write the current aggregated value
template <typename ColumnType, typename AggregateType, AggregateFunction func, typename AggregateKey>
std::enable_if_t<func == AggregateFunction::Min || func == AggregateFunction::Max || func == AggregateFunction::Sum,
                 void>
write_aggregate_values(std::shared_ptr<ValueSegment<AggregateType>> segment,
                       std::shared_ptr<std::unordered_map<AggregateKey, AggregateResult<AggregateType, ColumnType>,
                                                          std::hash<AggregateKey>>>
                           results) {
  DebugAssert(segment->is_nullable(), "Aggregate: Output segment needs to be nullable");

  auto& values = segment->values();
  auto& null_values = segment->null_values();

  values.resize(results->size());
  null_values.resize(results->size());

  size_t i = 0;
  for (auto& kv : *results) {
    null_values[i] = !kv.second.current_aggregate;

    if (kv.second.current_aggregate) {
      values[i] = *kv.second.current_aggregate;
    }
    ++i;
  }
}

// COUNT writes the aggregate counter
template <typename ColumnType, typename AggregateType, AggregateFunction func, typename AggregateKey>
std::enable_if_t<func == AggregateFunction::Count, void> write_aggregate_values(
    std::shared_ptr<ValueSegment<AggregateType>> segment,
    std::shared_ptr<
        std::unordered_map<AggregateKey, AggregateResult<AggregateType, ColumnType>, std::hash<AggregateKey>>>
        results) {
  DebugAssert(!segment->is_nullable(), "Aggregate: Output segment for COUNT shouldn't be nullable");

  auto& values = segment->values();
  values.resize(results->size());

  size_t i = 0;
  for (auto& kv : *results) {
    values[i] = kv.second.aggregate_count;
    ++i;
  }
}

// COUNT(DISTINCT) writes the number of distinct values
template <typename ColumnType, typename AggregateType, AggregateFunction func, typename AggregateKey>
std::enable_if_t<func == AggregateFunction::CountDistinct, void> write_aggregate_values(
    std::shared_ptr<ValueSegment<AggregateType>> segment,
    std::shared_ptr<
        std::unordered_map<AggregateKey, AggregateResult<AggregateType, ColumnType>, std::hash<AggregateKey>>>
        results) {
  DebugAssert(!segment->is_nullable(), "Aggregate: Output segment for COUNT shouldn't be nullable");

  auto& values = segment->values();
  values.resize(results->size());

  size_t i = 0;
  for (auto& kv : *results) {
    values[i] = kv.second.distinct_values.size();
    ++i;
  }
}

// AVG writes the calculated average from current aggregate and the aggregate counter
template <typename ColumnType, typename AggregateType, AggregateFunction func, typename AggregateKey>
std::enable_if_t<func == AggregateFunction::Avg && std::is_arithmetic_v<AggregateType>, void> write_aggregate_values(
    std::shared_ptr<ValueSegment<AggregateType>> segment,
    std::shared_ptr<
        std::unordered_map<AggregateKey, AggregateResult<AggregateType, ColumnType>, std::hash<AggregateKey>>>
        results) {
  DebugAssert(segment->is_nullable(), "Aggregate: Output segment needs to be nullable");

  auto& values = segment->values();
  auto& null_values = segment->null_values();

  values.resize(results->size());
  null_values.resize(results->size());

  size_t i = 0;
  for (auto& kv : *results) {
    null_values[i] = !kv.second.current_aggregate;

    if (kv.second.current_aggregate) {
      values[i] = *kv.second.current_aggregate / static_cast<AggregateType>(kv.second.aggregate_count);
    }
    ++i;
  }
}

// AVG is not defined for non-arithmetic types. Avoiding compiler errors.
template <typename ColumnType, typename AggregateType, AggregateFunction func, typename AggregateKey>
std::enable_if_t<func == AggregateFunction::Avg && !std::is_arithmetic_v<AggregateType>, void> write_aggregate_values(
    std::shared_ptr<ValueSegment<AggregateType>>,
    std::shared_ptr<
        std::unordered_map<AggregateKey, AggregateResult<AggregateType, ColumnType>, std::hash<AggregateKey>>>) {
  Fail("Invalid aggregate");
}

void Aggregate::_write_groupby_output(PosList& pos_list) {
  auto input_table = input_table_left();

  for (size_t group_column_index = 0; group_column_index < _groupby_column_ids.size(); ++group_column_index) {
    auto base_segments = std::vector<std::shared_ptr<const BaseSegment>>();
    for (const auto& chunk : input_table->chunks()) {
      base_segments.push_back(chunk->get_segment(_groupby_column_ids[group_column_index]));
    }
    _groupby_segments[group_column_index]->reserve(pos_list.size());
    for (const auto row_id : pos_list) {
      _groupby_segments[group_column_index]->append((*base_segments[row_id.chunk_id])[row_id.chunk_offset]);
    }
  }
}

template <typename AggregateKey, typename ColumnType>
void Aggregate::_write_aggregate_output(boost::hana::basic_type<ColumnType> type, ColumnID column_index,
                                        AggregateFunction function) {
  switch (function) {
    case AggregateFunction::Min:
      write_aggregate_output<ColumnType, AggregateFunction::Min, AggregateKey>(column_index);
      break;
    case AggregateFunction::Max:
      write_aggregate_output<ColumnType, AggregateFunction::Max, AggregateKey>(column_index);
      break;
    case AggregateFunction::Sum:
      write_aggregate_output<ColumnType, AggregateFunction::Sum, AggregateKey>(column_index);
      break;
    case AggregateFunction::Avg:
      write_aggregate_output<ColumnType, AggregateFunction::Avg, AggregateKey>(column_index);
      break;
    case AggregateFunction::Count:
      write_aggregate_output<ColumnType, AggregateFunction::Count, AggregateKey>(column_index);
      break;
    case AggregateFunction::CountDistinct:
      write_aggregate_output<ColumnType, AggregateFunction::CountDistinct, AggregateKey>(column_index);
      break;
  }
}

template <typename ColumnType, AggregateFunction function, typename AggregateKey>
void Aggregate::write_aggregate_output(ColumnID column_index) {
  // retrieve type information from the aggregation traits
  typename AggregateTraits<ColumnType, function>::AggregateType aggregate_type;
  auto aggregate_data_type = AggregateTraits<ColumnType, function>::AGGREGATE_DATA_TYPE;

  const auto& aggregate = _aggregates[column_index];

  if (aggregate_data_type == DataType::Null) {
    // if not specified, it’s the input column’s type
    aggregate_data_type = input_table_left()->column_data_type(*aggregate.column);
  }

  // Generate column name, TODO(anybody), actually, the AggregateExpression can do this, but the Aggregate operator
  // doesn't use Expressions, yet
  std::stringstream column_name_stream;
  if (aggregate.function == AggregateFunction::CountDistinct) {
    column_name_stream << "COUNT(DISTINCT ";
  } else {
    column_name_stream << aggregate_function_to_string.left.at(aggregate.function) << "(";
  }

  if (aggregate.column) {
    column_name_stream << input_table_left()->column_name(*aggregate.column);
  } else {
    column_name_stream << "*";
  }
  column_name_stream << ")";

  constexpr bool NEEDS_NULL = (function != AggregateFunction::Count && function != AggregateFunction::CountDistinct);
  _output_column_definitions.emplace_back(column_name_stream.str(), aggregate_data_type, NEEDS_NULL);

  auto output_segment = std::make_shared<ValueSegment<decltype(aggregate_type)>>(NEEDS_NULL);

  auto context = std::static_pointer_cast<AggregateContext<ColumnType, decltype(aggregate_type), AggregateKey>>(
      _contexts_per_column[column_index]);

  // write all group keys into the respective columns
  if (column_index == 0) {
    auto pos_list = PosList();
    pos_list.reserve(context->results->size());
    for (auto& map : *context->results) {
      pos_list.push_back(map.second.row_id);
    }
    _write_groupby_output(pos_list);
  }

  // write aggregated values into the segment
  if (!context->results->empty()) {
    write_aggregate_values<ColumnType, decltype(aggregate_type), function, AggregateKey>(output_segment,
                                                                                         context->results);
  } else if (_groupby_segments.empty()) {
    // If we did not GROUP BY anything and we have no results, we need to add NULL for most aggregates and 0 for count
    output_segment->values().push_back(decltype(aggregate_type){});
    if (function != AggregateFunction::Count && function != AggregateFunction::CountDistinct) {
      output_segment->null_values().push_back(true);
    }
  }

  _output_segments.push_back(output_segment);
}

template <typename AggregateKey>
std::shared_ptr<SegmentVisitorContext> Aggregate::_create_aggregate_context(const DataType data_type,
                                                                            const AggregateFunction function) const {
  std::shared_ptr<SegmentVisitorContext> context;
  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    switch (function) {
      case AggregateFunction::Min:
        context = _create_aggregate_context_impl<ColumnDataType, AggregateFunction::Min, AggregateKey>();
        break;
      case AggregateFunction::Max:
        context = _create_aggregate_context_impl<ColumnDataType, AggregateFunction::Max, AggregateKey>();
        break;
      case AggregateFunction::Sum:
        context = _create_aggregate_context_impl<ColumnDataType, AggregateFunction::Sum, AggregateKey>();
        break;
      case AggregateFunction::Avg:
        context = _create_aggregate_context_impl<ColumnDataType, AggregateFunction::Avg, AggregateKey>();
        break;
      case AggregateFunction::Count:
        context = _create_aggregate_context_impl<ColumnDataType, AggregateFunction::Count, AggregateKey>();
        break;
      case AggregateFunction::CountDistinct:
        context = _create_aggregate_context_impl<ColumnDataType, AggregateFunction::CountDistinct, AggregateKey>();
        break;
    }
  });
  return context;
}

template <typename ColumnDataType, AggregateFunction aggregate_function, typename AggregateKey>
std::shared_ptr<SegmentVisitorContext> Aggregate::_create_aggregate_context_impl() const {
  const auto context = std::make_shared<AggregateContext<
      ColumnDataType, typename AggregateTraits<ColumnDataType, aggregate_function>::AggregateType, AggregateKey>>();
  context->results = std::make_shared<typename decltype(context->results)::element_type>();
  return context;
}

}  // namespace opossum
