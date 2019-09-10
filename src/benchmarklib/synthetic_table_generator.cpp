#include "synthetic_table_generator.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "boost/math/distributions/pareto.hpp"
#include "boost/math/distributions/skew_normal.hpp"
#include "boost/math/distributions/uniform.hpp"

#include "scheduler/topology.hpp"

#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"

namespace {

using namespace opossum;  // NOLINT

template <typename T>
pmr_concurrent_vector<T> create_typed_segment_values(const std::vector<int>& values) {
  pmr_concurrent_vector<T> result;
  result.reserve(values.size());

  for (const auto& value : values) {
    result.push_back(SyntheticTableGenerator::convert_integer_value<T>(value));
  }

  return result;
}

template <typename T>
PolymorphicAllocator<T> get_allocator_for_type(const size_t node_id, const bool distribute_round_robin = false) {
#if HYRISE_NUMA_SUPPORT
  if (distribute_round_robin) {
    const auto memory_resource = Topology::get().get_memory_resource(node_id);
    return PolymorphicAllocator<T>{memory_resource};
  }
#endif

  return PolymorphicAllocator<T>{};
}

}  // namespace

namespace opossum {

std::shared_ptr<Table> SyntheticTableGenerator::generate_table(const size_t num_columns, const size_t num_rows,
                                                      const ChunkOffset chunk_size,
                                                      const SegmentEncodingSpec segment_encoding_spec) {
  auto table = generate_table({num_columns, {ColumnDataDistribution::make_uniform_config(0.0, _max_different_value)}},
                              {num_columns, {DataType::Int}}, num_rows, chunk_size, std::nullopt, UseMvcc::No, false);

  ChunkEncoder::encode_all_chunks(table, segment_encoding_spec);

  return table;
}

std::shared_ptr<Table> SyntheticTableGenerator::generate_table(
    const std::vector<ColumnDataDistribution>& column_data_distributions,
    const std::vector<DataType>& column_data_types, const size_t num_rows, const ChunkOffset chunk_size,
    const std::vector<SegmentEncodingSpec>& segment_encoding_specs,
    const std::optional<std::vector<std::string>> column_names, const UseMvcc use_mvcc,
    const bool numa_distribute_chunks) {
  Assert(column_data_distributions.size() == segment_encoding_specs.size(),
         "Length of value distributions needs to equal length of column encodings.");

  auto table = generate_table(column_data_distributions, column_data_types, num_rows, chunk_size, column_names,
                              use_mvcc, numa_distribute_chunks);

  ChunkEncoder::encode_all_chunks(table, segment_encoding_specs);

  return table;
}

std::shared_ptr<Table> SyntheticTableGenerator::generate_table(
    const std::vector<ColumnDataDistribution>& column_data_distributions,
    const std::vector<DataType>& column_data_types, const size_t num_rows, const ChunkOffset chunk_size,
    const std::optional<std::vector<std::string>> column_names, const UseMvcc use_mvcc,
    const bool numa_distribute_chunks) {
  Assert(chunk_size != 0ul, "cannot generate table with chunk size 0");
  Assert(column_data_distributions.size() == column_data_types.size(),
         "Length of value distributions needs to equal length of column data types.");
  if (column_names) {
    Assert(column_data_distributions.size() == column_names->size(),
           "When set, the number of column names needs to equal number of value distributions.");
  }

  const auto num_columns = column_data_distributions.size();
  const auto num_chunks = std::ceil(static_cast<double>(num_rows) / static_cast<double>(chunk_size));

  // add column definitions and initialize each value vector
  TableColumnDefinitions column_definitions;
  for (auto column_id = size_t{0}; column_id < num_columns; ++column_id) {
    const auto column_name = column_names ? (*column_names)[column_id] : "column_" + std::to_string(column_id + 1);
    column_definitions.emplace_back(column_name, column_data_types[column_id], false);
  }
  std::shared_ptr<Table> table = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, use_mvcc);

  std::random_device rd;
  // using mt19937 because std::default_random engine is not guaranteed to be a sensible default
  auto pseudorandom_engine = std::mt19937{};

  auto probability_dist = std::uniform_real_distribution{0.0, 1.0};
  auto generate_value_by_distribution_type = std::function<int(void)>{};

  pseudorandom_engine.seed(rd());

  auto node_id = size_t{0};
  for (auto chunk_index = ChunkOffset{0}; chunk_index < num_chunks; ++chunk_index) {
    // Obtain general (non-typed) allocators
    auto allocator_ptr_base_segment =
        get_allocator_for_type<std::shared_ptr<BaseSegment>>(node_id, numa_distribute_chunks);
    auto allocator_chunk = get_allocator_for_type<Chunk>(node_id, numa_distribute_chunks);

    auto segments = Segments(allocator_ptr_base_segment);

    for (auto column_index = ColumnID{0}; column_index < num_columns; ++column_index) {
      node_id = (node_id + 1) % Topology::get().nodes().size();

      resolve_data_type(column_data_types[column_index], [&](const auto column_data_type) {
        using ColumnDataType = typename decltype(column_data_type)::type;

        // get typed allocators
        auto allocator_value_segment =
            get_allocator_for_type<ValueSegment<ColumnDataType>>(node_id, numa_distribute_chunks);
        auto allocator = get_allocator_for_type<ColumnDataType>(node_id, numa_distribute_chunks);

        std::vector<int> values;
        values.reserve(chunk_size);
        const auto& column_data_distribution = column_data_distributions[column_index];

        // generate distribution from column configuration
        switch (column_data_distribution.distribution_type) {
          case DataDistributionType::Uniform: {
            auto uniform_dist = boost::math::uniform_distribution<double>{column_data_distribution.min_value,
                                                                          column_data_distribution.max_value};
            generate_value_by_distribution_type = [uniform_dist, &probability_dist, &pseudorandom_engine]() {
              const auto probability = probability_dist(pseudorandom_engine);
              return static_cast<int>(std::floor(boost::math::quantile(uniform_dist, probability)));
            };
            break;
          }
          case DataDistributionType::NormalSkewed: {
            auto skew_dist = boost::math::skew_normal_distribution<double>{column_data_distribution.skew_location,
                                                                           column_data_distribution.skew_scale,
                                                                           column_data_distribution.skew_shape};
            generate_value_by_distribution_type = [skew_dist, &probability_dist, &pseudorandom_engine]() {
              const auto probability = probability_dist(pseudorandom_engine);
              return static_cast<int>(std::round(boost::math::quantile(skew_dist, probability) * 10));
            };
            break;
          }
          case DataDistributionType::Pareto: {
            auto pareto_dist = boost::math::pareto_distribution<double>{column_data_distribution.pareto_scale,
                                                                        column_data_distribution.pareto_shape};
            generate_value_by_distribution_type = [pareto_dist, &probability_dist, &pseudorandom_engine]() {
              const auto probability = probability_dist(pseudorandom_engine);
              return static_cast<int>(std::floor(boost::math::quantile(pareto_dist, probability)));
            };
            break;
          }
        }

        // Generate values according to distribution. We first add min and max values to avoid hard-to-control
        // pruning via dictionaries. In the main loop, we then run (num_rows/chunk_size)-2 times.
        values.push_back(static_cast<int>(column_data_distribution.min_value));
        values.push_back(static_cast<int>(column_data_distribution.max_value));
        for (auto row_offset = size_t{0}; row_offset < chunk_size - 2; ++row_offset) {
          // bounds check
          if (chunk_index * chunk_size + (row_offset + 1) > num_rows - 2) {
            break;
          }
          values.push_back(generate_value_by_distribution_type());
        }

        auto typed_values = create_typed_segment_values<ColumnDataType>(values);
        segments.push_back(std::allocate_shared<ValueSegment<ColumnDataType>>(allocator_value_segment,
                                                                              std::move(typed_values), allocator));
      });
      // add full chunk to table
      if (column_index == num_columns - 1) {
        const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
        table->append_chunk(segments, mvcc_data, allocator_chunk);
      }
    }
  }

  return table;
}
}  // namespace opossum
