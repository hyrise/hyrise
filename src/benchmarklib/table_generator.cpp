#include "table_generator.hpp"

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
using namespace opossum;

/**
    * Function to cast an integer to the requested type.
    *   - in case of long, the integer is simply casted
    *   - in case of floating types, the integer slighlty modified
    *     and casted to ensure a matissa that is not fully zero'd
    *   - in case of strings, a 10 char string is created that has at least
    *     four leading spaces to ensure that scans have to evaluate at least
    *     the first four chars. The reason is that very often strings are
    *     dates in ERP systems and we assume that at least the year has to be
    *     read before a non-match can be determined. Randomized strings often
    *     lead to unrealistically fast string scans.
    */
template <typename T>
T convert_integer_value(const int input) {
  if constexpr (std::is_integral_v<T>) {
    return static_cast<T>(input);
  } else if constexpr (std::is_floating_point_v<T>) {
    // floating points are slightly shifted to avoid a zero'd mantissa.
    return static_cast<T>(input) * 0.999999f;
  } else {
    // TODO(Bouncner): fix the generation of strings
    const std::vector<const char> chars = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
        'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
        'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y'};
    const size_t chars_base = chars.size();
    Assert(static_cast<double>(input) < std::pow(chars_base, 6),
           "Integer too large. Cannot be represented in six chars.");

    pmr_string result{10, ' '};  // 10 spaces
    if (input == 0) {
      return result;
    }

    const size_t result_char_count =
        std::max(1ul, static_cast<size_t>(std::ceil(std::log(input) / std::log(chars_base))));
    size_t remainder = static_cast<size_t>(input);
    for (auto i = size_t{0}; i < result_char_count; ++i) {
      result[9 - i] = chars[remainder % chars_base];
      remainder = static_cast<size_t>(remainder / chars_base);
    }

    return result;
  }
}

template <typename T>
pmr_concurrent_vector<T> create_typed_segment_values(const std::vector<int>& values) {
  pmr_concurrent_vector<T> result;
  result.reserve(values.size());

  for (const auto& value : values) {
    result.push_back(convert_integer_value<T>(value));
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

std::shared_ptr<Table> TableGenerator::generate_table(const size_t num_columns, const size_t num_rows,
                                                      const ChunkOffset chunk_size,
                                                      const SegmentEncodingSpec segment_encoding_spec) {
  auto table = generate_table({num_columns, {ColumnDataDistribution::make_uniform_config(0.0, _max_different_value)}},
                              {num_columns, {DataType::Int}}, num_rows, chunk_size, UseMvcc::No, false);

  ChunkEncoder::encode_all_chunks(table, segment_encoding_spec);

  return table;
}

std::shared_ptr<Table> TableGenerator::generate_table(
    const std::vector<ColumnDataDistribution>& column_data_distributions,
    const std::vector<DataType>& column_data_types, const size_t num_rows, const ChunkOffset chunk_size,
    const std::vector<SegmentEncodingSpec>& segment_encoding_specs,
    const UseMvcc use_mvcc, const bool numa_distribute_chunks) {
  Assert(column_data_distributions.size() == segment_encoding_specs.size(),
         "Length of value distributions needs to equal length of column encodings.");

  auto table =
      generate_table(column_data_distributions, column_data_types, num_rows, chunk_size, use_mvcc, numa_distribute_chunks);

  ChunkEncoder::encode_all_chunks(table, segment_encoding_specs);

  return table;
}

std::shared_ptr<Table> TableGenerator::generate_table(
    const std::vector<ColumnDataDistribution>& column_data_distributions,
    const std::vector<DataType>& column_data_types, const size_t num_rows, const ChunkOffset chunk_size,
    const UseMvcc use_mvcc, const bool numa_distribute_chunks) {
  Assert(chunk_size != 0ul, "cannot generate table with chunk size 0");
  Assert(column_data_distributions.size() == column_data_types.size(),
         "Length of value distributions needs to equal length of column data types.");
  const auto num_columns = column_data_distributions.size();
  const auto num_chunks = std::ceil(static_cast<double>(num_rows) / static_cast<double>(chunk_size));

  // add column definitions and initialize each value vector
  TableColumnDefinitions column_definitions;
  for (auto column_id = size_t{0}; column_id < num_columns; ++column_id) {
    const auto column_name = "column_" + std::to_string(column_id + 1);
    column_definitions.emplace_back(column_name, column_data_types[column_id]);
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
    auto allocator_ptr_base_segment = get_allocator_for_type<std::shared_ptr<BaseSegment>>(numa_distribute_chunks, node_id);
    auto allocator_chunk = get_allocator_for_type<Chunk>(numa_distribute_chunks, node_id);

    auto segments = Segments(allocator_ptr_base_segment);

    for (auto column_index = ColumnID{0}; column_index < num_columns; ++column_index) {
      node_id = (node_id + 1) % Topology::get().nodes().size();

      resolve_data_type(column_data_types[column_index], [&](const auto column_data_type) {
        using ColumnDataType = typename decltype(column_data_type)::type;
        
        // get typed allocators
        auto allocator_value_segment = get_allocator_for_type<ValueSegment<ColumnDataType>>(numa_distribute_chunks, node_id);
        auto allocator = get_allocator_for_type<ColumnDataType>(numa_distribute_chunks, node_id);

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

        // generate values according to distribution
        for (auto row_offset = size_t{0}; row_offset < chunk_size; ++row_offset) {
          // bounds check
          if (chunk_index * chunk_size + (row_offset + 1) > num_rows) {
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
