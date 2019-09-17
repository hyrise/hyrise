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
  pmr_concurrent_vector<T> result(values.size());

  auto insert_position = size_t{0};
  for (const auto& value : values) {
    result[insert_position++] = SyntheticTableGenerator::generate_value<T>(value);
  }

  return result;
}

}  // namespace

namespace opossum {

std::shared_ptr<Table> SyntheticTableGenerator::generate_table(const size_t num_columns, const size_t num_rows,
                                                               const ChunkOffset chunk_size,
                                                               const SegmentEncodingSpec segment_encoding_spec) {
  auto table = generate_table({num_columns, {ColumnDataDistribution::make_uniform_config(0.0, _max_different_value)}},
                              {num_columns, {DataType::Int}}, num_rows, chunk_size, std::nullopt, std::nullopt, UseMvcc::No);

  ChunkEncoder::encode_all_chunks(table, segment_encoding_spec);

  return table;
}

std::shared_ptr<Table> SyntheticTableGenerator::generate_table(
    const std::vector<ColumnDataDistribution>& column_data_distributions,
    const std::vector<DataType>& column_data_types, const size_t num_rows, const ChunkOffset chunk_size,
    const std::optional<std::vector<SegmentEncodingSpec>>& segment_encoding_specs,
    const std::optional<std::vector<std::string>>& column_names, const UseMvcc use_mvcc) {
  Assert(chunk_size != 0ul, "cannot generate table with chunk size 0");
  Assert(column_data_distributions.size() == column_data_types.size(),
         "Length of value distributions needs to equal length of column data types.");
  if (column_names) {
    Assert(column_data_distributions.size() == column_names->size(),
           "When set, the number of column names needs to equal number of value distributions.");
  }
  if (segment_encoding_specs) {
    Assert(column_data_distributions.size() == segment_encoding_specs->size(),
         "Length of value distributions needs to equal length of column encodings.");
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

  std::random_device random_device;
  // Using std::mt19937 over std::default_random_engine since it provides better guarantees on the randomness
  // of values being created (see discussion: https://stackoverflow.com/q/30240899/1147726)
  auto pseudorandom_engine = std::mt19937{};

  pseudorandom_engine.seed(random_device());

  for (auto chunk_index = ChunkOffset{0}; chunk_index < num_chunks; ++chunk_index) {
    Segments segments(num_columns);
    for (auto column_index = ColumnID{0}; column_index < num_columns; ++column_index) {
      resolve_data_type(column_data_types[column_index], [&](const auto column_data_type) {
        using ColumnDataType = typename decltype(column_data_type)::type;

        std::vector<int> values;
        values.reserve(chunk_size);
        const auto& column_data_distribution = column_data_distributions[column_index];

        auto probability_dist = std::uniform_real_distribution{0.0, 1.0};
        auto generate_value_by_distribution_type = std::function<int(void)>{};

        // generate distribution from column configuration
        switch (column_data_distribution.distribution_type) {
          case DataDistributionType::Uniform: {
            const auto uniform_dist = boost::math::uniform_distribution<double>{column_data_distribution.min_value,
                                                                                column_data_distribution.max_value};
            generate_value_by_distribution_type = [uniform_dist, &probability_dist, &pseudorandom_engine]() {
              const auto probability = probability_dist(pseudorandom_engine);
              return static_cast<int>(std::round(boost::math::quantile(uniform_dist, probability)));
            };
            break;
          }
          case DataDistributionType::NormalSkewed: {
            const auto skew_dist = boost::math::skew_normal_distribution<double>{column_data_distribution.skew_location,
                                                                                 column_data_distribution.skew_scale,
                                                                                 column_data_distribution.skew_shape};
            generate_value_by_distribution_type = [skew_dist, &probability_dist, &pseudorandom_engine]() {
              const auto probability = probability_dist(pseudorandom_engine);
              return static_cast<int>(std::round(boost::math::quantile(skew_dist, probability) * 10));
            };
            break;
          }
          case DataDistributionType::Pareto: {
            const auto pareto_dist = boost::math::pareto_distribution<double>{column_data_distribution.pareto_scale,
                                                                              column_data_distribution.pareto_shape};
            generate_value_by_distribution_type = [pareto_dist, &probability_dist, &pseudorandom_engine]() {
              const auto probability = probability_dist(pseudorandom_engine);
              return static_cast<int>(std::round(boost::math::quantile(pareto_dist, probability)));
            };
            break;
          }
        }

        /**
        * Generate values according to distribution. We first add the given min and max values of that column to avoid
        * early exists via dictionary pruning (no matter which values are later searched, the local segment
        * dictionaries cannot prune them early). In the main loop, we thus execute the loop two times less.
        **/
        values.push_back(static_cast<int>(column_data_distribution.min_value));
        values.push_back(static_cast<int>(column_data_distribution.max_value));
        for (auto row_offset = size_t{0}; row_offset < chunk_size - 2; ++row_offset) {
          // bounds check
          if (chunk_index * chunk_size + (row_offset + 1) > num_rows - 2) {
            break;
          }
          values.push_back(generate_value_by_distribution_type());
        }

        segments[column_index] = 
            std::make_shared<ValueSegment<ColumnDataType>>(create_typed_segment_values<ColumnDataType>(values));
      });
      // add full chunk to table
      if (column_index == num_columns - 1) {
        if (use_mvcc == UseMvcc::Yes) {
          const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
          table->append_chunk(segments, mvcc_data);
        } else {
          table->append_chunk(segments);
        }

        if (segment_encoding_specs) {
          ChunkEncoder::encode_chunk(table->get_chunk(ChunkID{table->chunk_count() - 1}), table->column_data_types(), *segment_encoding_specs, true);
        }
      }
    }
  }

  return table;
}
}  // namespace opossum
