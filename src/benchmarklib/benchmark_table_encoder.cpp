#include "benchmark_table_encoder.hpp"

#include <atomic>
#include <thread>

#include "constant_mappings.hpp"
#include "encoding_config.hpp"
#include "resolve_type.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "storage/abstract_encoded_segment.hpp"
#include "storage/base_value_segment.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace {

using namespace opossum;  // NOLINT

ChunkEncodingSpec get_chunk_encoding_spec(const Chunk& chunk) {
  auto chunk_encoding_spec = ChunkEncodingSpec{chunk.column_count()};

  for (auto column_id = ColumnID{0}; column_id < chunk.column_count(); ++column_id) {
    const auto& abstract_segment = chunk.get_segment(column_id);
    chunk_encoding_spec[column_id] = get_segment_encoding_spec(abstract_segment);
  }

  return chunk_encoding_spec;
}

bool is_chunk_encoding_spec_satisfied(const ChunkEncodingSpec& expected_chunk_encoding_spec,
                                      const ChunkEncodingSpec& actual_chunk_encoding_spec) {
  if (expected_chunk_encoding_spec.size() != actual_chunk_encoding_spec.size()) return false;

  for (auto column_id = ColumnID{0}; column_id < actual_chunk_encoding_spec.size(); ++column_id) {
    if (expected_chunk_encoding_spec[column_id].encoding_type != actual_chunk_encoding_spec[column_id].encoding_type) {
      return false;
    }

    // If an explicit VectorCompressionType is requested, check whether it is used in the Segment. Otherwise, do not
    // care about the VectorCompressionType used.
    if (expected_chunk_encoding_spec[column_id].vector_compression_type) {
      if (expected_chunk_encoding_spec[column_id].vector_compression_type !=
          actual_chunk_encoding_spec[column_id].vector_compression_type) {
        return false;
      }
    }
  }

  return true;
}

}  // namespace

namespace opossum {

bool BenchmarkTableEncoder::encode(const std::string& table_name, const std::shared_ptr<Table>& table,
                                   const EncodingConfig& encoding_config) {
  /**
   * 1. Build the ChunkEncodingSpec, i.e. the Encoding to be used
   */
  const auto& type_mapping = encoding_config.type_encoding_mapping;
  const auto& custom_mapping = encoding_config.custom_encoding_mapping;

  const auto& column_mapping_it = custom_mapping.find(table_name);
  const auto table_has_custom_encoding = column_mapping_it != custom_mapping.end();

  ChunkEncodingSpec chunk_encoding_spec;

  for (ColumnID column_id{0}; column_id < table->column_count(); ++column_id) {
    // Check if a column specific encoding was specified
    if (table_has_custom_encoding) {
      const auto& column_name = table->column_name(column_id);
      const auto& encoding_by_column_name = column_mapping_it->second;
      const auto& segment_encoding = encoding_by_column_name.find(column_name);
      if (segment_encoding != encoding_by_column_name.end()) {
        // The column type has a custom encoding
        chunk_encoding_spec.push_back(segment_encoding->second);
        continue;
      }
    }

    // Check if a type specific encoding was specified
    const auto& column_data_type = table->column_data_type(column_id);
    const auto& encoding_by_data_type = type_mapping.find(column_data_type);
    if (encoding_by_data_type != type_mapping.end()) {
      // The column type has a specific encoding
      chunk_encoding_spec.push_back(encoding_by_data_type->second);
      continue;
    }

    // No column-specific or type-specific encoding was specified.
    // Use default if it is compatible with the column type or leave column Unencoded if it is not.
    if (encoding_supports_data_type(encoding_config.default_encoding_spec.encoding_type, column_data_type)) {
      chunk_encoding_spec.push_back(encoding_config.default_encoding_spec);
    } else {
      std::cout << " - Column '" << table_name << "." << table->column_name(column_id) << "' of type ";
      std::cout << column_data_type << " cannot be encoded as ";
      std::cout << encoding_config.default_encoding_spec.encoding_type << " and is ";
      std::cout << "left Unencoded." << std::endl;
      chunk_encoding_spec.emplace_back(EncodingType::Unencoded);
    }
  }

  /**
   * 2. Actually encode chunks
   */
  auto encoding_performed = std::atomic<bool>{false};
  const auto column_data_types = table->column_data_types();

  // Encode chunks in parallel, using `hardware_concurrency + 1` workers
  // Not using JobTasks here because we want parallelism even if the scheduler is disabled.
  auto next_chunk = std::atomic_uint{0};
  const auto thread_count = std::min(static_cast<uint>(table->chunk_count()), std::thread::hardware_concurrency() + 1);
  auto threads = std::vector<std::thread>{};
  threads.reserve(thread_count);

  for (auto thread_id = 0u; thread_id < thread_count; ++thread_id) {
    threads.emplace_back([&] {
      while (true) {
        auto my_chunk = next_chunk++;
        if (my_chunk >= table->chunk_count()) return;

        const auto chunk = table->get_chunk(ChunkID{my_chunk});
        Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");
        if (!is_chunk_encoding_spec_satisfied(chunk_encoding_spec, get_chunk_encoding_spec(*chunk))) {
          ChunkEncoder::encode_chunk(chunk, column_data_types, chunk_encoding_spec);
          encoding_performed = true;
        }
      }
    });
  }

  for (auto& thread : threads) thread.join();

  generate_chunk_pruning_statistics(table);

  return encoding_performed;
}

}  // namespace opossum
