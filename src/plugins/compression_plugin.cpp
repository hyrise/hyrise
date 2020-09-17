#include <chrono>

#include "compression_plugin.hpp"

#include "constant_mappings.hpp"
#include "operators/get_table.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"

namespace {

using namespace opossum;  // NOLINT

SegmentEncodingSpec get_segment_encoding_from_encoding_name(const std::string encoding_name) {
  auto encoding_name_cleaned = encoding_name;

  std::optional<VectorCompressionType> vector_compression = std::nullopt;
  if (encoding_name.find("SIMDBP128") != std::string::npos) {
    vector_compression = VectorCompressionType::SimdBp128;
    encoding_name_cleaned = encoding_name_cleaned.substr(0, encoding_name_cleaned.size() - 9);
  } else if (encoding_name.find("FSBA") != std::string::npos) {
    vector_compression = VectorCompressionType::FixedSizeByteAligned;
    encoding_name_cleaned = encoding_name_cleaned.substr(0, encoding_name_cleaned.size() - 4);
  }

  const auto encoding_type = encoding_type_to_string.right.at(encoding_name_cleaned);
  auto encoding_spec = SegmentEncodingSpec{encoding_type};
  if (vector_compression) {
    encoding_spec.vector_compression_type = *vector_compression;
  }
  return encoding_spec;
}

size_t get_all_segments_memory_usage() {
  auto result = size_t{0};
  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    const auto chunk_count = table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      if (!table->get_chunk(chunk_id)) {
        continue;
      }

      const auto& chunk = table->get_chunk(chunk_id);
      const auto column_count = chunk->column_count();
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        const auto segment = chunk->get_segment(column_id);
        result += segment->memory_usage(MemoryUsageCalculationMode::Sampled);
      }
    }
  }
  return result;
}

}  // namespace

namespace opossum {

std::string CompressionPlugin::description() const { return "CompressionPlugin"; }

void CompressionPlugin::start() {
  Hyrise::get().log_manager.add_message(description(), "Initialized!", LogLevel::Info);
  _memory_budget_setting = std::make_shared<MemoryBudgetSetting>();
  _memory_budget_setting->register_at_settings_manager();
  _loop_thread = std::make_unique<PausableLoopThread>(THREAD_INTERVAL, [&](size_t) { _optimize_compression(); });
}

int64_t CompressionPlugin::_compress_column(const std::string table_name, const std::string column_name,
                                            const std::string encoding_name, const bool column_was_accessed,
                                            const int64_t desired_memory_usage_reduction) {
  // Arbitrary values
  constexpr auto SLEEP_BETWEEN_SEGMENTS_MS = 25;
  constexpr auto SLEEP_BETWEEN_COLUMNS_MS = 50;

  if (!Hyrise::get().storage_manager.has_table(table_name)) {
    const auto message = "Table " + table_name + " not found. TPC-H data is probably not loaded.";
    Hyrise::get().log_manager.add_message(description(), message, LogLevel::Debug);
    return 0ul;
  }

  auto table = Hyrise::get().storage_manager.get_table(table_name);
  const auto column_id = table->column_id_by_name(column_name);
  const auto data_type = table->column_data_type(column_id);
  const auto chunk_count = table->chunk_count();

  const auto segment_encoding_spec = get_segment_encoding_from_encoding_name(encoding_name);

  auto memory_usage_old = int64_t{0};
  auto memory_usage_new = int64_t{0};
  auto achieved_memory_usage_reduction = int64_t{0};

  // TODO(user): check if we should increase or decrease the memory.

  auto encoded_segment_count = size_t{0};
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    if ((desired_memory_usage_reduction > 0 && achieved_memory_usage_reduction >= desired_memory_usage_reduction) ||
        (desired_memory_usage_reduction < 0 && achieved_memory_usage_reduction < desired_memory_usage_reduction)) {
      // Finish as soon as we have achieved the desired reduction in memory usage OR break if we have used up the
      // the budget for increasing the memory usage.
      break;
    }

    auto chunk = table->get_chunk(chunk_id);
    const auto abstract_segment = chunk->get_segment(column_id);

    const auto current_encoding_spec = get_segment_encoding_spec(abstract_segment);
    if (current_encoding_spec.encoding_type == segment_encoding_spec.encoding_type &&
        (!segment_encoding_spec.vector_compression_type ||
         segment_encoding_spec.vector_compression_type == current_encoding_spec.vector_compression_type)) {
      continue;
    }

    const auto previous_segment_size = abstract_segment->memory_usage(MemoryUsageCalculationMode::Sampled);
    memory_usage_old += previous_segment_size;

    const auto encoded_segment = ChunkEncoder::encode_segment(abstract_segment, data_type, segment_encoding_spec);
    if (abstract_segment == encoded_segment) {
      // No encoding took place, segment was already encoded with the requesting encoding.
      continue;
    }
    const auto new_segment_size = encoded_segment->memory_usage(MemoryUsageCalculationMode::Sampled);
    memory_usage_new += new_segment_size;

    _keep_alive_stash.emplace_back(abstract_segment);
    chunk->replace_segment(column_id, encoded_segment);
    achieved_memory_usage_reduction += previous_segment_size - new_segment_size;
    ++encoded_segment_count;
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_BETWEEN_SEGMENTS_MS));
  }

  if (memory_usage_old != memory_usage_new) {
    Assert(encoded_segment_count > 0, "Memory usage changed, but no segment encoding has been adapted.");
    const auto memory_change_in_megabytes =
        (static_cast<double>(memory_usage_old) - static_cast<double>(memory_usage_new)) / 1'000'000;
    std::stringstream stringstream;
    stringstream << "Encoded " << encoded_segment_count << " of " << chunk_count << " segments of " << table_name;
    stringstream << "." << column_name << " using " + encoding_name << ": ";
    stringstream << ((memory_change_in_megabytes > 0) ? "saved " : "added ");
    stringstream << std::fixed << std::setprecision(2) << std::abs(memory_change_in_megabytes) << " MB ";
    stringstream << (column_was_accessed ? "." : " (column has never been accessed).");
    Hyrise::get().log_manager.add_message(description(), stringstream.str(), LogLevel::Warning);

    // Sleep after segments have been encoded to lower the impact of the plugin.
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_BETWEEN_COLUMNS_MS));
  }

  return achieved_memory_usage_reduction;
}

void CompressionPlugin::_optimize_compression() {
  _keep_alive_stash.clear();
  const auto initial_system_memory_usage = get_all_segments_memory_usage();
  const auto memory_budget_mb = std::stoll(_memory_budget_setting->get());
  const auto memory_budget = memory_budget_mb * 1000 * 1000;
  const auto memory_usage_reduction =
      static_cast<int64_t>(initial_system_memory_usage) - static_cast<int64_t>(memory_budget);
  auto achieved_memory_usage_reduction = int64_t{0};

  for (const auto& column_config : _static_compression_config) {
    if (_stop_requested) {
      break;
    }

    if (!Hyrise::get().storage_manager.has_table(column_config[1])) {
      continue;
    }

    if (memory_usage_reduction > 0 && memory_usage_reduction < achieved_memory_usage_reduction) {
      // If we want to reduce and we have already reduced more than requested: break.
      break;
    } else if (memory_usage_reduction < 0 && memory_usage_reduction > achieved_memory_usage_reduction) {
      // If we can increase the budget and have achieved more than allowed: break.
      // We can overshoot the budget here.
      break;
    }

    const auto column_was_accessed = column_config[0] == "ACCESSED" ? true : false;
    if (memory_usage_reduction > 0) {
      // We need to reduce: apply greedy changes.
      achieved_memory_usage_reduction +=
          _compress_column(column_config[1], column_config[2], column_config[3], column_was_accessed,
                           memory_usage_reduction - achieved_memory_usage_reduction);
    } else if (memory_usage_reduction < 0 && column_was_accessed) {
      // We can increase: use DictFSBA for everything that is accessed.
      achieved_memory_usage_reduction +=
          _compress_column(column_config[1], column_config[2], "DictionaryFSBA", column_was_accessed,
                           memory_usage_reduction - achieved_memory_usage_reduction);
    }
  }

  std::stringstream stringstream;
  LogLevel log_level = LogLevel::Warning;
  if (static_cast<int64_t>(initial_system_memory_usage - achieved_memory_usage_reduction) > memory_budget) {
    stringstream << "Compression optimization finished: The memory budget is infeasible";
    log_level = LogLevel::Debug;
  } else {
    stringstream << "Compression optimization finished: memory budget is feasible";
  }
  stringstream << " (budget: " << (memory_budget / 1'000'000) << " MB, currrent size of table data: ";
  stringstream << (static_cast<int64_t>(initial_system_memory_usage - achieved_memory_usage_reduction) / 1'000'000);
  stringstream << " MB).";
  Hyrise::get().log_manager.add_message(description(), stringstream.str(), log_level);
}

void CompressionPlugin::stop() {
  _stop_requested = true;
  _memory_budget_setting->unregister_at_settings_manager();
}

EXPORT_PLUGIN(CompressionPlugin)

}  // namespace opossum
