#include <fstream>

#include "hyrise.hpp"
#include "utils/plugin_manager.hpp"

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "tpcds/tpcds_table_generator.hpp"

using namespace opossum;  // NOLINT

namespace {

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

int main(int argc, const char* argv[]) {
  constexpr auto SCALE_FACTOR = 1.0f;

  auto start_config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  start_config->max_runs = 5;
  start_config->enable_visualization = false;
  start_config->cache_binary_tables = false;

  const bool use_prepared_statements = false;
  auto start_context = BenchmarkRunner::create_context(*start_config);

  const std::vector<BenchmarkItemID> tpch_query_ids_warmup = {BenchmarkItemID{5}};
  auto start_item_runner = std::make_unique<TPCHBenchmarkItemRunner>(start_config, use_prepared_statements, SCALE_FACTOR, tpch_query_ids_warmup);
  // auto start_item_runner = std::make_unique<TPCHBenchmarkItemRunner>(start_config, use_prepared_statements, SCALE_FACTOR);
  BenchmarkRunner(*start_config, std::move(start_item_runner), std::make_unique<TPCHTableGenerator>(SCALE_FACTOR, start_config), start_context).run();

  std::string path = "configurations/";
  for (const auto& entry : std::filesystem::directory_iterator(path)) {
    const auto conf_path = entry.path();
    const auto conf_name = conf_path.stem();
    const auto filename = conf_path.filename().string();

    if (filename.find("conf") != 0 || filename.find(".json") != std::string::npos) {
      std::cout << "Skipping " << conf_path << std::endl;
      continue;
    }

    std::cout << "Benchmarking " << conf_name << " ..." << std::endl;

    {
      // To speed up the table generation, the node scheduler is used. To not interfere with any settings for the actual
      // Hyrise process (e.g., the test runner or the calibration), the current scheduler is stored, replaced, and
      // eventually set again.
      Hyrise::get().scheduler()->wait_for_all_tasks();
      const auto previous_scheduler = Hyrise::get().scheduler();
      Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

      auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
      config->max_runs = 50;
      config->enable_visualization = false;
      config->output_file_path = conf_name.string() + ".json";
      config->cache_binary_tables = false;
      config->max_duration = std::chrono::seconds(300);

      std::vector<std::shared_ptr<AbstractTask>> jobs;
      

      auto context = BenchmarkRunner::create_context(*config);

      std::ifstream configuration_file(entry.path().string());
      const auto line_count = std::count(std::istreambuf_iterator<char>(configuration_file), std::istreambuf_iterator<char>(), '\n');
      jobs.reserve(line_count + 10);
      std::string line;
      configuration_file.seekg(0, std::ios::beg);
      std::cout << "Reencoding: " << std::flush;
      while (std::getline(configuration_file, line))
      {
        std::vector<std::string> line_values;
        std::istringstream linestream(line);
        std::string value;
        while (std::getline(linestream, value, ','))
        {
          line_values.push_back(value);
        }

        const auto table_name = line_values[0];
        const auto column_name = line_values[1];
        const auto chunk_id = ChunkID{static_cast<uint32_t>(std::stoi(line_values[2]))};
        const auto encoding_type_str = line_values[3];
        const auto vector_compression_type_str = line_values[4];

        const auto& table = Hyrise::get().storage_manager.get_table(table_name);
        if (chunk_id >= table->chunk_count()) {
          continue;
        }
        const auto& chunk = table->get_chunk(chunk_id);
        const auto& column_id = table->column_id_by_name(column_name);
        const auto& segment = chunk->get_segment(column_id);
        const auto& data_type = table->column_data_type(column_id);

        const auto encoding_type = encoding_type_to_string.right.at(encoding_type_str);

        SegmentEncodingSpec spec = {encoding_type};
        if (vector_compression_type_str != "None") {
          const auto vector_compression_type = vector_compression_type_to_string.right.at(vector_compression_type_str);
          spec.vector_compression_type = vector_compression_type;
        }

        jobs.emplace_back(std::make_shared<JobTask>([chunk, segment, data_type, spec, column_id]() {
          const auto& encoded_segment = ChunkEncoder::encode_segment(segment, data_type, spec);
          chunk->replace_segment(column_id, encoded_segment);
        }));
        jobs.back()->schedule();
      }
      configuration_file.close();

      Hyrise::get().scheduler()->wait_for_tasks(jobs);
      Hyrise::get().scheduler()->wait_for_all_tasks();
      Hyrise::get().set_scheduler(previous_scheduler);  // set scheduler back to previous one.

      std::cout << " done." << std::endl;
      std::cout << "Starting benchmark." << std::endl;

      auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, use_prepared_statements, SCALE_FACTOR);//, tpch_query_ids_benchmark);
      BenchmarkRunner(*config, std::move(item_runner), nullptr, context).run();

      std::ofstream size_result;
      size_result.open(conf_name.string() + ".size");
      size_result << get_all_segments_memory_usage();
      size_result.close();
    }
  }
  return 0;

}
