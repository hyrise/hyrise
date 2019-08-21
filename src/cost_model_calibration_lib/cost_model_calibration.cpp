#include "cost_model_calibration.hpp"

#include <boost/algorithm/string/join.hpp>
#include <iostream>
#include <mutex>
#include <thread>

#include "cost_estimation/feature/cost_model_features.hpp"
#include "cost_model_calibration_query_runner.hpp"
#include "cost_model_calibration_table_generator.hpp"
#include "import_export/csv_writer.hpp"
#include "query/calibration_query_generator.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"

namespace opossum {

CostModelCalibration::CostModelCalibration(const CalibrationConfiguration configuration)
    : _configuration(configuration) {}

void CostModelCalibration::run_tpch6_costing() {
  CostModelCalibrationTableGenerator tableGenerator{_configuration, 100'000};

  _write_csv_header(_configuration.output_path);

  CostModelCalibrationQueryRunner queryRunner{_configuration};

  for (const auto& encoding : {EncodingType::Dictionary, EncodingType::Unencoded, EncodingType::RunLength}) {
    std::cout << "Now running with EncodingType::" << encoding_type_to_string.left.at(encoding) << std::endl;
    tableGenerator.load_tpch_tables(1.0f, encoding);

    const auto& queries = CalibrationQueryGenerator::generate_tpch_12();
    for (const auto& query : queries) {
      const auto examples = queryRunner.calibrate_query_from_lqp(query);
      _append_to_result_csv(_configuration.output_path, examples);
    }
  }
}

void CostModelCalibration::run() {
  CostModelCalibrationTableGenerator tableGenerator{_configuration, 100'000};
  if (!_configuration.run_tpch) {
    tableGenerator.load_calibration_tables();
    tableGenerator.generate_calibration_tables();
    std::cout << "Starting Calibration" << std::endl;
    _calibrate();
    std::cout << "Finished Calibration" << std::endl;
  } else {
    std::cout << "Now starting TPC-H" << std::endl;
    tableGenerator.load_tpch_tables(1.0f);
    _run_tpch();
  }
}

void CostModelCalibration::_run_tpch() {
  CostModelCalibrationQueryRunner queryRunner{_configuration};
  const auto number_of_iterations = _configuration.calibration_runs;
  _write_csv_header(_configuration.tpch_output_path);

  const auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());

  const auto tpch_query_generator = std::make_unique<opossum::TPCHBenchmarkItemRunner>(config, false, 1.0f);

  // Run just a single iteration for TPCH
  for (size_t i = 0; i < number_of_iterations; i++) {
    for (BenchmarkItemID tpch_query_id{0}; tpch_query_id < 22; ++tpch_query_id) {
      const auto tpch_sql = tpch_query_generator->build_query(tpch_query_id);

      // We want a warm cache.
      // Todo: could be turned on in every second run to have a warm cache only in some cases.
      queryRunner.calibrate_query_from_sql(tpch_sql);
      const auto examples = queryRunner.calibrate_query_from_sql(tpch_sql);
      //      const auto tpch_file_output_path = _configuration.tpch_output_path + "_" + std::to_string(query.first);

      _append_to_result_csv(_configuration.tpch_output_path, examples);

    }

    std::cout << "Finished iteration: " << i + 1 << std::endl;
  }
}

void CostModelCalibration::_calibrate() {
  const auto number_of_iterations = _configuration.calibration_runs;

  _write_csv_header(_configuration.output_path);

  CostModelCalibrationQueryRunner queryRunner{_configuration};

  std::vector<std::pair<std::string, size_t>> table_names;
  for (const auto& table_specification : _configuration.table_specifications) {
    table_names.emplace_back(std::make_pair(table_specification.table_name, table_specification.table_size));
  }
  if (!_configuration.calibrate_joins) {
    for (const auto& table_name : _configuration.generated_tables) {
      table_names.emplace_back(table_name, StorageManager::get().get_table(table_name)->row_count());
    }
  }

  const auto& columns = _configuration.columns;
  DebugAssert(!columns.empty(), "failed to parse ColumnSpecification");

  // Only use half of the available cores to avoid bandwidth problems. We always cap
  // at eight threads to avoid node-spanning execution for large servers with many CPUs.
  // const size_t concurrent_thread_count = std::thread::hardware_concurrency();
  const size_t threads_to_create = 1;//std::min(8ul, concurrent_thread_count / 2);

  CalibrationQueryGenerator generator(table_names, columns, _configuration);
  const auto& queries = generator.generate_queries();
  const size_t query_count = queries.size();
  const size_t queries_per_thread = static_cast<size_t>(query_count / threads_to_create);

  for (size_t iteration = size_t{0}; iteration < number_of_iterations; ++iteration) {
    std::vector<std::thread> threads;

    for (auto thread_id = size_t{0}; thread_id < threads_to_create; ++thread_id) {
      threads.push_back(std::thread([&, thread_id]() {
        std::vector<cost_model::CostModelFeatures> observations;
        const auto first_query = queries.begin() + thread_id * queries_per_thread;
        auto last_query = queries.begin() + (thread_id + 1) * queries_per_thread;
        if ((thread_id + 1) * queries_per_thread > query_count) {
          last_query = queries.end();
        }

        for (auto iter = first_query; iter != last_query; ++iter) {
          const auto query = *iter;
          const auto query_observations = queryRunner.calibrate_query_from_lqp(query);
          observations.insert(observations.end(), query_observations.begin(), query_observations.end());
          if (observations.size() > 1'000) {
            _append_to_result_csv(_configuration.output_path, observations);
            observations.clear();
          }
        }
        _append_to_result_csv(_configuration.output_path, observations);
      }));
    }

    for (auto& thread : threads) {
      thread.join();
    }
    std::cout << "Finished iteration #" << iteration + 1 << std::endl;
  }
}

void CostModelCalibration::_write_csv_header(const std::string& output_path) {
  const auto& columns = cost_model::CostModelFeatures{}.feature_names();

  std::ofstream stream;
  stream.exceptions(std::ofstream::failbit | std::ofstream::badbit);
  stream.open(output_path, std::ios::out);

  const auto header = boost::algorithm::join(columns, ",");
  stream << header << '\n';
  stream.close();
}

void CostModelCalibration::_append_to_result_csv(const std::string& output_path,
                                                 const std::vector<cost_model::CostModelFeatures>& features) {
  std::lock_guard<std::mutex> csv_guard(_csv_write_mutex);

  CsvWriter writer(output_path);
  for (const auto& feature : features) {
    const auto all_type_variants = feature.serialize();

    for (const auto& value : all_type_variants) {
      writer.write(value.second);
    }
    writer.end_line();
  }
}

}  // namespace opossum
