#include "cost_model_calibration.hpp"

#include <boost/algorithm/string/join.hpp>
#include <fstream>
#include <iostream>

#include "cost_model/feature/cost_model_features.hpp"
#include "cost_model_calibration_query_runner.hpp"
#include "cost_model_calibration_table_generator.hpp"
#include "import_export/csv_writer.hpp"
#include "query/calibration_query_generator.hpp"
#include "tpch/tpch_queries.hpp"

namespace opossum {

CostModelCalibration::CostModelCalibration(const CalibrationConfiguration configuration)
    : _configuration(configuration) {}

void CostModelCalibration::run_tpch6_costing() const {
  CostModelCalibrationTableGenerator tableGenerator{_configuration, 100000};

  _write_csv_header(_configuration.output_path);

  CostModelCalibrationQueryRunner queryRunner{_configuration};

  for (const auto& encoding : {EncodingType::Dictionary, EncodingType::Unencoded, EncodingType::RunLength}) {
    std::cout << "Now running with EncodingType::" << encoding_type_to_string.left.at(encoding);
    tableGenerator.load_tpch_tables(0.01f, encoding);

    const auto& queries = CalibrationQueryGenerator::generate_tpch_6();
    for (const auto& query : queries) {
      const auto examples = queryRunner.calibrate_query_from_lqp(query);
      _append_to_result_csv(_configuration.output_path, examples);
    }
  }
}

void CostModelCalibration::run() const {
  CostModelCalibrationTableGenerator tableGenerator{_configuration, 100000};
  tableGenerator.load_calibration_tables();
  //    tableGenerator.load_tpch_tables(1.0f);
  _calibrate();

  std::cout << "Finished Calibration, now starting TPC-H" << std::endl;
  //  _run_tpch();
}

void CostModelCalibration::_run_tpch() const {
  CostModelCalibrationQueryRunner queryRunner{_configuration};
  // Run just a single iteration for TPCH
  for (size_t i = 0; i < 1; i++) {
    for (const auto& query : opossum::tpch_queries) {
      std::cout << "Running TPCH " << std::to_string(query.first) << std::endl;

      const auto examples = queryRunner.calibrate_query_from_sql(query.second);

      const auto tpch_file_output_path = _configuration.tpch_output_path + "_" + std::to_string(query.first);
      _write_csv_header(tpch_file_output_path);
      _append_to_result_csv(tpch_file_output_path, examples);
    }
  }
}

void CostModelCalibration::_calibrate() const {
  const auto number_of_iterations = _configuration.calibration_runs;

  _write_csv_header(_configuration.output_path);

  CostModelCalibrationQueryRunner queryRunner{_configuration};

  std::vector<std::pair<std::string, size_t>> table_names;
  for (const auto& table_specification : _configuration.table_specifications) {
    table_names.emplace_back(std::make_pair(table_specification.table_name, table_specification.table_size));
  }

  const auto& columns = _configuration.columns;
  DebugAssert(!columns.empty(), "failed to parse ColumnSpecification");

  CalibrationQueryGenerator generator(table_names, columns, _configuration);

  for (size_t i = 0; i < number_of_iterations; i++) {
    // Regenerate Queries for each iteration...

    const auto& queries = generator.generate_queries();
    for (const auto& query : queries) {
      const auto examples = queryRunner.calibrate_query_from_lqp(query);
      _append_to_result_csv(_configuration.output_path, examples);
    }

    std::cout << "Finished iteration " << i << std::endl;
  }
}

const std::vector<std::string> CostModelCalibration::_collect_csv_header_columns() const {
  //  std::vector<std::string> columns{};

  return cost_model::CalibrationFeatures{}.feature_names();
  //  const auto hardware_features = ConstantHardwareFeatures{}.feature_names();
  //  const auto runtime_features = RuntimeHardwareFeatures{}.feature_names();
  //  const auto join_features = JoinFeatures{}.feature_names();
  //  const auto projection_features = ProjectionFeatures{}.feature_names();
  //  const auto table_scan_features = TableScanFeatures{}.feature_names();

  //  columns.insert(std::end(columns), std::begin(calibration_features), std::end(calibration_features));
  //  columns.insert(std::end(columns), std::begin(hardware_features), std::end(hardware_features));
  //  columns.insert(std::end(columns), std::begin(runtime_features), std::end(runtime_features));
  //  columns.insert(std::end(columns), std::begin(join_features), std::end(join_features));
  //  columns.insert(std::end(columns), std::begin(projection_features), std::end(projection_features));
  //  columns.insert(std::end(columns), std::begin(table_scan_features), std::end(table_scan_features));

  //  return columns;
}

void CostModelCalibration::_write_csv_header(const std::string& output_path) const {
  const auto& columns = _collect_csv_header_columns();

  std::ofstream stream;
  stream.exceptions(std::ofstream::failbit | std::ofstream::badbit);
  stream.open(output_path, std::ios::out);

  const auto header = boost::algorithm::join(columns, ",");
  stream << header << '\n';
  stream.close();
}

void CostModelCalibration::_append_to_result_csv(const std::string& output_path,
                                                 const std::vector<cost_model::CalibrationFeatures>& features) const {
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
