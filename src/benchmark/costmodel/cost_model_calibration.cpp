#include <iostream>
#include <fstream>
#include <json.hpp>

#include "cost_model_calibration.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/storage_manager.hpp"
#include "utils/format_duration.hpp"
#include "utils/load_table.hpp"


namespace opossum {

CostModelCalibration::CostModelCalibration(const nlohmann::json& configuration): _configuration(configuration) {
  const auto tableSpecifications = configuration["table_specifications"];

  for (const auto& tableSpecification : tableSpecifications) {
    auto table = load_table(tableSpecification["table_path"], 1000);
    StorageManager::get().add_table(tableSpecification["table_name"], table);
  }
}

void CostModelCalibration::calibrate() {
  size_t number_of_iterations = _configuration["calibration_runs"];
  auto queries = _generateQueries(_configuration["table_specifications"]);

  for (size_t i = 0; i < number_of_iterations; i++) {
    for (const auto& query : queries) {
      auto pipeline_builder = SQLPipelineBuilder{query};
      pipeline_builder.dont_cleanup_temporaries();
      auto pipeline = pipeline_builder.create_pipeline();

      // Execute the query, we don't care about the results
      pipeline.get_result_table();

      auto query_plans = pipeline.get_query_plans();
      for (const auto & query_plan : query_plans) {
        for (const auto& root : query_plan->tree_roots()) {
          _traverse(root);
        }
      }
    }
    std::cout << "Finished iteration " << i << std::endl;
  }

  // TODO: make output path configurable
  auto outputPath = _configuration["output_path"];
  std::ofstream myfile;
  myfile.open (outputPath);
  myfile << std::setw(2) << _operators << std::endl;
  myfile.close();
//  std::cout << std::setw(2) << _operators << std::endl;
}

void CostModelCalibration::_traverse(const std::shared_ptr<const AbstractOperator> & op) {
  _printOperator(op);

  if (op->input_left() != nullptr) {
    _traverse(op->input_left());
  }

  if (op->input_right() != nullptr) {
    _traverse(op->input_right());
  }
}

void CostModelCalibration::_printOperator(const std::shared_ptr<const AbstractOperator> & op) {
  auto description = op->name();
  auto time = op->base_performance_data().walltime;
  auto execution_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(time).count();

  if (const auto& output = op->get_output()) {
    // Inputs
    auto left_input_row_count = (op->input_left()) ? op->input_left()->get_output()->row_count() : 0;
    auto right_input_row_count = (op->input_right()) ? op->input_right()->get_output()->row_count() : 0;
    auto left_input_chunk_count = (op->input_left()) ? op->input_left()->get_output()->chunk_count() : 0;
    auto right_input_chunk_count = (op->input_right()) ? op->input_right()->get_output()->chunk_count() : 0;
    auto left_input_memory_usage = (op->input_left()) ? op->input_left()->get_output()->estimate_memory_usage() : 0;
    auto right_input_memory_usage = (op->input_right()) ? op->input_right()->get_output()->estimate_memory_usage() : 0;

    // Output
    auto output_row_count = output->row_count();
    // Calculate cross-join cardinality. Use 1 for cases, in which one side is empty to avoid divisions by zero
    auto total_input_row_count = ((left_input_row_count != 0) ? left_input_row_count : 1) * ((right_input_row_count != 0) ? right_input_row_count : 1);
    auto output_selectivity = output_row_count / total_input_row_count;
    auto output_chunk_count = output->chunk_count();
    auto output_memory_usage = output->estimate_memory_usage();

    nlohmann::json operator_result{
            {"operator_type", description},
            {"execution_time_ns", execution_time_ns},
            {"output_row_count", output_row_count},
            {"output_selectivity", output_selectivity},
            {"left_input_row_count", left_input_row_count},
            {"left_input_chunk_count", left_input_chunk_count},
            {"right_input_row_count", right_input_row_count},
            {"right_input_chunk_count", right_input_chunk_count},
            // strong-typedef ChunkID is not JSON-compatible, get underlying value here
            {"output_chunk_count", output_chunk_count.t},
            {"output_memory_usage_bytes", output_memory_usage},
            {"left_input_memory_usage_bytes", left_input_memory_usage},
            {"right_input_memory_usage_bytes", right_input_memory_usage},
    };

    _operators.push_back(operator_result);
  }
}

const std::vector<std::string> CostModelCalibration::_generateQueries(const nlohmann::json& table_definitions) {

  std::vector<std::string> queries;

  for (const auto & table_definition : table_definitions) {
    std::cout << "Using table definition for table " << table_definition["table_name"] << " to generate queries" << std::endl;
  }

  return std::vector<std::string> {
    "SELECT column_a FROM SomeTable;",
//            "SELECT column_b FROM SomeTable;",
//            "SELECT column_c FROM SomeTable;",
//            "SELECT column_a, column_b, column_c FROM SomeTable;",
//            "SELECT * FROM SomeTable;",
            "SELECT column_a, column_b, column_c FROM SomeTable WHERE column_a = 753;",
            "SELECT column_a, column_b, column_c FROM SomeTable WHERE column_a = 345;",
            "SELECT column_a, column_b, column_c, column_d FROM SomeTable WHERE column_d = 4;",
            "SELECT column_a, column_b, column_c, column_d FROM SomeTable WHERE column_d = 7;",
            "SELECT column_a, column_b, column_c, column_d FROM SomeTable WHERE column_d = 9;",
            "SELECT column_a, column_b, column_c FROM SomeTable WHERE column_a < 200;",
            "SELECT column_a, column_b, column_c FROM SomeTable WHERE column_a < 600;",
            "SELECT column_a, column_b, column_c FROM SomeTable WHERE column_a < 900;",
            "SELECT column_a, column_b, column_c FROM SomeTable WHERE column_a < 900 AND column_d = 4;",
            "SELECT column_a, column_b, column_c FROM SomeTable WHERE column_a < 900 AND column_b < 'Bradley Davis';",
            "SELECT column_b FROM SomeTable WHERE column_b < 'Bradley Davis';",
            "SELECT column_a FROM SomeSecondTable WHERE column_b = 4"
//            "SELECT COUNT(*) FROM SomeTable"
  };
}


}  // namespace opossum