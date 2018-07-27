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
  for (size_t i = 0; i < number_of_iterations; i++) {
    for (const auto& query : _queries) {
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
  std::ostringstream out;
  auto time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(time).count();

  if (const auto& output = op->get_output()) {
    auto row_count = output->row_count();
    auto chunk_count = output->chunk_count();
    auto mem_usage = output->estimate_memory_usage();

    auto left_input_row_count = (op->input_left()) ? op->input_left()->get_output()->row_count() : 0;
    auto right_input_row_count = (op->input_right()) ? op->input_right()->get_output()->row_count() : 0;

    auto left_input_mem_usage = (op->input_left()) ? op->input_left()->get_output()->estimate_memory_usage() : 0;
    auto right_input_mem_usage = (op->input_right()) ? op->input_right()->get_output()->estimate_memory_usage() : 0;

    nlohmann::json operator_result{
            {"operator", description},
            {"time_ns", time_ns},
            {"output_row_count", row_count},
            {"left_input_row_count", left_input_row_count},
            {"right_input_row_count", right_input_row_count},
            // strong-typedef ChunkID is not JSON-compatible, get underlying value here
            {"chunk_count", chunk_count.t},
            {"output_mem_usage_bytes", mem_usage},
            {"left_input_mem_usage_bytes", left_input_mem_usage},
            {"right_input_mem_usage_bytes", right_input_mem_usage},
    };

    _operators.push_back(operator_result);
  }
}


}  // namespace opossum