#include <algorithm>
#include <iostream>
#include <fstream>
#include <json.hpp>

#include "cost_model_calibration.hpp"
#include "query/calibration_query_generator.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "utils/format_duration.hpp"
#include "utils/load_table.hpp"


namespace opossum {

CostModelCalibration::CostModelCalibration(const CalibrationConfiguration configuration): _configuration(configuration) {
  const auto table_specifications = configuration.table_specifications;

  for (const auto& table_specification : table_specifications) {
    auto table = load_table(table_specification.table_path, 1000);

    ChunkEncodingSpec chunk_spec;

    for (const auto& column_specification : table_specification.columns) {
      auto column = column_specification.second;
      chunk_spec.push_back(column.encoding);
    }

    ChunkEncoder::encode_all_chunks(table, chunk_spec);
    StorageManager::get().add_table(table_specification.table_name, table);

    std::cout << "Loaded table " << table_specification.table_name << " successfully." << std::endl;
  }
}

void CostModelCalibration::calibrate() {
  auto number_of_iterations = _configuration.calibration_runs;

  for (size_t i = 0; i < number_of_iterations; i++) {
    // Regenerate Queries for each iteration...
    auto queries = CalibrationQueryGenerator::generate_queries(_configuration.table_specifications);

    for (const auto& query : queries) {
//      std::cout << "Running " << query << std::endl;
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

  auto outputPath = _configuration.output_path;

  nlohmann::json output_json(_operators);

  // output file per operator type
  std::ofstream myfile;
  myfile.open(outputPath);
  myfile << std::setw(2) << output_json << std::endl;
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
//    auto right_input_chunk_count = (op->input_right()) ? op->input_right()->get_output()->chunk_count() : 0;
    auto left_input_memory_usage = (op->input_left()) ? op->input_left()->get_output()->estimate_memory_usage() : 0;
//    auto right_input_memory_usage = (op->input_right()) ? op->input_right()->get_output()->estimate_memory_usage() : 0;

    // Output
    auto output_row_count = output->row_count();
    // Calculate cross-join cardinality. Use 1 for cases, in which one side is empty to avoid divisions by zero in the next step
    auto total_input_row_count = std::max<uint64_t>(1, left_input_row_count) * std::max<uint64_t>(1, right_input_row_count);
    auto output_selectivity = output_row_count / double(total_input_row_count);
//    auto output_chunk_count = output->chunk_count();
//    auto output_memory_usage = output->estimate_memory_usage();

    nlohmann::json operator_result{
//            {"operator_type", description},
            {"execution_time_ns", execution_time_ns},
            {"output_row_count", output_row_count},
            {"output_selectivity", output_selectivity},
            {"left_input_row_count", left_input_row_count},
            {"left_input_chunk_count", left_input_chunk_count},
//            {"right_input_row_count", right_input_row_count},
//            {"right_input_chunk_count", right_input_chunk_count},
            // strong-typedef ChunkID is not JSON-compatible, get underlying value here
//            {"output_chunk_count", output_chunk_count.t},
//            {"output_memory_usage_bytes", output_memory_usage},
            {"left_input_memory_usage_bytes", left_input_memory_usage},
//            {"right_input_memory_usage_bytes", right_input_memory_usage},
    };

    if (description == "TableScan") {
      // Feature Encoding
//      auto left_input_table = op->input_table_left();
//      auto scan_column = left_input_table->get_chunk(ChunkID{0})->
    } else if (description == "Projection") {
      // Feature Column Counts
      auto num_input_columns = op->input_table_left()->column_count();
      auto num_output_columns = op->get_output()->column_count();

      operator_result["input_column_count"] = num_input_columns;
      operator_result["output_column_count"] = num_output_columns;
    }

    _operators[description].push_back(operator_result);
//    _operators.push_back(operator_result);
  }
}

}  // namespace opossum