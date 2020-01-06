//
// Created by Lukas Böhme on 05.01.20.
//

#include "abstract_operator_measurements.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"
#include <fstream>
#include "string"

namespace opossum {

    AbstractOperatorMeasurements::AbstractOperatorMeasurements(std::shared_ptr<const AbstractOperator> op) : _op(op) {};

    std::vector<const std::string> AbstractOperatorMeasurements::as_vector() const {
      std::vector<const std::string> values;

      values.emplace_back(get_input_rows_left());  // INPUT_TABLE_LEFT
      values.emplace_back(get_input_rows_right());  // INPUT_TABLE_RIGHT
      values.emplace_back(get_output_rows());  // OUTPUT_TABLE
      values.emplace_back(get_runtime_ns());  // RUNTIME

      return values;
    }

    std::vector<const std::string> AbstractOperatorMeasurements::get_header_as_vector(){
      std::vector<const std::string> headers;

      headers.emplace_back("INPUT_TABLE_LEFT");
      headers.emplace_back("INPUT_TABLE_RIGHT");
      headers.emplace_back("OUTPUT_TABLE");
      headers.emplace_back("RUNTIME_NS");

      return headers;
    }

    void AbstractOperatorMeasurements::export_to_csv(const std::string& path_to_csv) const {
      vector_to_csv(as_vector(), path_to_csv, ","); //TODO change delimiter
    }

    void AbstractOperatorMeasurements::vector_to_csv(std::vector<const std::string> vector, const std::string& path_to_csv, const std::string& delimiter) const {
      //Open file
      std::ofstream outfile;
      outfile.open(path_to_csv , std::ofstream::out | std::ofstream::app);

      std::cout << outfile.is_open();
      //Add all with delimiter
      int columns_with_delimiter= int(vector.size() - 1);  // Is this correct?
      for (int i = 0; i < columns_with_delimiter; ++i){
        outfile << vector[i] << delimiter;
      }

      //Add last column without delimiter
      outfile << vector[columns_with_delimiter] << "\n";
      outfile.flush();
      outfile.close();
    }

    std::string AbstractOperatorMeasurements::get_input_rows_left() const {
      return std::to_string(_op->input_table_left() != nullptr ? _op->input_table_left()->row_count() : 0);
    }

    std::string AbstractOperatorMeasurements::get_input_rows_right() const {
      return "0"; //TODO Input Table does not work?!
      //return std::to_string(_op->input_table_right() != nullptr ? _op->input_table_right()->row_count() : 0);
    }

    std::string AbstractOperatorMeasurements::get_output_rows() const {
      return std::to_string(_op->get_output() != nullptr ? _op->get_output()->row_count() : 0);
    }

    std::string AbstractOperatorMeasurements::get_runtime_ns() const {
      return std::to_string(_op->performance_data().walltime.count());
    }
}