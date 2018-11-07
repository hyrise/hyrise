#include <boost/algorithm/string.hpp>
#include <fstream>

#include "benchmark_utils.hpp"
#include "configuration/calibration_configuration.hpp"
#include "cost_model_calibration.hpp"

/**
 * This class runs Calibration Queries for the Cost Model.
 * Measured values are exported as JSON.
 */
int main(int argc, char* argv[]) {
  /**
   * Relevant inputs in config
   *
   * - input tables with table spec
   * - path for output json
   * - number of iterations for calibration
   *
   * TODO: Proper documentation of Calibration Configuration JSON
   */

  if (!opossum::CLIConfigParser::cli_has_json_config(argc, argv)) {
    std::cout << "Missing Calibration Configuration" << std::endl;
    return 0;
  }

  nlohmann::json json_config;
  std::ifstream json_file{argv[1]};
  json_file >> json_config;
  json_file.close();

  opossum::CalibrationConfiguration calibration_config = json_config;

  if (argc == 3) {
    calibration_config.output_path = argv[2];
  }

  std::cout << calibration_config.output_path << std::endl;
  const auto cost_model_calibration = opossum::CostModelCalibration(calibration_config);
  cost_model_calibration.load_tables();
  cost_model_calibration.load_tpch_tables();

  cost_model_calibration.calibrate();
  cost_model_calibration.run_tpch();
}
