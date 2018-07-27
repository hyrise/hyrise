#include <boost/algorithm/string.hpp>
#include <fstream>

#include "benchmark_utils.hpp"
#include "cost_model_calibration.hpp"

/**
 * This class runs Calibration Queries for the Cost Model.
 * Measured values are exported as JSON.
 */
int main(int argc, char* argv[]) {
  // TODO: Input should contain JSON with TableDefinitions
  /**
   * Relevant inputs in config
   *
   * - input tables with table spec
   * - path for output json
   * - number of iterations for calibration
   */

  if (!opossum::CLIConfigParser::cli_has_json_config(argc, argv)) {
    std::cout << "Missing Calibration Configuration" << std::endl;
    return 0;
  }

  nlohmann::json json_config;
  std::ifstream json_file{argv[1]};
  json_file >> json_config;

  opossum::CostModelCalibration(json_config).calibrate();
}