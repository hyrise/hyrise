#include <fstream>

#include <boost/asio/ip/host_name.hpp>

#include "cli_config_parser.hpp"
#include "configuration/calibration_configuration.hpp"
#include "cost_model_calibration.hpp"
#include "calibration_helper.hpp"

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


  // Store the config file with a timestamp
  auto now_iso_date = opossum::get_time_as_iso_string(std::chrono::system_clock::now());
  auto host_name = boost::asio::ip::host_name();
  std::ifstream src(argv[1], std::ios::binary);
  std::ofstream dst(std::string(argv[1]) + "_" + host_name + "_" + now_iso_date, std::ios::binary);

  dst << src.rdbuf();
  dst.close();

  opossum::CalibrationConfiguration calibration_config = json_config;

  if (argc == 3) {
    calibration_config.output_path = argv[2];
  }

  std::cout << calibration_config.output_path << std::endl;
  auto cost_model_calibration = opossum::CostModelCalibration(calibration_config);
  cost_model_calibration.run();
  //  cost_model_calibration.run_tpch6_costing();
}
