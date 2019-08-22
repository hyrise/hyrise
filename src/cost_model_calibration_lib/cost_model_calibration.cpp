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
  if (_configuration.run_tpch) {
    std::cout << "Now starting TPC-H" << std::endl;
    tableGenerator.load_tpch_tables(1.0f);
    _run_tpch();
  } else if (_configuration.run_tpcds) {
    std::cout << "Now starting TPC-DS" << std::endl;
    tableGenerator.load_tpcds_tables(1.0f);
    _run_tpcc();
  } else {
    tableGenerator.load_calibration_tables();
    tableGenerator.generate_calibration_tables();
    std::cout << "Starting Calibration" << std::endl;
    _calibrate();
    std::cout << "Finished Calibration" << std::endl;
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

void CostModelCalibration::_run_tpcc() {
  CostModelCalibrationQueryRunner queryRunner{_configuration};
  const auto number_of_iterations = _configuration.calibration_runs;
  _write_csv_header(_configuration.tpch_output_path);

  // const auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());

  // const auto tpcc_query_generator = std::make_unique<opossum::TPCHBenchmarkItemRunner>(config, false, 1.0f);

  const std::vector<const std::string> queries{
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_09 FROM STOCK WHERE S_I_ID = 64829 AND S_W_ID = 2",
    "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = 4 AND C_D_ID = 6 AND C_ID = 2471",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 14853 AND S_W_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_04 FROM STOCK WHERE S_I_ID = 16597 AND S_W_ID = 1",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 12353",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 62469",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 25283 AND S_W_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_04 FROM STOCK WHERE S_I_ID = 41603 AND S_W_ID = 3",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 98753",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_10 FROM STOCK WHERE S_I_ID = 82357 AND S_W_ID = 2",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 11461",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 12996",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 3749",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01 FROM STOCK WHERE S_I_ID = 65971 AND S_W_ID = 1",
    "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = 5 AND D_W_ID = 1",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 24596",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 90180 AND S_W_ID = 3",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_02 FROM STOCK WHERE S_I_ID = 80548 AND S_W_ID = 1",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 96701",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_04 FROM STOCK WHERE S_I_ID = 82627 AND S_W_ID = 3",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 33399",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 87601 AND S_W_ID = 2",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 17088 AND S_W_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_02 FROM STOCK WHERE S_I_ID = 98753 AND S_W_ID = 2",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_04 FROM STOCK WHERE S_I_ID = 92053 AND S_W_ID = 1",
    "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM \"ORDER\" WHERE O_W_ID = 1 AND O_D_ID = 3 AND O_C_ID = 2015 ORDER BY O_ID DESC LIMIT 1",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 70085 AND S_W_ID = 3",
    "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = 6 AND D_W_ID = 2",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 65669",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 88114",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 44484",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_10 FROM STOCK WHERE S_I_ID = 73400 AND S_W_ID = 4",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 14770",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 89797 AND S_W_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 16995 AND S_W_ID = 4",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 49853",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 44227 AND S_W_ID = 4",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 8900",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 65189",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_08 FROM STOCK WHERE S_I_ID = 51397 AND S_W_ID = 2",
    "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = 3 AND D_ID = 2",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 76995",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 25153",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 92677 AND S_W_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_02 FROM STOCK WHERE S_I_ID = 82169 AND S_W_ID = 3",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 14020",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_08 FROM STOCK WHERE S_I_ID = 29376 AND S_W_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 81925 AND S_W_ID = 4",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 23229",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 41669 AND S_W_ID = 3",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_02 FROM STOCK WHERE S_I_ID = 88243 AND S_W_ID = 1",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_02 FROM STOCK WHERE S_I_ID = 88699 AND S_W_ID = 3",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 43411",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 54939 AND S_W_ID = 3",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_04 FROM STOCK WHERE S_I_ID = 29277 AND S_W_ID = 1",
    "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = 3 AND C_D_ID = 4 AND C_ID = 1619",
    "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = 9 AND D_W_ID = 3",
    "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = 9 AND D_W_ID = 3",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 64197",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 17056",
    "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = 4 AND OL_D_ID = 2 AND OL_O_ID = 1083",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 93684",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_07 FROM STOCK WHERE S_I_ID = 47787 AND S_W_ID = 1",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_09 FROM STOCK WHERE S_I_ID = 57981 AND S_W_ID = 1",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 86701 AND S_W_ID = 3",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 49157",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 51842",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 74069",
    "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = 1 AND C_D_ID = 6 ORDER BY C_FIRST",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_04 FROM STOCK WHERE S_I_ID = 46337 AND S_W_ID = 1",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 52917 AND S_W_ID = 4",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 82625",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_03 FROM STOCK WHERE S_I_ID = 15869 AND S_W_ID = 1",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_03 FROM STOCK WHERE S_I_ID = 28861 AND S_W_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 2693 AND S_W_ID = 3",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_03 FROM STOCK WHERE S_I_ID = 49157 AND S_W_ID = 1",
    "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = 1 AND C_D_ID = 3 AND C_ID = 2015",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 57413",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 89603 AND S_W_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_04 FROM STOCK WHERE S_I_ID = 92356 AND S_W_ID = 3",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 90045",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01 FROM STOCK WHERE S_I_ID = 49579 AND S_W_ID = 1",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_09 FROM STOCK WHERE S_I_ID = 47363 AND S_W_ID = 3",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 3564",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 89797",
    "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 66245",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_09 FROM STOCK WHERE S_I_ID = 24255 AND S_W_ID = 1",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 7793",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 71970",
    "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = 4 AND C_D_ID = 4 ORDER BY C_FIRST",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_03 FROM STOCK WHERE S_I_ID = 47548 AND S_W_ID = 4",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 6689",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 16452",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 70321",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_08 FROM STOCK WHERE S_I_ID = 99005 AND S_W_ID = 1",
    "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = 3 AND C_D_ID = 8 ORDER BY C_FIRST",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 90372",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 41561",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 77985 AND S_W_ID = 4",
    "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = 4 AND C_D_ID = 2 AND C_ID = 2049",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 8389",
    "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = 3 AND C_D_ID = 9 ORDER BY C_FIRST",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 94273",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_10 FROM STOCK WHERE S_I_ID = 82411 AND S_W_ID = 2",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 90307",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_09 FROM STOCK WHERE S_I_ID = 37573 AND S_W_ID = 3",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_08 FROM STOCK WHERE S_I_ID = 34693 AND S_W_ID = 1",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_03 FROM STOCK WHERE S_I_ID = 57860 AND S_W_ID = 1",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 56960",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_09 FROM STOCK WHERE S_I_ID = 30885 AND S_W_ID = 3",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 65857 AND S_W_ID = 4",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 64130",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 98817",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 42919 AND S_W_ID = 4",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 25283",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 41157",
    "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = 3 AND D_W_ID = 4",
    "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = 3 AND D_W_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_08 FROM STOCK WHERE S_I_ID = 94433 AND S_W_ID = 4",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 65971",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 71797 AND S_W_ID = 3",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 66117",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_03 FROM STOCK WHERE S_I_ID = 68277 AND S_W_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_08 FROM STOCK WHERE S_I_ID = 22660 AND S_W_ID = 2",
    "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM \"ORDER\" WHERE O_W_ID = 4 AND O_D_ID = 2 AND O_C_ID = 2049 ORDER BY O_ID DESC LIMIT 1",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 94273 AND S_W_ID = 4",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 57668",
    "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = 4 AND D_W_ID = 1",
    "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = 4 AND D_W_ID = 1",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_08 FROM STOCK WHERE S_I_ID = 44485 AND S_W_ID = 4",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 60080",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 81349",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 80037 AND S_W_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 17089 AND S_W_ID = 3",
    "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = 3 AND D_ID = 7",
    "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = 3 AND D_ID = 7",
    "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = 1 AND C_D_ID = 3 AND C_ID = 1505",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 86707",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 48324",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 27268",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_09 FROM STOCK WHERE S_I_ID = 81537 AND S_W_ID = 2",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 83968 AND S_W_ID = 3",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 24255",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 96869",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 53829",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 86469 AND S_W_ID = 2",
    "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK WHERE OL_W_ID = 2 AND OL_D_ID = 9 AND OL_O_ID < 3151 AND OL_O_ID >= 3131 AND S_W_ID = 2 AND S_I_ID = OL_I_ID AND S_QUANTITY < 17",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 65477 AND S_W_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_09 FROM STOCK WHERE S_I_ID = 27268 AND S_W_ID = 1",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 57636",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_04 FROM STOCK WHERE S_I_ID = 88773 AND S_W_ID = 1",
    "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = 3 AND D_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 70259 AND S_W_ID = 3",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 7984",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_09 FROM STOCK WHERE S_I_ID = 65159 AND S_W_ID = 1",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_04 FROM STOCK WHERE S_I_ID = 81349 AND S_W_ID = 4",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 68151",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 41669",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 41669",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 65159",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_08 FROM STOCK WHERE S_I_ID = 63427 AND S_W_ID = 2",
    "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = 1 AND C_D_ID = 4 AND C_ID = 848",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 4547",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01 FROM STOCK WHERE S_I_ID = 58049 AND S_W_ID = 1",
    "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = 2",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 33001",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01 FROM STOCK WHERE S_I_ID = 87717 AND S_W_ID = 1",
    "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = 4 AND C_D_ID = 4 AND C_ID = 73",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_02 FROM STOCK WHERE S_I_ID = 24217 AND S_W_ID = 1",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 41353",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 30949",
    "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = 2 AND C_D_ID = 6 AND C_ID = 2048",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_05 FROM STOCK WHERE S_I_ID = 44484 AND S_W_ID = 3",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01 FROM STOCK WHERE S_I_ID = 82548 AND S_W_ID = 1",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_08 FROM STOCK WHERE S_I_ID = 41563 AND S_W_ID = 4",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 38335",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_06 FROM STOCK WHERE S_I_ID = 39269 AND S_W_ID = 4",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_08 FROM STOCK WHERE S_I_ID = 66084 AND S_W_ID = 2",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 6340",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 88699",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 41141",
    "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 8857",
    "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = 4 AND OL_D_ID = 4 AND OL_O_ID = 2903",
    "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_02 FROM STOCK WHERE S_I_ID = 8900 AND S_W_ID = 3"
  };


  // Run just a single iteration for TPCH
  for (size_t i = 0; i < number_of_iterations; i++) {
    for (const auto& query : queries) {
      // const auto tpch_sql = tpch_query_generator->build_query(tpch_query_id);

      // We want a warm cache.
      // Todo: could be turned on in every second run to have a warm cache only in some cases.
      queryRunner.calibrate_query_from_sql(query);
      const auto examples = queryRunner.calibrate_query_from_sql(query);
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
