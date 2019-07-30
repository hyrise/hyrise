#include "tpcc/tpcc_table_generator.hpp"

#include <algorithm>

#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "operators/print.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpcc/constants.hpp"
#include "tpcc/tpcc_benchmark_item_runner.hpp"

using namespace opossum;  // NOLINT

/**
 * This benchmark measures Hyrise's performance executing the TPC-C benchmark. As with the other TPC-* benchmarks, we
 * took some liberty in interpreting the standard. Most notably, all parts about the simulated terminals are ignored.
 * Instead, only the queries leading to the terminal output are executed. In the research world, doing so has become
 * the de facto standard.
 *
 * Other limitations (that may be removed in the future):
 *  - No primary / foreign keys are used as they are currently unsupported
 *  - Values that are "retrieved" by the terminal are just selected, but not necessarily materialized
 *  - Data is not persisted as logging is currently unsupported; this means that the durability tests are not executed
 *  - As decimals are not supported, we use floats instead
 *  - The delivery transaction is not executed in a "deferred" mode; as such, no delivery result file is written
 *  - We do not execute the isolation tests, as we consider our MVCC tests to be sufficient
 *
 * Most importantly, we do not claim to report correctly calculated tpmC.
 *
 * main() is mostly concerned with parsing the CLI options while BenchmarkRunner.run() performs the actual benchmark
 * logic.
 */

namespace {
void check_consistency(const int num_warehouses);
}

int main(int argc, char* argv[]) {
  auto cli_options = BenchmarkRunner::get_basic_cli_options("TPC-C Benchmark");

  // clang-format off
  cli_options.add_options()
    // We use -s instead of -w for consistency with the options of our other TPC-x binaries.
    ("s,scale", "Scale factor (warehouses)", cxxopts::value<int>()->default_value("1")); // NOLINT
  // clang-format on

  std::shared_ptr<BenchmarkConfig> config;
  int num_warehouses;

  if (CLIConfigParser::cli_has_json_config(argc, argv)) {
    // JSON config file was passed in
    const auto json_config = CLIConfigParser::parse_json_config_file(argv[1]);
    num_warehouses = json_config.value("scale", 1);

    config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_basic_options_json_config(json_config));
  } else {
    // Parse regular command line args
    const auto cli_parse_result = cli_options.parse(argc, argv);

    if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) return 0;

    num_warehouses = cli_parse_result["scale"].as<int>();

    config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_basic_cli_options(cli_parse_result));
  }

  auto context = BenchmarkRunner::create_context(*config);

  std::cout << "- TPC-C scale factor (number of warehouses) is " << num_warehouses << std::endl;

  // Add TPC-C-specific information
  context.emplace("scale_factor", num_warehouses);

  // Run the benchmark
  auto item_runner = std::make_unique<TPCCBenchmarkItemRunner>(config, num_warehouses);
  BenchmarkRunner(*config, std::move(item_runner), std::make_unique<TPCCTableGenerator>(num_warehouses, config),
                  context)
      .run();

  std::cout << "- Running consistency checks at the end of the benchmark" << std::endl;
  check_consistency(num_warehouses);
  std::cout << "- Consistency checks passed" << std::endl;
}

namespace {
template<typename T>
bool floats_near(T a, T b) {
  // Tolerate 0.1% discrepancy due to float variations
  return std::max(a, b) / std::min(a, b) <= 1.001;
}

void check_consistency(const int num_warehouses) {
  using namespace opossum;

  // new_order_counts[5-1][2-1] will hold the number of new_orders for W_ID 5, D_ID 2
  // TODO replace 10 with NUM_DISTRICTS etc.
  std::vector<std::vector<int64_t>> new_order_counts(num_warehouses, std::vector<int64_t>(10));

  {
    // Consistency condition 1 (see 3.3.2.1)
    auto pipeline = SQLPipelineBuilder{"SELECT W_ID, MAX(W_YTD), SUM(D_YTD) FROM WAREHOUSE, DISTRICT WHERE W_ID = D_W_ID GROUP BY W_ID"}
                        .create_pipeline();
    const auto [pipeline_status, table] = pipeline.get_result_table();
    Assert(table && table->row_count() == static_cast<size_t>(num_warehouses), "Lost a warehouse");
    for (auto row_id = size_t{0}; row_id < table->row_count(); ++row_id) {
      const auto w_ytd = double{table->get_value<float>(ColumnID{1}, row_id)};
      const auto d_ytd = table->get_value<double>(ColumnID{2}, row_id);

      Assert(floats_near(w_ytd, d_ytd), "Mismatching YTD for WAREHOUSE and HISTORY");
    }
  }

  {
    // Consistency condition 2
    for (auto w_id = 1; w_id <= num_warehouses; ++w_id) {
      auto district_pipeline = SQLPipelineBuilder{std::string{"SELECT D_NEXT_O_ID - 1 FROM DISTRICT WHERE D_W_ID = "} + std::to_string(w_id) + " ORDER BY D_ID"}
                          .create_pipeline();
      const auto [district_pipeline_status, district_table] = district_pipeline.get_result_table();
      Assert(district_table && district_table->row_count() == 10, "Lost a district");
      for (auto d_id = 1; d_id <= 10; ++d_id) {
        const auto max_o_id = district_table->get_value<int>(ColumnID{0}, d_id - 1);

        auto order_pipeline = SQLPipelineBuilder{std::string{"SELECT MAX(O_ID) FROM \"ORDER\" WHERE O_W_ID = "} + std::to_string(w_id) + " AND O_D_ID = " + std::to_string(d_id)}
                            .create_pipeline();
        const auto [order_pipeline_status, order_table] = order_pipeline.get_result_table();
        Assert(order_table && order_table->row_count() == 1, "Did not find MAX(O_ID)");
        Assert(order_table->get_value<int>(ColumnID{0}, 0) == max_o_id, "Mismatching order IDs");

        auto new_order_pipeline = SQLPipelineBuilder{std::string{"SELECT COUNT(*), MAX(NO_O_ID) FROM NEW_ORDER WHERE NO_W_ID = "} + std::to_string(w_id) + " AND NO_D_ID = " + std::to_string(d_id)}
                            .create_pipeline();
        const auto [new_order_pipeline_status, new_order_table] = new_order_pipeline.get_result_table();
        Assert(order_table && order_table->row_count() == 1, "Could not retrieve new_orders");
        const auto new_order_count = new_order_table->get_value<int64_t>(ColumnID{0}, 0);
        new_order_counts[w_id - 1][d_id - 1] = new_order_count;
        if (new_order_count > 0) {
          Assert(new_order_table->get_value<int>(ColumnID{1}, 0) == max_o_id, "Mismatching order IDs");
        }
      }
    }
  }

  {
    // Consistency condition 3
    auto new_order_pipeline = SQLPipelineBuilder{"SELECT NO_W_ID, NO_D_ID, MIN(NO_O_ID), MAX(NO_O_ID) FROM NEW_ORDER GROUP BY NO_W_ID, NO_D_ID"}
                        .create_pipeline();
    const auto [new_order_pipeline_status, new_order_table] = new_order_pipeline.get_result_table();
    Assert(new_order_table, "Could not retrieve new_orders");
    for (auto row_id = size_t{0}; row_id < new_order_table->row_count(); ++row_id) {
      // TODO replace get_value<int> with int32_t
      const auto w_id = new_order_table->get_value<int>(ColumnID{0}, row_id);
      const auto d_id = new_order_table->get_value<int>(ColumnID{1}, row_id);
      const auto min_o_id = new_order_table->get_value<int>(ColumnID{2}, row_id);
      const auto max_o_id = new_order_table->get_value<int>(ColumnID{3}, row_id);
      Assert(max_o_id - min_o_id + 1 == new_order_counts[w_id - 1][d_id - 1], "Mismatching order IDs");
    }
  }

  {
    // Consistency condition 4
    auto order_pipeline = SQLPipelineBuilder{"SELECT O_W_ID, O_D_ID, SUM(O_OL_CNT) FROM \"ORDER\" GROUP BY O_W_ID, O_D_ID ORDER BY O_W_ID, O_D_ID"}
                        .create_pipeline();
    const auto [order_pipeline_status, order_table] = order_pipeline.get_result_table();
    Assert(order_table && order_table->row_count() == static_cast<size_t>(num_warehouses * 10), "Did not find SUM(O_OL_CNT) for all districts");

    auto order_line_pipeline = SQLPipelineBuilder{"SELECT OL_W_ID, OL_D_ID, COUNT(*) FROM ORDER_LINE GROUP BY OL_W_ID, OL_D_ID ORDER BY OL_W_ID, OL_D_ID"}
                        .create_pipeline();
    const auto [order_line_pipeline_status, order_line_table] = order_line_pipeline.get_result_table();
    Assert(order_line_table && order_line_table->row_count() == static_cast<size_t>(num_warehouses * 10), "Did not find COUNT(*) FROM ORDER_LINE for all districts");

    for (auto row_id = size_t{0}; row_id < order_line_table->row_count(); ++row_id) {
      Assert(order_table->get_value<int64_t>(ColumnID{2}, row_id) == order_line_table->get_value<int64_t>(ColumnID{2}, row_id), "Mismatching order_line count");
    }
  }

  {
    // Consistency condition 5
    auto pipeline = SQLPipelineBuilder{"SELECT * FROM \"ORDER\" WHERE O_CARRIER_ID = -1 AND NOT EXISTS (SELECT NO_W_ID FROM NEW_ORDER WHERE O_W_ID = NO_W_ID AND O_D_ID = NO_D_ID AND NO_O_ID = O_ID)"}
                        .create_pipeline();
    const auto [pipeline_status, table] = pipeline.get_result_table();
    Assert(table && table->row_count() == size_t{0}, "Found fulfilled order without O_CARRIER_ID");
  }

  {
    // Consistency condition 6
    auto pipeline = SQLPipelineBuilder{"SELECT O_W_ID, O_D_ID, O_ID, MAX(O_OL_CNT), COUNT(*) FROM \"ORDER\" LEFT JOIN ORDER_LINE ON O_W_ID = OL_W_ID AND O_D_ID = OL_D_ID AND O_ID = OL_O_ID GROUP BY O_W_ID, O_D_ID, O_ID"}
                        .create_pipeline();
    const auto [pipeline_status, table] = pipeline.get_result_table();
    Assert(table && table->row_count() > size_t{0}, "Failed to retrieve order / order lines");
    for (auto row_id = size_t{0}; row_id < table->row_count(); ++row_id) {
      Assert(table->get_value<int32_t>(ColumnID{3}, row_id) == table->get_value<int64_t>(ColumnID{4}, row_id), "Mismatching number of order lines");
    }
  }

  {
    // Consistency condition 7
    auto pipeline = SQLPipelineBuilder{"SELECT * FROM ORDER_LINE LEFT JOIN \"ORDER\" ON OL_W_ID = O_W_ID AND OL_D_ID = O_D_ID AND OL_O_ID = O_ID WHERE OL_DELIVERY_D = -1 AND O_CARRIER_ID <> -1"}
                        .create_pipeline();
    const auto [pipeline_status, table] = pipeline.get_result_table();
    Assert(table && table->row_count() == size_t{0}, "Found order line without OL_DELIVERY_D even though the order was delivered");
  }

  {
    // Consistency condition 8
    auto pipeline = SQLPipelineBuilder{"SELECT W_ID, MAX(W_YTD), SUM(H_AMOUNT) FROM WAREHOUSE, \"HISTORY\" WHERE W_ID = H_W_ID GROUP BY W_ID"}
                        .create_pipeline();
    const auto [pipeline_status, table] = pipeline.get_result_table();
    Assert(table && table->row_count() == static_cast<size_t>(num_warehouses), "Lost a warehouse");
    for (auto row_id = size_t{0}; row_id < table->row_count(); ++row_id) {
      const auto w_ytd = double{table->get_value<float>(ColumnID{1}, row_id)};
      const auto h_amount = table->get_value<double>(ColumnID{2}, row_id);

      Assert(floats_near(w_ytd, h_amount), "Mismatching YTD for WAREHOUSE and HISTORY");
    }
  }

  {
    // Consistency condition 9
    auto pipeline = SQLPipelineBuilder{"SELECT D_W_ID, D_ID, MAX(D_YTD), SUM(H_AMOUNT) FROM DISTRICT, \"HISTORY\" WHERE D_W_ID = H_W_ID AND D_ID = H_D_ID GROUP BY D_W_ID, D_ID"}
                        .create_pipeline();
    const auto [pipeline_status, table] = pipeline.get_result_table();
    Assert(table && table->row_count() == static_cast<size_t>(num_warehouses * 10), "Lost a district");
    for (auto row_id = size_t{0}; row_id < table->row_count(); ++row_id) {
      const auto d_ytd = double{table->get_value<float>(ColumnID{2}, row_id)};
      const auto h_amount = table->get_value<double>(ColumnID{3}, row_id);

      Assert(floats_near(d_ytd, h_amount), "Mismatching YTD for DISTRICT and HISTORY");
    }
  }

  {
    // Consistency condition 10
    auto pipeline = SQLPipelineBuilder{"SELECT C_W_ID, C_D_ID, C_ID, MAX(C_BALANCE), (CASE WHEN SUM_OL_AMOUNT IS NULL THEN 0 ELSE SUM_OL_AMOUNT END) AS SUM_OL_AMOUNT_NONNULL, SUM_H_AMOUNT FROM CUSTOMER LEFT JOIN (SELECT O_W_ID, O_D_ID, O_C_ID, SUM(OL_AMOUNT) FROM \"ORDER\", ORDER_LINE WHERE OL_W_ID = O_W_ID AND OL_D_ID = O_D_ID AND OL_O_ID = O_ID AND OL_DELIVERY_D <> -1 GROUP BY O_W_ID, O_D_ID, O_C_ID) AS sub1(O_W_ID, O_D_ID, O_C_ID, SUM_OL_AMOUNT) ON O_W_ID = C_W_ID AND O_D_ID = C_D_ID AND O_C_ID = C_ID LEFT JOIN (SELECT H_W_ID, H_D_ID, H_C_ID, SUM(H_AMOUNT) FROM \"HISTORY\" GROUP BY H_W_ID, H_D_ID, H_C_ID) AS sub2(H_W_ID, H_D_ID, H_C_ID, SUM_H_AMOUNT) ON H_W_ID = C_W_ID AND H_D_ID = C_D_ID AND H_C_ID = C_ID GROUP BY C_W_ID, C_D_ID, C_ID, SUM_OL_AMOUNT_NONNULL, SUM_H_AMOUNT"}
                        .create_pipeline();
    const auto [pipeline_status, table] = pipeline.get_result_table();
    Assert(table && table->row_count() == static_cast<size_t>(num_warehouses * 10 * NUM_CUSTOMERS_PER_DISTRICT), "Lost a customer");
    for (auto row_id = size_t{0}; row_id < table->row_count(); ++row_id) {
      const auto c_balance = double{table->get_value<float>(ColumnID{3}, row_id)};
      const auto sum_ol_amount = table->get_value<double>(ColumnID{4}, row_id);
      const auto sum_h_amount = table->get_value<double>(ColumnID{5}, row_id);

      Assert(floats_near(sum_ol_amount - sum_h_amount, c_balance), "Mismatching amounts for customer");
    }
  }

  {
    // Consistency condition 11
    auto pipeline = SQLPipelineBuilder{"SELECT D_W_ID, D_ID, (SELECT COUNT(*) FROM \"ORDER\" WHERE O_W_ID = D_W_ID AND O_D_ID = D_ID), (SELECT COUNT(*) FROM NEW_ORDER WHERE NO_W_ID = D_W_ID AND NO_D_ID = D_ID) FROM DISTRICT"}
                        .create_pipeline();
    const auto [pipeline_status, table] = pipeline.get_result_table();
    Assert(table && table->row_count() == static_cast<size_t>(num_warehouses * 10), "Lost a district");
    for (auto row_id = size_t{0}; row_id < table->row_count(); ++row_id) {
      const auto o_count = table->get_value<int64_t>(ColumnID{2}, row_id);
      const auto no_count = table->get_value<int64_t>(ColumnID{3}, row_id);

      Assert(o_count - no_count == NUM_ORDERS_PER_DISTRICT - NUM_NEW_ORDERS_PER_DISTRICT, "Mismatching YTD for DISTRICT and HISTORY");
    }
  }

  {
    // Consistency condition 12
    auto pipeline = SQLPipelineBuilder{"SELECT C_W_ID, C_D_ID, C_ID, C_BALANCE, C_YTD_PAYMENT, SUM(OL_AMOUNT) FROM CUSTOMER LEFT JOIN \"ORDER\" ON O_W_ID = C_W_ID AND O_D_ID = C_D_ID AND O_C_ID = C_ID LEFT JOIN ORDER_LINE ON OL_W_ID = O_W_ID AND OL_D_ID = O_D_ID AND OL_O_ID = O_ID WHERE OL_DELIVERY_D <> -1 GROUP BY C_W_ID, C_D_ID, C_ID, C_BALANCE, C_YTD_PAYMENT"}
                        .create_pipeline();
    const auto [pipeline_status, table] = pipeline.get_result_table();
    Assert(table && table->row_count() == static_cast<size_t>(num_warehouses * 10 * NUM_CUSTOMERS_PER_DISTRICT), "Lost a customer");
    for (auto row_id = size_t{0}; row_id < table->row_count(); ++row_id) {
      const auto c_balance = double{table->get_value<float>(ColumnID{3}, row_id)};
      const auto c_ytd_payment = double{table->get_value<float>(ColumnID{4}, row_id)};
      const auto sum_ol_amount = table->get_value<double>(ColumnID{5}, row_id);

      Assert(floats_near(c_balance + c_ytd_payment, sum_ol_amount), "Mismatching YTD for CUSTOMER and ORDER");
    }
  }
}

}

