#include <filesystem>
#include <fstream>
#include <iostream>

#include "benchmark_config.hpp"
#include "benchmark_table_encoder.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_meta.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "operators/print.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/load_table.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

/**
 * Measure:
 *  -  CSV      CSV-loading path
 *  -  NONE     default
 *  -         Load filtering
 *    -  DB_CUSTKEY_ONLY                 NODBGEN and custkey
 *    -  DB_CUSTKEY_AND_MKTSEGMENT       NODBGEN and custkey+mktsegment
 *  -       DBGen filtering
 *    -  CUSTKEY_ONLY       custkey AND custkey
 *    -  CUSTKEY_AND_MKTSEGMENT       custkey+mktsegment AND custkey+mktsegment
 */

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Scale factor needs to be passed as argument." << std::endl;
    std::exit(EXIT_FAILURE);
  }

  const auto scale_factor = std::strtof(argv[1], nullptr);

  const auto table_name = std::string{"customer"};

  const auto* env_column_configuration = std::getenv("COLUMN_CONFIGURATION");
  Assert(env_column_configuration, "Column configuration is required.");
  const auto column_configuration = std::string{env_column_configuration};
  Assert(column_configuration == "CSV" ||
         column_configuration == "NONE" ||
         column_configuration == "DB_Q3_COLUMNS" ||
         column_configuration == "Q3_COLUMNS", "Unexpected column_configuration.");

  const auto path = "data_integration__loading_results_q3.csv";
  const auto append = std::filesystem::exists(std::filesystem::path{path});
  auto result_file = std::fstream{};
  result_file.open(path, append ? std::ios::app : std::ios::out);
  if (!append) {
    result_file << "COLUMN_CONFIGURATION,SCALE_FACTOR,RUN_ID,RUN_CONFIG,TABLE_NAMES,STEP,RUNTIME_US" << std::endl;
  }

  /*
  const auto run_configs = std::vector<std::pair<std::string, std::shared_ptr<AbstractScheduler>>>{
    {std::string{"SINGLE-THREADED"}, std::make_shared<ImmediateExecutionScheduler>()},
    {std::string{"MULTI-THREADED"}, std::make_shared<NodeQueueScheduler>()}
  };
  */
  const auto run_configs = std::vector<std::pair<std::string, std::shared_ptr<AbstractScheduler>>>{
    {std::string{"MULTI-THREADED"}, std::make_shared<NodeQueueScheduler>()}
  };

  constexpr auto RUN_COUNT = size_t{11};

  for (auto run_id = size_t{0}; run_id < RUN_COUNT + 1 /* one warmup run */; ++run_id) {
    for (const auto& [scheduler_str, scheduler] : run_configs) {
      Hyrise::get().topology.use_default_topology();
      Hyrise::get().set_scheduler(scheduler);

      //
      //      LOADING AND MERGING OF SUBTABLES
      //

      auto begin_table_generation = std::chrono::steady_clock::now();
      auto end_table_generation = std::chrono::steady_clock::now();
      auto begin_table_loading = std::chrono::steady_clock::now();
      auto tpch_table_generator = TPCHTableGenerator(scale_factor, ClusteringConfiguration::None);
      tpch_table_generator.reset_and_initialize();
      const auto customer_row_count = tpch_table_generator.customer_row_count();
      const auto orders_row_count = tpch_table_generator.orders_row_count();

      auto customer_table = std::shared_ptr<Table>{};
      auto orders_table = std::shared_ptr<Table>{};
      auto lineitem_table = std::shared_ptr<Table>{};

      if (column_configuration != "CSV") {
        customer_table = tpch_table_generator.create_customer_table(customer_row_count, 0);
        const auto& orders_and_lineitem = tpch_table_generator.create_orders_and_lineitem_tables(orders_row_count, 0);
        orders_table = orders_and_lineitem.first;
        lineitem_table = orders_and_lineitem.second;
      } else {
        const auto initial_path = std::filesystem::current_path();

        const auto dbgen_path = std::string{"./tpch-dbgen/"};
        Assert(std::filesystem::exists(std::filesystem::path{dbgen_path}), "dbgen directory (./tpch-dbgen) not found.");

        std::filesystem::current_path(dbgen_path);
        Assert(std::filesystem::exists(std::filesystem::path{"dbgen"}), "Cannot find dbgen in given directory.");
        begin_table_generation = std::chrono::steady_clock::now();
        const auto command = "./dbgen -f -q -s " + std::to_string(scale_factor);
        const auto result = std::system(command.c_str());
        end_table_generation = std::chrono::steady_clock::now();
        Assert(result == 0, "dbgen call ('" + command + "') failed with return code " + std::to_string(result) + ".");
        std::filesystem::current_path(initial_path);

        customer_table = tpch_table_generator.create_customer_table(0, 0);
        const auto& orders_and_lineitem = tpch_table_generator.create_orders_and_lineitem_tables(0, 0);

        orders_table = orders_and_lineitem.first;
        lineitem_table = orders_and_lineitem.second;

        begin_table_loading = std::chrono::steady_clock::now();
        for (auto& [table, file_name] : std::vector<std::tuple<std::shared_ptr<Table>, std::string>>{
                                          {customer_table, dbgen_path + "customer.tbl"},
                                          {orders_table, dbgen_path + "orders.tbl"},
                                          {lineitem_table, dbgen_path + "lineitem.tbl"}}) {
          auto infile = std::ifstream{file_name};
          Assert(infile.is_open(), "load_table: Could not find file " + file_name);

          load_table(table, infile);
        }
      }

      const auto end_table_loading = std::chrono::steady_clock::now();

      Assert(customer_table->row_count() == customer_row_count, "Expected and actual row count differ for customer table.");
      Assert(orders_table->row_count() == orders_row_count, "Expected and actual row count differ for orders table.");
      if (run_id == 0 && scheduler_str == run_configs[0].first) {
        std::cout << "Running " << run_configs.size() * RUN_COUNT << " experiments for configuration " << env_column_configuration << ":\n";
        std::cout << "\t'customer' table has " << customer_table->row_count() << " rows in ";
        std::cout << customer_table->chunk_count() << " chunks: " << std::flush;
      } else {
        std::cout << "." << std::flush;
      }

      //
      //      ENCODING
      //

      const auto begin_finalization_and_encoding = std::chrono::steady_clock::now();
      auto encoding_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
      encoding_jobs.reserve(3);
      for (const auto& [table_name, table] : std::vector<std::pair<std::string, std::shared_ptr<Table>>>
                                                        {{std::string{"customer"}, customer_table},
                                                         {std::string{"orders"}, orders_table},
                                                         {std::string{"lineitem"}, lineitem_table}}) {
        encoding_jobs.emplace_back(std::make_shared<JobTask>([&, table_name=table_name, table=table]() {
          const auto chunk_count = table->chunk_count();
          for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
            const auto& chunk = table->get_chunk(chunk_id);
            if (chunk->is_mutable()) {
              chunk->finalize();
            }
          }

          const auto benchmark_config = BenchmarkConfig::get_default_config();
          BenchmarkTableEncoder::encode(table_name, table, benchmark_config.encoding_config);
        }));
      }
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(encoding_jobs);
      const auto end_finalization_and_encoding = std::chrono::steady_clock::now();



      //
      //      STATISTICS
      //
      auto& storage_manager = Hyrise::get().storage_manager;
      for (const auto& table_name : {std::string{"customer"}, std::string{"orders"}, std::string{"lineitem"}}) {
        if (storage_manager.has_table(table_name)) {
          storage_manager.drop_table(table_name);
        }
      }

      const auto begin_table_adding = std::chrono::steady_clock::now();
      auto sm_adding_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
      sm_adding_jobs.reserve(3);
      for (const auto& [table_name, table] : std::vector<std::pair<std::string, std::shared_ptr<Table>>>
                                                        {{std::string{"customer"}, customer_table},
                                                         {std::string{"orders"}, orders_table},
                                                         {std::string{"lineitem"}, lineitem_table}}) {
        sm_adding_jobs.emplace_back(std::make_shared<JobTask>([&storage_manager, table=table, table_name=table_name]() {
          storage_manager.add_table(table_name, table);
        }));
      }
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(sm_adding_jobs);
      const auto end_table_adding = std::chrono::steady_clock::now();

      auto begin_query = std::chrono::steady_clock::now();
      for (auto query_run_id = size_t{0}; query_run_id < 11; ++query_run_id) {
        if (query_run_id == 1) {
          // Run 0 is warmup.
          begin_query = std::chrono::steady_clock::now();
        }

        auto q3_pipeline = SQLPipelineBuilder{std::string{"SELECT l_orderkey, SUM(l_extendedprice*(1.0-l_discount)) as revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;"}}
                                             .create_pipeline();
        const auto [q3_pipeline_status, q3_result] = q3_pipeline.get_result_table();

        Assert(q3_pipeline_status == SQLPipelineStatus::Success, "Q3 failed.");
        Assert(scale_factor < 10.0 || q3_result->row_count() == 10, "Unexpected result size.");
      }
      const auto end_query = std::chrono::steady_clock::now();

      Hyrise::get().scheduler()->finish();

      if (run_id == 0) {
        // Warm up run
        continue;
      }

      for (const auto& [step_name, runtimes] : std::vector{std::pair{std::string{"GENERATION"}, std::pair{begin_table_generation, end_table_generation}},
                                                           std::pair{std::string{"LOADING"}, std::pair{begin_table_loading, end_table_loading}},
                                                           std::pair{std::string{"ENCODING"}, std::pair{begin_finalization_and_encoding, end_finalization_and_encoding}},
                                                           std::pair{std::string{"ADDING"}, std::pair{begin_table_adding, end_table_adding}},
                                                           std::pair{std::string{"QUERY"}, std::pair{begin_query, end_query}}}) {
        const auto runtime_us = std::chrono::duration_cast<std::chrono::microseconds>(runtimes.second - runtimes.first).count();
        result_file << column_configuration << "," << scale_factor << "," << run_id << "," << scheduler_str << ",";
        result_file << "CUSTOMER_ORDERS_LINEITEM" << "," << step_name << "," << runtime_us << std::endl;
      }
    }
  }
  result_file.close();
  std::cout << std::endl;
}
