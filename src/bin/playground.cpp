#include <filesystem>
#include <fstream>
#include <iostream>

#include "benchmark_config.hpp"
#include "benchmark_table_encoder.hpp"
#include "hyrise.hpp"
#include "operators/print.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "tpch/tpch_table_generator.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

/**
 * Measure:
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
  Assert(column_configuration == "NONE" ||
         column_configuration == "DB_Q3_COLUMNS" ||
         column_configuration == "Q3_COLUMNS", "Unexpected column_configuration.");

  const auto path = "data_integration__loading_results_q3.csv";
  const auto append = std::filesystem::exists(std::filesystem::path{path});
  auto result_file = std::fstream{};
  result_file.open(path, append ? std::ios::app : std::ios::out);
  if (!append) {
    result_file << "COLUMN_CONFIGURATION,SCALE_FACTOR,RUN_ID,RUN_CONFIG,TABLE_NAMES,STEP,RUNTIME_US" << std::endl;
  }

  const auto run_configs = std::vector<std::pair<std::string, std::shared_ptr<AbstractScheduler>>>{
    {std::string{"SINGLE-THREADED"}, std::make_shared<ImmediateExecutionScheduler>()},
    {std::string{"MULTI-THREADED"}, std::make_shared<NodeQueueScheduler>()}
  };

  constexpr auto RUN_COUNT = size_t{11};
  const auto ROW_COUNT_PER_GENERATE_TASK = size_t{5 * Chunk::DEFAULT_SIZE};  // 327.680 rows.
  constexpr auto PARALLEL_GENERATION = false;

  for (auto run_id = size_t{0}; run_id < RUN_COUNT + 1 /* one warmup run */; ++run_id) {
    for (const auto& [scheduler_str, scheduler] : run_configs) {
      Hyrise::get().topology.use_default_topology();
      Hyrise::get().set_scheduler(scheduler);

      //
      //      LOADING AND MERGING OF SUBTABLES
      //

      const auto begin_table_creation = std::chrono::steady_clock::now();
      auto tpch_table_generator = TPCHTableGenerator(scale_factor, ClusteringConfiguration::None);
      tpch_table_generator.reset_seeds();
      const auto customer_row_count = tpch_table_generator.customer_row_count();
      const auto orders_row_count = tpch_table_generator.orders_row_count();

      auto customer_table = std::shared_ptr<Table>{};
      auto orders_table = std::shared_ptr<Table>{};
      auto lineitem_table = std::shared_ptr<Table>{};

      if constexpr (PARALLEL_GENERATION) {
        auto column_definitions = std::optional<std::vector<TableColumnDefinition>>{};
        const auto job_count = static_cast<size_t>(std::ceil(static_cast<double>(customer_row_count) / static_cast<double>(ROW_COUNT_PER_GENERATE_TASK)));
        const auto generated_chunk_count = static_cast<size_t>(std::ceil(static_cast<double>(customer_row_count) / static_cast<double>(Chunk::DEFAULT_SIZE)));
        const auto max_chunk_count_per_job = ROW_COUNT_PER_GENERATE_TASK / Chunk::DEFAULT_SIZE;
        auto generated_chunks = std::vector<std::shared_ptr<Chunk>>(generated_chunk_count);
        auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
        jobs.reserve(job_count);

        auto accumulated_rows = size_t{0};
        auto job_id = size_t{0};

        while (accumulated_rows < customer_row_count) {
          const auto remaining = customer_row_count - accumulated_rows;
          const auto rows_to_generate = std::min(ROW_COUNT_PER_GENERATE_TASK, remaining);
          const auto job_chunk_count = static_cast<size_t>(std::ceil(static_cast<double>(rows_to_generate) / static_cast<double>(Chunk::DEFAULT_SIZE)));

          const auto start_offset = job_id == 0 ? 0 : job_id * max_chunk_count_per_job;
          jobs.emplace_back(std::make_shared<JobTask>([&, rows_to_generate, accumulated_rows, job_id, job_chunk_count, start_offset]() {
            std::printf("job_id %zu started.\n", job_id);
            const auto partial_table = tpch_table_generator.create_customer_table(rows_to_generate, accumulated_rows);
            Assert(partial_table->chunk_count() == job_chunk_count,
                   "Unexpected chunk count of " + std::to_string(partial_table->chunk_count()) + " (expected " +
                   std::to_string(job_chunk_count) + " chunks).");
            Assert(partial_table->row_count() == rows_to_generate, "Unexpected row count.");

            for (auto chunk_id = ChunkID{0}; chunk_id < job_chunk_count; ++chunk_id) {
              // std::printf("job id %zu appends to position %zu (job_chunk_count is %zu)\n", job_id, start_offset + chunk_id, job_chunk_count);
              generated_chunks[start_offset + chunk_id] = partial_table->get_chunk(chunk_id);
            }

            if (!column_definitions) {
              column_definitions = partial_table->column_definitions();
            }
            std::printf("job_id %zu finished.\n", job_id);
          }));

          accumulated_rows += rows_to_generate;
          ++job_id;
        }
        Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

        Assert(column_definitions, "Unexpected nullopt for column definitions.");
        customer_table = std::make_shared<Table>(*column_definitions, TableType::Data, std::move(generated_chunks), UseMvcc::Yes);
      } else {
        customer_table = tpch_table_generator.create_customer_table(customer_row_count, 0);
        const auto& orders_and_lineitem = tpch_table_generator.create_orders_and_lineitem_tables(orders_row_count, 0);
        orders_table = orders_and_lineitem.first;
        lineitem_table = orders_and_lineitem.second;
      }

      const auto end_table_creation = std::chrono::steady_clock::now();

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
      for (const auto& table : {customer_table, orders_table, lineitem_table}) {
        encoding_jobs.emplace_back(std::make_shared<JobTask>([&, table]() {
          const auto chunk_count = table->chunk_count();
          for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
            const auto& chunk = table->get_chunk(chunk_id);
            if (chunk->is_mutable()) {
              chunk->finalize();
            }
          }

          const auto benchmark_config = BenchmarkConfig::get_default_config();
          BenchmarkTableEncoder::encode("no_mapping", table, benchmark_config.encoding_config);
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

      const auto begin_query = std::chrono::steady_clock::now();
      auto q3_pipeline = SQLPipelineBuilder{std::string{"SELECT l_orderkey, SUM(l_extendedprice*(1.0-l_discount)) as revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;"}}
                                   .create_pipeline();
      const auto [q3_pipeline_status, q3_result] = q3_pipeline.get_result_table();
      const auto end_query = std::chrono::steady_clock::now();

      if (run_id == 0) {  // Run 0 has the "expected" seed.
        for (const auto& [table_name, table] : std::vector<std::pair<std::string, std::shared_ptr<Table>>>
                                                          {{std::string{"customer"}, customer_table},
                                                           {std::string{"orders"}, orders_table},
                                                           {std::string{"lineitem"}, lineitem_table}}) {
          std::cout << "table_name: " << table_name << " >> " << storage_manager.get_table(table_name)->row_count() << std::endl;
        }
        Assert(q3_pipeline_status == SQLPipelineStatus::Success, "Q3 failed.");
        std::cout << q3_result->row_count() << std::endl;
        Print::print(q3_result);
        Assert(q3_result->row_count() == 10, "Unexpected result size.");
      }

      Hyrise::get().scheduler()->finish();

      if (run_id == 0) {
        // Warm up run
        continue;
      }

      for (const auto& [step_name, runtimes] : std::vector{std::pair{std::string{"LOADING"}, std::pair{begin_table_creation, end_table_creation}},
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
