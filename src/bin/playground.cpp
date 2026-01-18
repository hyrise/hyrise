#include <iostream>
#include <memory>
#include <fstream>

#include "../lib/optimizer/optimizer.hpp"
#include "../lib/sql/sql_pipeline_builder.hpp"
#include "benchmark_config.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

// int test_ceb_queries() {
//   auto benchmark_config = std::make_shared<BenchmarkConfig>();

//   std::string data_path =
//       getenv("DATA_PATH") ? getenv("DATA_PATH") : "/Users/paulrossler/Documents/masterthesis/hyrise/imdb_data";

//   FileBasedTableGenerator table_generator{benchmark_config, data_path};
//   table_generator.generate_and_store();
//   std::cout << "Tables loaded." << std::endl;

//   auto optimizer = Optimizer::create_default_optimizer();

//   std::string path = getenv("QUERY_PATH")
//                          ? getenv("QUERY_PATH")
//                          : "/Users/paulrossler/Documents/masterthesis/hyrise/resources/benchmark/ceb/test";

//   FileBasedBenchmarkItemRunner benchmark_item_runner{
//       benchmark_config, path, std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"}, std::nullopt};

//   std::cout << benchmark_item_runner.items().size() << " items to run." << std::endl;
//   std::cout << benchmark_item_runner.invalid_queries << " invalid queries." << std::endl;

//   int can_be_optimized = 0;
//   int cannot_be_optimized = 0;

//   for (auto query : benchmark_item_runner.queries()) {
//     SQLPipelineBuilder builder = SQLPipelineBuilder{query.sql};
//     auto pipeline = builder.create_pipeline();

//     try {
//       auto optimized_pipeline = pipeline.get_optimized_logical_plans();
//       can_be_optimized++;
//     } catch (const std::exception& e) {
//       std::cout << "Error with query: " << query.name << ": " << e.what() << std::endl;
//       cannot_be_optimized++;
//     }
//   }

//   std::cout << "Out of " << benchmark_item_runner.queries().size() << " queries, " << std::endl;
//   std::cout << can_be_optimized << " queries can be optimized." << std::endl;
//   std::cout << cannot_be_optimized << " queries cannot be optimized." << std::endl;
//   std::cout << benchmark_item_runner.invalid_queries << " invalid queries." << std::endl;
//   // total queries
//   std::cout << "Total queries: " << benchmark_item_runner.queries().size() << std::endl;

//   std::cout << "Optimized logical plan:" << std::endl;
//   return 0;
// }


// int print_histograms() {
//   auto benchmark_config = std::make_shared<BenchmarkConfig>();

//   std::vector<std::string> data_paths = {
//       "/Users/paulrossler/Documents/masterthesis/hyrise/imdb_data",
//       // "/Users/paulrossler/Documents/masterthesis/hyrise/ssb_data/sf-1",
//       // "/Users/paulrossler/Documents/masterthesis/hyrise/tpcc_cached_tables/sf-1",
//       // "/Users/paulrossler/Documents/masterthesis/hyrise/tpcds_cached_tables/sf-1",
//        //"/Users/paulrossler/Documents/masterthesis/hyrise/tpch_cached_tables/sf-1.000000"
//     };
  
//       auto filename = "table_statistics_output.txt";

//   std::ofstream out(filename);

//   for (auto data_path : data_paths) {
//     // std::cout << "Loading data from path: " << data_path << std::endl;
//     FileBasedTableGenerator table_generator{benchmark_config, data_path};
//     table_generator.generate_and_store();
//     // std::cout << "Tables loaded." << std::endl;

//     for (auto table : Hyrise::get().storage_manager.tables()) {
//       // std::cout << "Table: " << table.first << std::endl;
//       // std::cout << table.second->table_statistics()->column_statistics.size() << " columns with statistics."
//       //           << std::endl;
//       // std::cout << table.second->second_table_statistics()->column_statistics.size()
//       //           << " columns with second statistics." << std::endl;
//       out << "Table: " << table.first << std::endl;

//       // write to file
//       out << "First statistics:" << std::endl;
//       out << *table.second->table_statistics() << std::endl;
//       out << "Second statistics:" << std::endl;
//       out << *table.second->second_table_statistics() << std::endl;

//       Hyrise::get().storage_manager.drop_table(table.first);
//     }
//   }
//   out.close();
//   return 0;
// }

// void build_histograms() {
//   auto benchmark_config = std::make_shared<BenchmarkConfig>();

//   std::vector<std::string> data_paths = {
//       "/Users/paulrossler/Documents/masterthesis/hyrise/imdb_data",
//       "/Users/paulrossler/Documents/masterthesis/hyrise/ssb_data/sf-1",
//       "/Users/paulrossler/Documents/masterthesis/hyrise/tpcc_cached_tables/sf-1",
//       "/Users/paulrossler/Documents/masterthesis/hyrise/tpcds_cached_tables/sf-1",
//       "/Users/paulrossler/Documents/masterthesis/hyrise/tpch_cached_tables/sf-1.000000"
//     };

//   for (auto data_path : data_paths) {
//     FileBasedTableGenerator table_generator{benchmark_config, data_path};
//     table_generator.generate_and_store();
//   }
// }

int main() {
  // print_histograms();
  // build_histograms();
  return 0;
}
