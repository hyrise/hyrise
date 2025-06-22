#include <chrono>
#include <iostream>
#include <string>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "cxxopts.hpp"
#include "magic_enum.hpp"
#include "SQLParserResult.h"

#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "hyrise.hpp"
#include "jcch/jcch_benchmark_item_runner.hpp"
#include "jcch/jcch_table_generator.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_queries.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/assert.hpp"
#include "utils/sqlite_add_indices.hpp"

using namespace hyrise;  // NOLINT

/**
 * This benchmark measures Hyrise's performance executing the TPC-H *queries*, it doesn't (yet) support running the
 * TPC-H *benchmark* exactly as it is specified.
 * (Among other things, the TPC-H requires performing data refreshes and has strict requirements for the number of
 * sessions running in parallel. See http://www.tpc.org/tpch/default.asp for more info)
 * The benchmark offers a wide range of options (scale_factor, chunk_size, ...) but most notably it offers two modes:
 * IndividualQueries and PermutedQuerySets. See docs on BenchmarkMode for details.
 * The benchmark will stop issuing new queries if either enough iterations have taken place or enough time has passed.
 *
 * main() is mostly concerned with parsing the CLI options while BenchmarkRunner.run() performs the actual benchmark
 * logic.
 *
 * The same binary is used to run the JCC-H benchmark. For this, simply use the -j flag.
 */

int main(int argc, char* argv[]) {
  auto cli_options = BenchmarkRunner::get_basic_cli_options("TPC-H/JCC-H Benchmark");

  // clang-format off
  cli_options.add_options()
    ("s,scale", "Database scale factor (10.0 ~ 10 GB)", cxxopts::value<float>()->default_value("10"))
    ("q,queries", "Specify queries to run (comma-separated query ids, e.g. \"--queries 1,3,19\"), default is all", cxxopts::value<std::string>()) // NOLINT
    ("use_prepared_statements", "Use prepared statements instead of random SQL strings", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("j,jcch", "Use JCC-H data and query generators instead of TPC-H. If this parameter is used, table data always "
               "contains skew. With --jcch=skewed, queries are generated to be affected by this skew. With "
               "--jcch=normal, query parameters access the unskewed part of the tables ", cxxopts::value<std::string>()->default_value("")) // NOLINT
    ("clustering", "Clustering of TPC-H data. The default of --clustering=None means the data is stored as generated "
                   "by the TPC-H data generator. With --clustering=\"Pruning\", the two largest tables 'lineitem' "
                   "and 'orders' are sorted by 'l_shipdate' and 'o_orderdate' for improved chunk pruning. Both are "
                   "legal TPC-H input data.", cxxopts::value<std::string>()->default_value("None")) // NOLINT
    ("lmem_ids", "Specify the local memory NUMA nodes (comma-separated NUMA IDs, e.g., --lmem_ids 0,2)", cxxopts::value<std::string>()->default_value("0")) // NOLINT
    ("rmem_ids", "Specify the remote memory NUMA nodes (comma-separated NUMA IDs, e.g., --rmem_ids 1,3,4)", cxxopts::value<std::string>()) // NOLINT
    ("rmem_weights", "Specify the remote memory NUMA node weights (comma-separated weights, e.g., --rmem_ids 2,1,1)", cxxopts::value<std::string>()) // NOLINT
    ("fixed_columns", "Specify the number of most frequently accesses columns fixed in local memory", cxxopts::value<uint32_t>()->default_value("0")); // NOLINT
  // clang-format on

  auto config = std::shared_ptr<BenchmarkConfig>{};
  auto comma_separated_queries = std::string{};
  auto scale_factor = float{};
  auto use_prepared_statements = false;
  auto jcch = false;
  auto jcch_skewed = false;
  auto comma_separated_lmem_ids = std::string{};
  auto comma_separated_rmem_ids = std::string{};
  auto comma_separated_rmem_weights = std::string{};
  auto fixed_local_columns = uint32_t{};

  // Parse command line args
  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) {
    return 0;
  }

  if (cli_parse_result.count("queries")) {
    comma_separated_queries = cli_parse_result["queries"].as<std::string>();
  }

  scale_factor = cli_parse_result["scale"].as<float>();

  config = CLIConfigParser::parse_cli_options(cli_parse_result);
  {
    const auto json_ext = std::string{".json"};
    const auto& path = config->output_file_path;
    Assert(path, "Output file path is not set (-o <path>).");
    Assert((*path).size() >= json_ext.size() && (*path).substr((*path).size() - json_ext.size()) == json_ext,
      "Output file path does not end with .json");
  }

  use_prepared_statements = cli_parse_result["use_prepared_statements"].as<bool>();
  jcch = cli_parse_result.count("jcch");
  if (jcch) {
    const auto jcch_mode = cli_parse_result["jcch"].as<std::string>();
    if (jcch_mode == "skewed") {
      jcch_skewed = true;
    } else if (jcch_mode == "normal") {  // NOLINT
      jcch_skewed = false;
    } else {
      Fail("Invalid JCC-H mode, use skewed or normal.");
    }
  }

  if (cli_parse_result.count("lmem_ids")) {
    comma_separated_lmem_ids = cli_parse_result["lmem_ids"].as<std::string>();
  }

  if (cli_parse_result.count("rmem_ids")) {
    comma_separated_rmem_ids = cli_parse_result["rmem_ids"].as<std::string>();
  }

  if (cli_parse_result.count("rmem_weights")) {
    comma_separated_rmem_weights = cli_parse_result["rmem_weights"].as<std::string>();
  }

  if (cli_parse_result.count("fixed_columns")) {
    fixed_local_columns = cli_parse_result["fixed_columns"].as<uint32_t>();
  }

  auto clustering_configuration = ClusteringConfiguration::None;
  if (cli_parse_result.count("clustering")) {
    auto clustering_configuration_parameter = cli_parse_result["clustering"].as<std::string>();
    if (clustering_configuration_parameter == "Pruning") {
      clustering_configuration = ClusteringConfiguration::Pruning;
    } else if (clustering_configuration_parameter != "None") {
      Fail("Invalid clustering config: '" + clustering_configuration_parameter + "'.");
    }

    std::cout << "- Clustering with '" << magic_enum::enum_name(clustering_configuration) << "' configuration\n";
  }

  std::vector<BenchmarkItemID> item_ids;

  // Build list of query ids to be benchmarked and display it
  if (comma_separated_queries.empty()) {
    std::transform(tpch_queries.begin(), tpch_queries.end(), std::back_inserter(item_ids), [](auto& pair) {
      return BenchmarkItemID{pair.first - 1};
    });
  } else {
    // Split the input into query ids, ignoring leading, trailing, or duplicate commas
    auto item_ids_str = std::vector<std::string>();
    boost::trim_if(comma_separated_queries, boost::is_any_of(","));
    boost::split(item_ids_str, comma_separated_queries, boost::is_any_of(","), boost::token_compress_on);
    std::transform(item_ids_str.begin(), item_ids_str.end(), std::back_inserter(item_ids), [](const auto& item_id_str) {
      const auto item_id =
          BenchmarkItemID{boost::lexical_cast<BenchmarkItemID::base_type, std::string>(item_id_str) - 1};
      DebugAssert(item_id < 22, "There are only 22 queries.");
      return item_id;
    });
  }

  // Build list of memory ids
  auto lmem_node_ids = NumaNodeIDs{};
  if (!comma_separated_lmem_ids.empty()) {
    auto local_numa_nodes_str = std::vector<std::string>();
    boost::trim_if(comma_separated_lmem_ids, boost::is_any_of(","));
    boost::split(local_numa_nodes_str, comma_separated_lmem_ids, boost::is_any_of(","), boost::token_compress_on);
    std::transform(local_numa_nodes_str.begin(), local_numa_nodes_str.end(), std::back_inserter(lmem_node_ids), [](const auto& node_id_str) {
      return static_cast<NumaNodeID>(std::stoul(node_id_str));
    });
  }

  auto rmem_node_ids = NumaNodeIDs{};
  if (!comma_separated_rmem_ids.empty()) {
    auto remote_numa_nodes_str = std::vector<std::string>();
    boost::trim_if(comma_separated_rmem_ids, boost::is_any_of(","));
    boost::split(remote_numa_nodes_str, comma_separated_rmem_ids, boost::is_any_of(","), boost::token_compress_on);
    std::transform(remote_numa_nodes_str.begin(), remote_numa_nodes_str.end(), std::back_inserter(rmem_node_ids), [](const auto& node_id_str) {
      return static_cast<NumaNodeID>(std::stoul(node_id_str));
    });
  }

  {
    auto print_lmem_node_ids = std::vector<std::string>();
    std::for_each(lmem_node_ids.begin(), lmem_node_ids.end(), [&print_lmem_node_ids](auto& id) {
      print_lmem_node_ids.push_back(std::to_string(id));
    });
    std::cout << "- Local memory NUMA node IDs: [ " << boost::algorithm::join(print_lmem_node_ids, ", ") << " ]\n";
  }

  {
    auto print_rmem_node_ids = std::vector<std::string>();
    std::for_each(rmem_node_ids.begin(), rmem_node_ids.end(), [&print_rmem_node_ids](auto& id) {
      print_rmem_node_ids.push_back(std::to_string(id));
    });
    std::cout << "- Remote memory NUMA node IDs: [ " << boost::algorithm::join(print_rmem_node_ids, ", ") << " ]\n";
  }


  // Build list of remote memory weights
  auto rmem_weights = InterleavingWeights{};
  if (!comma_separated_rmem_weights.empty()) {
    auto rmem_weights_str = std::vector<std::string>();
    boost::trim_if(comma_separated_rmem_weights, boost::is_any_of(","));
    boost::split(rmem_weights_str, comma_separated_rmem_weights, boost::is_any_of(","), boost::token_compress_on);
    std::transform(rmem_weights_str.begin(), rmem_weights_str.end(), std::back_inserter(rmem_weights), [](const auto& weight_str) {
      return static_cast<uint32_t>(std::stoul(weight_str));
    });
  }

  {
    auto print_rmem_weights = std::vector<std::string>();
    std::for_each(rmem_weights.begin(), rmem_weights.end(), [&print_rmem_weights](auto& weight) {
      print_rmem_weights.push_back(std::to_string(weight));
    });
    std::cout << "- Remote memory NUMA node weights: [ " << boost::algorithm::join(print_rmem_weights, ", ") << " ]\n";
  }

  std::cout << "- # columns fixed in local memory: " << fixed_local_columns << "\n";

  const auto type = [&](){
    if (fixed_local_columns > 0) {
      return PlacementType::AccessFrequency;
    }
    if (rmem_node_ids.size() > 0) {
      if (rmem_weights.size() > 0) {
        return PlacementType::PagesWeightedInterleaved;
      }
      return PlacementType::PagesRoundRobinInterleaved;
    }
    return PlacementType::None;
  }();
  std::cout << "- Data placement type: " << magic_enum::enum_name(type) << "\n";
  Hyrise::get().placement_options = {.scale_factor = scale_factor, .lmem_node_ids = lmem_node_ids,
                                     .rmem_node_ids = rmem_node_ids, .rmem_weights = rmem_weights, .type = type,
                                     .num_most_frequently_columns_local = fixed_local_columns,
                                     .json_path = *config->output_file_path};

  std::cout << "- Benchmarking Queries: [ ";
  auto printable_item_ids = std::vector<std::string>();
  std::for_each(item_ids.begin(), item_ids.end(), [&printable_item_ids](auto& id) {
    printable_item_ids.push_back(std::to_string(id + 1));
  });
  std::cout << boost::algorithm::join(printable_item_ids, ", ") << " ]\n";

  auto context = BenchmarkRunner::create_context(*config);

  Assert(!use_prepared_statements || !config->verify, "SQLite validation does not work with prepared statements.");

  if (config->verify) {
    // Hack: We cannot verify Q15, thus we remove it from the list of queries
    auto it = std::remove(item_ids.begin(), item_ids.end(), 15 - 1);
    if (it != item_ids.end()) {
      // The problem is that the last part of the query, "DROP VIEW", does not return a table. Since we also have
      // the TPC-H test against a known-to-be-good table, we do not want the additional complexity for handling this
      // in the BenchmarkRunner.
      std::cout << "- Skipping Query 15 because it cannot easily be verified\n";
      item_ids.erase(it, item_ids.end());
    }
  }

  std::cout << "- " << (jcch ? "JCC-H" : "TPC-H") << " scale factor is " << scale_factor << '\n';
  std::cout << "- Using prepared statements: " << (use_prepared_statements ? "yes" : "no") << '\n';

  // Add TPCH-specific information
  context.emplace("scale_factor", scale_factor);
  context.emplace("clustering", magic_enum::enum_name(clustering_configuration));
  context.emplace("use_prepared_statements", use_prepared_statements);

  auto table_generator = std::unique_ptr<AbstractTableGenerator>{};
  auto item_runner = std::unique_ptr<AbstractBenchmarkItemRunner>{};

  if (jcch) {
    // Different from the TPC-H benchmark, where the table and query generators are immediately embedded in Hyrise, the
    // JCC-H implementation calls those generators externally. This is because we would get linking conflicts if we were
    // to include both generators. Unfortunately, this approach is somewhat slower (30s to start SF1 with TPC-H, 1m18s
    // with JCC-H).
    //
    // JCC-H has both a skewed and a "normal" (i.e., unskewed) mode. The unskewed mode is not the same as TPC-H. You can
    // find details in the JCC-H paper: https://ir.cwi.nl/pub/27429

    // Try to find dbgen/qgen binaries
    auto jcch_dbgen_path =
        std::filesystem::canonical(std::string{argv[0]}).remove_filename() / "third_party/jcch-dbgen";
    Assert(std::filesystem::exists(jcch_dbgen_path / "dbgen"),
           std::string{"JCC-H dbgen not found at "} + jcch_dbgen_path.c_str());
    Assert(std::filesystem::exists(jcch_dbgen_path / "qgen"),
           std::string{"JCC-H qgen not found at "} + jcch_dbgen_path.c_str());

    // Create the jcch_data directory (if needed) and generate the jcch_data/sf-... path
    auto jcch_data_path_str = std::ostringstream{};
    jcch_data_path_str << "jcch_data/sf-" << std::noshowpoint << scale_factor;
    std::filesystem::create_directories(jcch_data_path_str.str());
    // Success of create_directories is guaranteed by the call to fs::canonical, which fails on invalid paths:
    const auto jcch_data_path = std::filesystem::canonical(jcch_data_path_str.str());
    const auto jcch_tables_path = jcch_data_path.string() + "/tables";

    std::cout << "- Using JCC-H dbgen from " << jcch_dbgen_path << '\n';
    std::cout << "- Storing JCC-H tables and query parameters in " << jcch_data_path << '\n';
    std::cout << "- JCC-H query parameters are " << (jcch_skewed ? "skewed" : "not skewed") << '\n';

    // Create the table generator and item runner
    table_generator = std::make_unique<JCCHTableGenerator>(jcch_dbgen_path, jcch_tables_path, scale_factor,
                                                           clustering_configuration, config);
    item_runner = std::make_unique<JCCHBenchmarkItemRunner>(jcch_skewed, jcch_dbgen_path, jcch_data_path, config,
                                                            use_prepared_statements, scale_factor,
                                                            clustering_configuration, item_ids);
  } else {
    table_generator = std::make_unique<TPCHTableGenerator>(scale_factor, clustering_configuration, config);
    item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, use_prepared_statements, scale_factor,
                                                            clustering_configuration, item_ids);
  }

  auto benchmark_runner =
      std::make_shared<BenchmarkRunner>(*config, std::move(item_runner), std::move(table_generator), context);
  Hyrise::get().benchmark_runner = benchmark_runner;

  if (config->verify) {
    add_indices_to_sqlite("resources/benchmark/tpch/schema.sql", "resources/benchmark/tpch/indices.sql",
                          benchmark_runner->sqlite_wrapper);
  }

  benchmark_runner->run();
}
