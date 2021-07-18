#include <iostream>
#include <chrono>

#include <fstream>

#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "storage/table.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

#include <boost/algorithm/string.hpp>
#include <cxxopts.hpp>

#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"
#include "utils/performance_warning.hpp"
#include "utils/sqlite_add_indices.hpp"


#define STRING(X) #X
#define print(X) std::cout << STRING(X) << " ";
#define println(X) std::cout << STRING(X) << std::endl;
#define show(X) std::cout << STRING(X) << " = " << X << " ";
#define showln(X) std::cout << STRING(X) << " = " << X << std::endl;

#define TIMEIT(X, Y) begin = std::chrono::steady_clock::now(); X; end = std::chrono::steady_clock::now(); std::cout << Y << ": Time difference = \t" << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "[µs]" << std::endl;

using namespace opossum;  // NOLINT

template<typename T>
std::shared_ptr<EqualDistinctCountHistogram<T>> merge_histograms(std::vector<std::shared_ptr<EqualDistinctCountHistogram<T>>>& histograms, size_t start, size_t end, const size_t bin_count) {
  if (start == end) return histograms[start];
  if (end == start+1) {
    return EqualDistinctCountHistogram<T>::merge(histograms[start], histograms[end], 2*bin_count);
  }
  int middle = start + (end - start) / 2;
  return EqualDistinctCountHistogram<T>::merge(merge_histograms(histograms, start, middle, 2*bin_count), merge_histograms(histograms, middle+1, end, 2*bin_count), 2*bin_count);
}

std::string indented_string(int n, std::string message) {

    // Construct string of 'n - message.length' spaces
    std::string indent(n - message.length(), ' ');

    return message + indent;
}


void job(int argc, char** argv) {
  auto cli_options = BenchmarkRunner::get_basic_cli_options("Hyrise Join Order Benchmark");

  const auto DEFAULT_TABLE_PATH = "imdb_data";
  const auto DEFAULT_QUERY_PATH = "third_party/join-order-benchmark";

  // clang-format off
  cli_options.add_options()
  ("table_path", "Directory containing the Tables as csv, tbl or binary files. CSV files require meta-files, see csv_meta.hpp or any *.csv.json file.", cxxopts::value<std::string>()->default_value(DEFAULT_TABLE_PATH)) // NOLINT
  ("query_path", "Directory containing the .sql files of the Join Order Benchmark", cxxopts::value<std::string>()->default_value(DEFAULT_QUERY_PATH)) // NOLINT
  ("q,queries", "Subset of queries to run as a comma separated list", cxxopts::value<std::string>()->default_value("all")); // NOLINT
  // clang-format on

  std::shared_ptr<BenchmarkConfig> benchmark_config;
  std::string query_path;
  std::string table_path;
  // Comma-separated query names or "all"
  std::string queries_str;

  // Parse command line args
  const auto cli_parse_result = cli_options.parse(argc, argv);

  // if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) return 0;

  query_path = cli_parse_result["query_path"].as<std::string>();
  table_path = cli_parse_result["table_path"].as<std::string>();
  queries_str = cli_parse_result["queries"].as<std::string>();

  benchmark_config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_cli_options(cli_parse_result));

  // Check that the options "query_path" and "table_path" were specified
  if (query_path.empty() || table_path.empty()) {
    std::cerr << "Need to specify --query_path=path/to/queries and --table_path=path/to/table_files" << std::endl;
    std::cerr << cli_options.help({}) << std::endl;
    return;
  }
  std::cout << "Loading tables..." << std::endl;
  auto table_generator = std::make_unique<FileBasedTableGenerator>(benchmark_config, table_path);
  auto table_info_by_name = table_generator->generate();
  for (const auto e : table_info_by_name) {
    std::cout << e.first << std::endl;
    const auto table = table_info_by_name.at(e.first).table;
    const auto table_chunk_count = table->chunk_count();
    const auto table_row_count = table->row_count();
    showln(table_row_count);
    showln(table_chunk_count);
  }
}

int main(int argc, char** argv) {
  // job(argc, argv);
  // return 0;
  // const auto chunk_size = 1000;
  const auto dir_001 = std::string{"resources/test_data/tbl/tpch/sf-0.02/"};

  const auto scale_factor = 1.0f;
  const auto table_info_by_name = TPCHTableGenerator(scale_factor, ClusteringConfiguration::None).generate();

  // EXPECT_EQ(table_info_by_name.at("part").table->row_count(), std::floor(200'000 * scale_factor));
  // EXPECT_EQ(table_info_by_name.at("supplier").table->row_count(), std::floor(10'000 * scale_factor));
  // EXPECT_EQ(table_info_by_name.at("partsupp").table->row_count(), std::floor(800'000 * scale_factor));
  // EXPECT_EQ(table_info_by_name.at("customer").table->row_count(), std::floor(150'000 * scale_factor));
  // EXPECT_EQ(table_info_by_name.at("orders").table->row_count(), std::floor(1'500'000 * scale_factor));
  // EXPECT_EQ(table_info_by_name.at("nation").table->row_count(), std::floor(25));
  // EXPECT_EQ(table_info_by_name.at("region").table->row_count(), std::floor(5));

  // part:           |int|   string| string| string| string| int|    string| float|  string|
  // supplier:       |int|   string| string| int|    string| float|  string|
  // partsupp:       |int|   int|    int|    float|  string|
  // customer:       |int|   string| string| int|    string| float|  string| string|
  // orders:         |int|   int|    string| float|  string| string| string| int|    string|
  // nation:         |int|   string| int|    string|
  // region:         |int|   string| string|

  for (auto table_name : {"part", "supplier", "partsupp", "customer", "orders", "nation", "region", "lineitem"}) {
    std::cout << indented_string(10, table_name) << "\t| ";
    const auto table = table_info_by_name.at(table_name).table;
    const auto table_row_count = table->row_count();
    const auto table_chunk_count = table->chunk_count();
    auto column = ColumnID{0};
    std::cout << table_row_count << "rows: ";
    for (auto column_type  : table_info_by_name.at(table_name).table->column_data_types()) {
      std::cout << column_type << ", ";
      resolve_data_type(column_type, [&](auto type) {
        using ColumnType = typename decltype(type)::type;
        auto map = std::unordered_map<ColumnType, int>();
        for (ChunkID i = ChunkID{0}; i < table_chunk_count; i++) {
          const auto& chunk = table->get_chunk(i);
          for (const auto& value : std::dynamic_pointer_cast<ValueSegment<ColumnType>>(chunk->get_segment(column))->values()) {
            // auto value = boost::get<ColumnType>(row[column]);
            map[value]++;
          }
        }
        std::cout << map.size() << " || ";
      });
      column++;
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;
  //return 0;

  std::chrono::steady_clock::time_point begin;
  std::chrono::steady_clock::time_point end;
  for (auto table_name : {"partsupp", "orders", "part", "supplier", "customer", "nation", "region"}) {
    auto column = ColumnID{0};
    println();println(); println("##########################"); showln(table_name);

    std::ofstream output_file;
    output_file.open ("../outputs/measurements_q_"+std::string(table_name) + ".txt");
    output_file << "column_id,column_type,time_per_segment_histograms,time_merging_histograms_hlls,time_merging_histograms_without_hlls,time_full_histogram,error_full,error_merged_hll,error_merged_without_hll,full_hist_q,merged_hist_q,merged_hist_without_hlls_q" << std::endl;

    const auto table = table_info_by_name.at(table_name).table;
    const auto table_chunk_count = table->chunk_count();
    const auto table_row_count = table->row_count();
    const auto histogram_bin_count = std::min<size_t>(100, std::max<size_t>(5, table_row_count / 2'000));
    showln(table_row_count);
    showln(table_chunk_count);
    showln(histogram_bin_count);
    for (auto column_type : table_info_by_name.at(table_name).table->column_data_types()) {
      if (column_type == DataType::String) {column++; continue;};
      println();
      showln(column); showln(column_type);
      output_file << column << "," << column_type <<",";
      // if (column == 0)
      resolve_data_type(column_type, [&](auto type) {
        using ColumnType = typename decltype(type)::type;

        auto histograms = std::vector<std::shared_ptr<EqualDistinctCountHistogram<ColumnType>>>();

        TIMEIT(
          for(auto i = ChunkID{0}; i < table_chunk_count; i++) {
            auto hist = EqualDistinctCountHistogram<ColumnType>::from_segment(*table, column, ChunkID{i}, 2*histogram_bin_count);
            histograms.push_back(hist);
          }
        , "creating_per_segment_histograms");
        output_file << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << ",";
        TIMEIT(
          const auto merged_hist = EqualDistinctCountHistogram<ColumnType>::merge(histograms, histogram_bin_count);
        , "merge_all_histograms_with_hlls");
        output_file << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << ",";
        TIMEIT(
          const auto merged_hist_wo_hlls = EqualDistinctCountHistogram<ColumnType>::merge(histograms, histogram_bin_count, false);
        , "merge_all_histograms_without_hlls");
        output_file << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << ",";
        TIMEIT(
          const auto full_hist = EqualDistinctCountHistogram<ColumnType>::from_column(*table, column, histogram_bin_count);
        , "creating_the_full_histogram");
        output_file << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << ",";
        // TIMEIT(
        //   const auto merged_hist_pyramid = merge_histograms(histograms, 0u, histograms.size()-1, histogram_bin_count);
        // , "histogram_merge_pyramid");
        // TIMEIT(
        //   auto histograms_size = histograms.size();
        //   std::shared_ptr<EqualDistinctCountHistogram<ColumnType>> row_merged_histogram = histograms[0];
        //   for (auto i = 1u; i < histograms_size; i++) {
        //     row_merged_histogram = EqualDistinctCountHistogram<ColumnType>::merge(row_merged_histogram, histograms[i], histogram_bin_count);
        //   }
        // , "histogram_merge_row");

        auto map = std::unordered_map<ColumnType, int>();
        for (ChunkID i = ChunkID{0}; i < table_chunk_count; i++) {
          const auto& chunk = table->get_chunk(i);
          for (const auto& value : std::dynamic_pointer_cast<ValueSegment<ColumnType>>(chunk->get_segment(column))->values()) {
            // auto value = boost::get<ColumnType>(row[column]);
            map[value]++;
          }
        }

        auto full_hist_error = 0.0;
        auto merged_hist_error = 0.0;
        auto merged_hist_wo_hlls_error = 0.0;
        // auto merged_row_error = 0.0;
        // auto merged_pyramid_error = 0.0;
        auto full_hist_q = 1.0;
        auto merged_hist_q = 1.0;
        auto merged_hist_wo_hlls_q = 1.0;
        for (auto entry : map) {
          auto value = entry.first;
          auto count = entry.second;
          auto estimate = full_hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
          full_hist_error += std::abs(estimate - count);
          auto merged_wo_hlls_estimate = merged_hist_wo_hlls->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
          merged_hist_wo_hlls_error += std::abs(merged_wo_hlls_estimate - count);
          auto merged_estimate = merged_hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
          merged_hist_error += std::abs(merged_estimate - count);
          // auto merged_row_estimate = row_merged_histogram->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
          // merged_row_error += std::abs(merged_row_estimate - count);
          // auto merged_pyramid_estimate = merged_hist_pyramid->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
          // merged_pyramid_error += std::abs(merged_pyramid_estimate - count);
          full_hist_q = std::max((double)full_hist_q, std::max((double)estimate/count, (double)count/estimate));
          merged_hist_q = std::max((double)merged_hist_q, std::max((double)merged_estimate/count, (double)merged_estimate/estimate));
          merged_hist_wo_hlls_q = std::max((double)merged_hist_wo_hlls_q, std::max((double)merged_wo_hlls_estimate/count, (double)merged_wo_hlls_estimate/estimate));

        }
        output_file << full_hist_error / map.size() << "," << merged_hist_error / map.size() << "," << merged_hist_wo_hlls_error / map.size() << ",";
        output_file << full_hist_q << "," << merged_hist_q << "," << merged_hist_wo_hlls_q << std::endl;
        show(full_hist_error);
        show(merged_hist_error);
        show(merged_hist_wo_hlls_error);
        // show(merged_row_error);
        // show(merged_pyramid_error);
        show(full_hist->bin_count());

      });
      column++;
    }
    output_file.close();
  }


  // auto parts = load_table(dir_001 + "part.tbl", chunk_size);
  // auto customers = load_table(dir_001 + "customer.tbl", chunk_size);
  // auto lineitem = load_table(dir_001 + "lineitem.tbl", chunk_size);

  // Column:          0     |1     |2     |3     |4     |5     |6     |7     |8     |9     |10    |11    |12    |13    |14    |15
  // Customer Table:  int   |string|string|int   |string|float |string|string 
  // Part Table:      int   |string|string|string|string|int   |string|float |string
  // LineItem:        int   |int   |int   |int   |float |float |float |float |string|string|string|string|string|string|string|string

  // auto table = table_info_by_name.at("customer").table;
  // auto column = ColumnID{0};
  // using ColumnType = int;
  // auto chunk_count = table->chunk_count();
  

  // showln(chunk_count);
  // const auto histogram_bin_count = std::min<size_t>(100, std::max<size_t>(5, table->row_count() / 2'000));
  // showln(histogram_bin_count);
  // auto histograms = std::vector<std::shared_ptr<EqualDistinctCountHistogram<ColumnType>>>();
  // TIMEIT(
  //   for(auto i = ChunkID{0}; i < chunk_count; i++) {
  //     auto hist = EqualDistinctCountHistogram<ColumnType>::from_segment(*table, column, ChunkID{i}, 2*histogram_bin_count);
  //     histograms.push_back(hist);
  //   }
  // , "small_histograms");
  // TIMEIT(
  //   //const auto merged_hist = histograms[0];
  //   const auto merged_hist = EqualDistinctCountHistogram<ColumnType>::merge(histograms, histogram_bin_count);
  // , "histogram_merge");
  // TIMEIT(
  //   const auto full_hist = EqualDistinctCountHistogram<ColumnType>::from_column(*table, column, histogram_bin_count);
  // , "full_histogram");
  // auto map = std::unordered_map<ColumnType,int>();
  // for (auto row : table->get_rows()) {
  //   auto value = boost::get<ColumnType>(row[column]);
  //   map[value]++;
  // }
  // TIMEIT(
  //   const auto merged_hist_pyramid = merge_histograms(histograms, 0u, histograms.size()-1, histogram_bin_count);
  // , "histogram_merge_pyramid");
  // TIMEIT(
  //   auto histograms_size = histograms.size();
  //   std::shared_ptr<EqualDistinctCountHistogram<ColumnType>> row_merged_histogram = histograms[0];
  //   for (auto i = 1u; i < histograms_size; i++) {
  //     row_merged_histogram = EqualDistinctCountHistogram<ColumnType>::merge(row_merged_histogram, histograms[i], histogram_bin_count);
  //   }
  // , "histogram_merge_row");
  
  // auto full_hist_error = 0.0;
  // auto merged_hist_error = 0.0;
  // auto merged_row_error = 0.0;
  // auto merged_pyramid_error = 0.0;
  // int counter = 0;
  // for (auto entry: map) {
  //   auto value = entry.first;
  //   auto count = entry.second;
  //   auto estimate = full_hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
  //   full_hist_error += std::abs(estimate - count);
  //   auto merged_estimate = merged_hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
  //   merged_hist_error += std::abs(merged_estimate - count);
  //   auto merged_row_estimate = row_merged_histogram->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
  //   merged_row_error += std::abs(merged_row_estimate - count);
  //   auto merged_pyramid_estimate = merged_hist_pyramid->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
  //   merged_pyramid_error += std::abs(merged_pyramid_estimate - count);
  //   // if ((counter < 15)) {
  //   //   println();
  //   //   show(value); showln(count);
  //   //   show(estimate); show(merged_estimate); show(merged_row_estimate); showln(merged_pyramid_estimate);
  //   //   auto binID = merged_hist_pyramid->_bin_for_value(value);
  //   //   showln(binID);
  //   //   show(merged_hist_pyramid->bin_minimum(binID)); showln(merged_hist_pyramid->bin_maximum(binID));
  //   // }
  //   counter++;
  // }
  // showln(table->row_count());
  // showln(full_hist_error);
  // showln(merged_hist_error);
  // showln(merged_row_error);
  // showln(merged_pyramid_error);
  // show(full_hist->bin_count()); show(merged_hist->bin_count()); show(row_merged_histogram->bin_count()); showln(merged_hist_pyramid->bin_count()); 

  return 0;
}

