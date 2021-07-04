#include <iostream>
#include <chrono>

#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "storage/table.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

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

int main() {
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
  
  for (auto table_name : {"part", "supplier", "partsupp", "customer", "orders", "nation", "region"}) {
    std::cout << indented_string(10, table_name) << "\t| ";
    for (auto column_type  : table_info_by_name.at(table_name).table->column_data_types()) {
      std::cout << column_type << "| ";
    }
    std::cout << std::endl;
  }
  std::cout << std::endl;


  std::chrono::steady_clock::time_point begin;
  std::chrono::steady_clock::time_point end;
  for (auto table_name : {"orders", "part", "supplier", "partsupp", "customer", "nation", "region"}) {
    auto column = ColumnID{0};
    println();println(); println("##########################"); showln(table_name);

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
      resolve_data_type(column_type, [&](auto type) {
        using ColumnType = typename decltype(type)::type;

        auto histograms = std::vector<std::shared_ptr<EqualDistinctCountHistogram<ColumnType>>>();

        TIMEIT(
          for(auto i = ChunkID{0}; i < table_chunk_count; i++) {
            auto hist = EqualDistinctCountHistogram<ColumnType>::from_segment(*table, column, ChunkID{i}, 2*histogram_bin_count);
            histograms.push_back(hist);
          }
        , "creating_per_segment_histograms");
        TIMEIT(
          const auto merged_hist = EqualDistinctCountHistogram<ColumnType>::merge(histograms, histogram_bin_count);
        , "merge_all_histograms_with_hlls");
        TIMEIT(
          const auto full_hist = EqualDistinctCountHistogram<ColumnType>::from_column(*table, column, histogram_bin_count);
        , "creating_the_full_histogram");
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
        // auto merged_row_error = 0.0;
        // auto merged_pyramid_error = 0.0;
        for (auto entry : map) {
          auto value = entry.first;
          auto count = entry.second;
          auto estimate = full_hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
          full_hist_error += std::abs(estimate - count);
          auto merged_estimate = merged_hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
          merged_hist_error += std::abs(merged_estimate - count);
          // auto merged_row_estimate = row_merged_histogram->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
          // merged_row_error += std::abs(merged_row_estimate - count);
          // auto merged_pyramid_estimate = merged_hist_pyramid->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
          // merged_pyramid_error += std::abs(merged_pyramid_estimate - count);
        }
        show(full_hist_error);
        show(merged_hist_error);
        // show(merged_row_error);
        // show(merged_pyramid_error);
        show(full_hist->bin_count()); show(merged_hist->bin_count());

      });
      column++;
    }
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

