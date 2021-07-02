#include <iostream>
#include <chrono>

#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"

#define STRING(X) #X
#define print(X) std::cout << STRING(X) << " ";
#define println(X) std::cout << STRING(X) << std::endl;
#define show(X) std::cout << STRING(X) << " = " << X << " ";
#define showln(X) std::cout << STRING(X) << " = " << X << std::endl;

#define TIMEIT(X, Y) begin = std::chrono::steady_clock::now(); X; end = std::chrono::steady_clock::now(); std::cout << Y << ": Time difference = " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "[µs]" << std::endl;

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

int main() {
  std::cout << "Hello world!!" << std::endl;

  const auto chunk_size = 1000;
  const auto dir_001 = std::string{"resources/test_data/tbl/tpch/sf-0.01/"};
  auto parts = load_table(dir_001 + "part.tbl", chunk_size);
  auto customers = load_table(dir_001 + "customer.tbl", chunk_size);
  auto lineitem = load_table(dir_001 + "lineitem.tbl", chunk_size);

  
  // Column:          0     |1     |2     |3     |4     |5     |6     |7     |8
  // Customer Table:  int   |string|string|int   |string|float |string|string 
  // Part Table:      int   |string|string|string|string|int   |string|float |string
  // LineItem:        int   |int   |int   |int   |float |float |float |float |string|string|string|string|string|string|string|string
  auto table = lineitem;
  auto column = ColumnID{6};
  using ColumnType = float;
  auto chunk_count = table->chunk_count();
  std::chrono::steady_clock::time_point begin;
  std::chrono::steady_clock::time_point end;

  showln(chunk_count);
  const auto histogram_bin_count = std::min<size_t>(100, std::max<size_t>(5, table->row_count() / 2'000));
  showln(histogram_bin_count);
  auto histograms = std::vector<std::shared_ptr<EqualDistinctCountHistogram<ColumnType>>>();
  TIMEIT(
    for(auto i = ChunkID{0}; i < chunk_count; i++) {
      auto hist = EqualDistinctCountHistogram<ColumnType>::from_segment(*table, column, ChunkID{i}, 2*histogram_bin_count);
      histograms.push_back(hist);
    }
  , "small_histograms");
  TIMEIT(
    //const auto merged_hist = histograms[0];
    const auto merged_hist = EqualDistinctCountHistogram<ColumnType>::merge(histograms, histogram_bin_count);
  , "histogram_merge");
  TIMEIT(
    const auto full_hist = EqualDistinctCountHistogram<ColumnType>::from_column(*table, column, histogram_bin_count);
  , "full_histogram");
  auto map = std::unordered_map<ColumnType,int>();
  for (auto row : table->get_rows()) {
    auto value = boost::get<ColumnType>(row[column]);
    map[value]++;
  }
  TIMEIT(
    const auto merged_hist_pyramid = merge_histograms(histograms, 0u, histograms.size()-1, histogram_bin_count);
  , "histogram_merge_pyramid");
  TIMEIT(
    auto histograms_size = histograms.size();
    std::shared_ptr<EqualDistinctCountHistogram<ColumnType>> row_merged_histogram = histograms[0];
    for (auto i = 1u; i < histograms_size; i++) {
      row_merged_histogram = EqualDistinctCountHistogram<ColumnType>::merge(row_merged_histogram, histograms[i], histogram_bin_count);
    }
  , "histogram_merge_row");
  
  auto full_hist_error = 0.0;
  auto merged_hist_error = 0.0;
  auto merged_row_error = 0.0;
  auto merged_pyramid_error = 0.0;
  int counter = 0;
  for (auto entry: map) {
    auto value = entry.first;
    auto count = entry.second;
    auto estimate = full_hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
    full_hist_error += std::abs(estimate - count);
    auto merged_estimate = merged_hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
    merged_hist_error += std::abs(merged_estimate - count);
    auto merged_row_estimate = row_merged_histogram->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
    merged_row_error += std::abs(merged_row_estimate - count);
    auto merged_pyramid_estimate = merged_hist_pyramid->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
    merged_pyramid_error += std::abs(merged_pyramid_estimate - count);
    // if ((counter < 15)) {
    //   println();
    //   show(value); showln(count);
    //   show(estimate); show(merged_estimate); show(merged_row_estimate); showln(merged_pyramid_estimate);
    //   auto binID = merged_hist_pyramid->_bin_for_value(value);
    //   showln(binID);
    //   show(merged_hist_pyramid->bin_minimum(binID)); showln(merged_hist_pyramid->bin_maximum(binID));
    // }
    counter++;
  }
  showln(table->row_count());
  showln(full_hist_error);
  showln(merged_hist_error);
  showln(merged_row_error);
  showln(merged_pyramid_error);
  show(full_hist->bin_count()); show(merged_hist->bin_count()); show(row_merged_histogram->bin_count()); showln(merged_hist_pyramid->bin_count()); 

  return 0;
}

