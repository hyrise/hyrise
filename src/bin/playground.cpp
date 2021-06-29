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
  auto table = customers;
  auto column = ColumnID{0};
  auto chunk_count = table->chunk_count();
  std::chrono::steady_clock::time_point begin;
  std::chrono::steady_clock::time_point end;

  showln(chunk_count);
  const auto histogram_bin_count = std::min<size_t>(10, std::max<size_t>(5, table->row_count() / 2'000));
  TIMEIT(auto histograms = std::vector<std::shared_ptr<EqualDistinctCountHistogram<int>>>();
    for(auto i = ChunkID{0}; i < chunk_count; i++) {
      auto hist = EqualDistinctCountHistogram<int>::from_segment(*table, column, ChunkID{i}, histogram_bin_count);
      histograms.push_back(hist);
    }
  , "small_histograms");
  TIMEIT(
    const auto merged_hist = EqualDistinctCountHistogram<int>::merge(histograms, histogram_bin_count);
  , "histogram_merge")
  TIMEIT(
    const auto full_hist = EqualDistinctCountHistogram<int>::from_column(*table, column, histogram_bin_count);
  , "full_histogram");
  auto map = std::unordered_map<int,int>();
  for (auto row : table->get_rows()) {
    auto value = boost::get<int>(row[column]);
    map[value]++;
  }

  auto full_hist_error = 0.0;
  auto merged_hist_error = 0.0;
  int counter = 0;
  for (auto entry: map) {
    auto value = entry.first;
    auto count = entry.second;
    auto estimate = full_hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
    full_hist_error += std::abs(estimate - count);
    auto merged_estimate = merged_hist->estimate_cardinality_and_distinct_count(PredicateCondition::BetweenInclusive, value, value).first;
    merged_hist_error += std::abs(merged_estimate - count);
    if ((counter < 15) || (merged_estimate > 1.0)) {
      show(value); showln(count);
      show(estimate); showln(merged_estimate);
    }
    counter++;
  }
  showln(full_hist_error);
  showln(merged_hist_error);

  return 0;
}

