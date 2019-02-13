#include <iostream>

#include "types.hpp"
#include "utils/load_table.hpp"
#include "import_export/csv_parser.hpp"
#include "operators/table_scan.hpp"
#include "statistics/histograms/equal_distinct_count_histogram.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/storage_manager.hpp"
#include "sql/sql_pipeline_builder.hpp"

using namespace opossum;  // NOLINT

int main() {
  const auto movie_companies = CsvParser{}.parse("imdb_sample_001/movie_companies.csv", std::nullopt, 100'000);
  const auto histogram = EqualDistinctCountHistogram<std::string>::from_segment(movie_companies->get_chunk(ChunkID{0})->get_segment(ColumnID{4}), 50, StringHistogramDomain{});

  std::cout << histogram->description(true) << std::endl;

  StorageManager::get().add_table("movie_companies", movie_companies);
  
  std::cout << SQLPipelineBuilder{"SELECT * FROM movie_companies WHERE note IS NULL"};
  
  return 0;
}
