#include <iostream>

#include "types.hpp"
#include "utils/load_table.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "import_export/csv_parser.hpp"
#include "operators/import_binary.hpp"
#include "operators/table_scan.hpp"
#include "statistics/table_statistics2.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/histograms/equal_distinct_count_histogram.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/storage_manager.hpp"
#include "sql/sql_pipeline_builder.hpp"

using namespace opossum;  // NOLINT
using namespace opossum::expression_functional;  // NOLINT
using namespace std::string_literals;  // NOLINT

int main() {
  const auto imdb_dir = "../imdb_sample_001/"s;

  StorageManager::get().add_table("movie_companies", ImportBinary::read_binary(imdb_dir + "movie_companies.bin"));
  StorageManager::get().add_table("movie_keyword", ImportBinary::read_binary(imdb_dir + "movie_keyword.bin"));

  const auto movie_companies = StoredTableNode::make("movie_companies");
  const auto movie_keyword = StoredTableNode::make("movie_keyword");

  const auto mc_movie_id = movie_companies->get_column("movie_id");
  const auto mk_movie_id = movie_keyword->get_column("movie_id");
  const auto mc_note = movie_companies->get_column("note");

  const auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(mc_movie_id, mk_movie_id),
    PredicateNode::make(is_null_(mc_note),
      movie_companies),
    movie_keyword);

  std::cout << (*CardinalityEstimator{}.estimate_statistics(lqp)) << std::endl;

  
  return 0;
}
