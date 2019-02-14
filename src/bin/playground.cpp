#include <iostream>

#include "types.hpp"
#include "utils/load_table.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
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
  const auto imdb_dir = "../imdb_sample_01/"s;

  StorageManager::get().add_table("movie_companies", ImportBinary::read_binary(imdb_dir + "movie_companies.bin"));
  StorageManager::get().add_table("company_type", ImportBinary::read_binary(imdb_dir + "company_type.bin"));
//  StorageManager::get().add_table("movie_keyword", ImportBinary::read_binary(imdb_dir + "movie_keyword.bin"));

//  StorageManager::get().add_table("movie_companies", CsvParser{}.parse(imdb_dir + "movie_companies.csv"));
//  StorageManager::get().add_table("movie_keyword", CsvParser{}.parse(imdb_dir + "movie_keyword.csv"));

  const auto movie_companies = StoredTableNode::make("movie_companies");
  const auto company_type = StoredTableNode::make("company_type");
//  const auto movie_keyword = StoredTableNode::make("movie_keyword");

  const auto mc_movie_id = movie_companies->get_column("movie_id");
  const auto mc_company_type_id = movie_companies->get_column("company_type_id");
  const auto mc_note = movie_companies->get_column("note");

  const auto ct_kind = company_type->get_column("kind");
  const auto ct_id = company_type->get_column("id");
//  const auto mk_movie_id = movie_keyword->get_column("movie_id");

  const auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(mc_company_type_id, ct_id),
    PredicateNode::make(is_not_null_(ct_kind),
      PredicateNode::make(not_equals_(ct_kind, "production companies"),
        ProjectionNode::make(expression_vector(ct_kind, ct_id), company_type))),
    PredicateNode::make(is_not_null_(mc_note),
      ProjectionNode::make(expression_vector(mc_note, mc_company_type_id), movie_companies)));
//
//  std::cout << (*CardinalityEstimator{}.estimate_statistics(lqp)) << std::endl;
//  std::cout << (*CardinalityEstimator{}.estimate_statistics(lqp->left_input())) << std::endl;
//  std::cout << (*CardinalityEstimator{}.estimate_statistics(lqp->left_input()->left_input())) << std::endl;
  std::cout << (*CardinalityEstimator{}.estimate_statistics(lqp->right_input())) << std::endl;
  std::cout << (*CardinalityEstimator{}.estimate_statistics(lqp->right_input()->left_input())) << std::endl;

  
  return 0;
}
