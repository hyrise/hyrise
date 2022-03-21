#include <iostream>

#include <boost/algorithm/string.hpp>

#include "benchmark_config.hpp"
#include "hyrise.hpp"
#include "operators/import.hpp"
#include "operators/export.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_translator.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "types.hpp"
#include "visualization/lqp_visualizer.hpp"
#include "visualization/pqp_visualizer.hpp"
#include "import_export/binary/binary_writer.hpp"
#include "import_export/binary/binary_parser.hpp"

using namespace opossum;  // NOLINT

int main() {

  const std::unordered_map<std::string, std::string> sort_tables = {
      {"web_sales", "ws_sold_date_sk"},
      {"store_sales", "ss_sold_date_sk"}};

  const std::string table_path = "/home/Daniel.Lindner/hyrise/tpcds_cached_tables/sf-10/";
  const std::string out_path = "/home/Daniel.Lindner/hyrise/tpcds_cached_tables/sf-10-sort/";
  const std::string file_extension = ".bin";

  for (const auto& [table_name, sort_column] : sort_tables) {
    const auto file_path = table_path + table_name + file_extension;
    std::cout << "load " << file_path << std::endl;
    auto table = BinaryParser::parse(file_path);
    std::cout << "sort" << std::endl;
    auto table_wrapper = std::make_shared<TableWrapper>(table);
    const auto sort_column_id = table->column_id_by_name(sort_column);
    //auto sort_definition = SortColumnDefinition{sort_column_id};
    auto sort_definitions = std::vector<SortColumnDefinition>{SortColumnDefinition{sort_column_id}};
    auto sort = std::make_shared<Sort>(table_wrapper, sort_definitions, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes);
    table_wrapper->execute();
    sort->execute();
    std::cout << "encode" << std::endl;
    auto sorted_table = std::const_pointer_cast<Table>(sort->get_output());
    if (sorted_table->last_chunk()->is_mutable()) {sorted_table->last_chunk()->finalize();}
    ChunkEncoder::encode_all_chunks(sorted_table);
    std::cout << "export" << std::endl;
    const auto output_path = out_path + table_name + file_extension;
    BinaryWriter::write(*sorted_table, output_path);

  }

  /*
  const std::unordered_map<std::string, std::vector<std::string>> table_keys = {
      {"catalog_sales", {"cs_item_sk", "cs_order_number"}},
      {"date_dim", {"d_date_sk"}},
      {"customer_demographics", {"cd_demo_sk"}}};
  const size_t num_union_tuples = 1000;
  const size_t num_repetitions = 100;

  std::string measurement_table = "customer_demographics";
  for (const auto& [table_name, key_columns] : table_keys) {
    const auto file_path = table_path + table_name + file_extension;
    std::cout << "load " << file_path << std::endl;
    const auto importer = std::make_shared<Import>(file_path, table_name);
    importer->execute();

    //if (table_name != measurement_table) {
      std::cout << " - add keys " << boost::algorithm::join(key_columns, ", ") << std::endl;
      const auto table = Hyrise::get().storage_manager.get_table(table_name);
      std::unordered_set<ColumnID> columns;
      for (const auto& column : key_columns) {
        columns.emplace(table->column_id_by_name(column));
      }
      table->add_soft_key_constraint({columns, KeyConstraintType::PRIMARY_KEY});
    //}
  }

  Hyrise::get().dependency_usage_config = std::make_shared<DependencyUsageConfig>();
  auto pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  auto lqp_cache = std::make_shared<SQLLogicalPlanCache>();

  // auto table_generator = std::make_unique<TPCDSTableGenerator>(10, std::make_shared<config>);
  // table_generator->generate_and_store();
  // const auto inv_table = Hyrise::get().storage_manager.get_table("inventory");
  // std::cout << inv_table->last_chunk()->size() << std::endl;

  std::vector<std::chrono::nanoseconds> runtimes;
  runtimes.reserve(num_repetitions);
  // const std::string query = "SELECT mk.movie_id FROM keyword AS k, movie_keyword AS mk WHERE k.keyword IS NOT"
  //                           "NULL AND k.keyword IN ('murder', 'murder-in-title', 'blood', 'violence')"
  //                           "AND k.id = mk.keyword_id";
  const std::string query =
      "SELECT cs_item_sk FROM   catalog_sales, customer_demographics WHERE cs_bill_cdemo_sk = cd_demo_sk  AND "
      "cd_gender = 'F'  AND cd_marital_status = 'W'  AND cd_education_status = 'Secondary'";

  std::cout << std::endl << "JoinToSemi OFF" << std::endl;

  std::cout << std::endl << query << std::endl;
  for (size_t current_repetition = 0; current_repetition <= num_repetitions; ++current_repetition) {
    auto pipeline = SQLPipelineBuilder{query}.with_pqp_cache(pqp_cache).with_lqp_cache(lqp_cache).create_pipeline();
    pipeline.get_result_table();
    // const auto pqps = pipeline.get_physical_plans();
    const auto metrics = pipeline.metrics();
    // std::cout << metrics << std::endl << format_duration(metrics.statement_metrics.at(0)->plan_execution_duration)
    //           << std::endl;
    if (current_repetition == 0) {
      std::cout << metrics << std::endl;
      continue;
    }

    runtimes.push_back(metrics.statement_metrics.at(0)->plan_execution_duration);

    if (current_repetition == num_repetitions) {
      GraphvizConfig graphviz_config;
      graphviz_config.format = "svg";
      const auto& pqps = pipeline.get_physical_plans();
      std::string suffix = "_jts_off";
      PQPVisualizer{graphviz_config, {}, {}, {}}.visualize(pqps, "PQP" + suffix + ".svg");
    }
  }
  std::chrono::nanoseconds sum;
  for (const auto& runtime : runtimes) {
    sum += runtime;
  }
  auto avg = sum / std::chrono::nanoseconds(runtimes.size());
  std::cout << avg << std::endl;
  std::cout << format_duration(sum) << std::endl;
  std::cout << static_cast<double>(avg) / 1'000'000.0 << std::endl;

  std::cout << std::endl << std::endl << "JoinToSemi ON" << std::endl;
  runtimes.clear();
  pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  lqp_cache = std::make_shared<SQLLogicalPlanCache>();

  std::cout << " -  enable JTS" << std::endl;
  auto dependency_config = std::make_shared<DependencyUsageConfig>();
  dependency_config->enable_join_to_semi = true;
  Hyrise::get().dependency_usage_config = dependency_config;

  const auto m_table = Hyrise::get().storage_manager.get_table(measurement_table);
  //const auto measurement_table_keys = table_keys.at(measurement_table);
  //std::cout << " - add keys " << boost::algorithm::join(measurement_table_keys, ", ") << std::endl;
  //std::unordered_set<ColumnID> columns;
  //for (const auto& column : measurement_table_keys) {
  //  columns.emplace(m_table->column_id_by_name(column));
  //}
  //m_table->add_soft_key_constraint({columns, KeyConstraintType::PRIMARY_KEY});


  std::cout << std::endl << query << std::endl;
  for (size_t current_repetition = 0; current_repetition <= num_repetitions; ++current_repetition) {
    auto pipeline = SQLPipelineBuilder{query}.with_pqp_cache(pqp_cache).with_lqp_cache(lqp_cache).create_pipeline();
    pipeline.get_result_table();
    const auto metrics = pipeline.metrics();
    if (current_repetition == 0) {
      std::cout << metrics << std::endl;
      continue;
    }

    runtimes.push_back(metrics.statement_metrics.at(0)->plan_execution_duration);

    if (current_repetition == num_repetitions) {
      GraphvizConfig graphviz_config;
      graphviz_config.format = "svg";
      const auto& pqps = pipeline.get_physical_plans();
      std::string suffix = "_jts_on";
      PQPVisualizer{graphviz_config, {}, {}, {}}.visualize(pqps, "PQP" + suffix + ".svg");
    }
  }
  std::chrono::nanoseconds sum2;
  for (const auto& runtime : runtimes) {
    sum2 += runtime;
  }
  avg = sum2 / std::chrono::nanoseconds(runtimes.size());
  std::cout << avg << std::endl;
  std::cout << format_duration(sum2) << std::endl;
  std::cout << static_cast<double>(avg) / 1'000'000.0 << std::endl;

  std::cout << std::endl << std::endl << "UNION" << std::endl;
  runtimes.clear();
  pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  lqp_cache = std::make_shared<SQLLogicalPlanCache>();


  const auto new_table =
      std::make_shared<Table>(m_table->column_definitions(), TableType::Data, Chunk::DEFAULT_SIZE, UseMvcc::Yes);
  auto num_qualifying_tuples = static_cast<int32_t>(std::round((27'440.0 / 1'920'800.0) * num_union_tuples));
  std::cout << num_qualifying_tuples << " qualifying tuples" << std::endl;

  for (int32_t i = 0; i < static_cast<int32_t>(num_union_tuples); ++i) {
    pmr_string gender = i < num_qualifying_tuples ? "F" : "M";
    std::vector<AllTypeVariant> row = {
        i,          gender,     pmr_string{"W"}, pmr_string{"Secondary"}, int32_t{500}, pmr_string{"Good"},
        int32_t{0}, int32_t{0}, int32_t{0}};
    new_table->append(row);
  }

  // const auto new_table_mutable = std::const_pointer_cast<Table>(new_table);
  new_table->last_chunk()->finalize();
  ChunkEncoder::encode_all_chunks(new_table);
  const auto new_table_name = measurement_table + "_new";
  std::cout << "add " << new_table_name << " with " << new_table->chunk_count() << " chunks and "
            << new_table->row_count() << " rows" << std::endl;
  Hyrise::get().storage_manager.add_table(new_table_name, new_table);

  // const std::string query2 = query + " UNION ALL SELECT cs_item_sk FROM   catalog_sales, "
  //                           + new_table_name + " WHERE cs_bill_cdemo_sk = cd_demo_sk  AND cd_gender = 'F'  AND"
  //                           "cd_marital_status = 'W'  AND cd_education_status = 'Secondary'";
  const std::string query2 = "SELECT cs_item_sk FROM   catalog_sales, " + new_table_name +
                             " WHERE cs_bill_cdemo_sk = cd_demo_sk  AND cd_gender = 'F'  AND cd_marital_status = 'W'  "
                             "AND cd_education_status = 'Secondary'";

  std::cout << std::endl << query2 << std::endl;
  for (size_t current_repetition = 0; current_repetition <= num_repetitions; ++current_repetition) {
    auto pipeline = SQLPipelineBuilder{query2}.with_pqp_cache(pqp_cache).with_lqp_cache(lqp_cache).create_pipeline();
    pipeline.get_result_table();
    const auto metrics = pipeline.metrics();
    if (current_repetition == 0) {
      std::cout << metrics << std::endl;
      continue;
    }

    runtimes.push_back(metrics.statement_metrics.at(0)->plan_execution_duration);

    if (current_repetition == num_repetitions) {
      GraphvizConfig graphviz_config;
      graphviz_config.format = "svg";
      const auto& pqps = pipeline.get_physical_plans();
      std::string suffix = "_union";
      PQPVisualizer{graphviz_config, {}, {}, {}}.visualize(pqps, "PQP" + suffix + ".svg");
    }
  }
  std::chrono::nanoseconds sum3;
  for (const auto& runtime : runtimes) {
    sum3 += runtime;
  }
  avg = sum3 / std::chrono::nanoseconds(runtimes.size());
  std::cout << avg << std::endl;
  std::cout << format_duration(sum3) << std::endl;
  std::cout << static_cast<double>(avg) / 1'000'000.0 << std::endl;


  std::cout << std::endl << std::endl << "UNION + diP" << std::endl;
  runtimes.clear();
  pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  lqp_cache = std::make_shared<SQLLogicalPlanCache>();

  const std::string query3 = "SELECT cs_item_sk FROM   catalog_sales, " + new_table_name +
                             " WHERE cs_bill_cdemo_sk = cd_demo_sk  AND cd_gender = 'F'  AND cd_marital_status = 'W'  "
                             "AND cd_education_status = 'Secondary' and catalog_sales.cs_bill_cdemo_sk BETWEEN 0 AND " +
                             std::to_string(num_qualifying_tuples);

  std::cout << std::endl << query3 << std::endl;
  for (size_t current_repetition = 0; current_repetition <= num_repetitions; ++current_repetition) {
    auto pipeline = SQLPipelineBuilder{query3}.with_pqp_cache(pqp_cache).with_lqp_cache(lqp_cache).create_pipeline();
    pipeline.get_result_table();
    const auto metrics = pipeline.metrics();
    if (current_repetition == 0) {
      std::cout << metrics << std::endl;
      continue;
    }

    runtimes.push_back(metrics.statement_metrics.at(0)->plan_execution_duration);

    if (current_repetition == num_repetitions) {
      GraphvizConfig graphviz_config;
      graphviz_config.format = "svg";
      const auto& pqps = pipeline.get_physical_plans();
      std::string suffix = "_union_dip";
      PQPVisualizer{graphviz_config, {}, {}, {}}.visualize(pqps, "PQP" + suffix + ".svg");
    }
  }
  std::chrono::nanoseconds sum4;
  for (const auto& runtime : runtimes) {
    sum4 += runtime;
  }
  avg = sum4 / std::chrono::nanoseconds(runtimes.size());
  std::cout << avg << std::endl;
  std::cout << format_duration(sum4) << std::endl;
  std::cout << static_cast<double>(avg) / 1'000'000.0 << std::endl;
  */

  return 0;
}
