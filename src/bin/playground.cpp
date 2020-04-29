#include <iostream>
#include <fstream>

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/print.hpp"
#include "optimizer/optimizer.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "optimizer/strategy/column_pruning_rule.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "storage/chunk_encoder.hpp"
#include "utils/load_table.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

using namespace opossum::expression_functional;  // NOLINT

using namespace opossum;  // NOLINT

constexpr auto TBL_FILE = "../../data/10mio_pings_int.tbl";
constexpr auto WORKLOAD_FILE_SQL = "../../data/workload_sql.csv";
constexpr auto WORKLOAD_FILE_CSV = "../../data/workload.csv";
constexpr auto CONFIG_PATH = "../../data/config";
constexpr auto CHUNK_SIZE = size_t{100'000};
constexpr auto TABLE_NAME = "PING";
constexpr auto TABLE_NAME_FOR_COPYING = "PING_ORIGINAL";

// returns a vector with all lines of the file
std::vector<std::vector<std::string>> read_file(const std::string file) {
  std::ifstream f(file);
  std::string line;
  std::vector<std::vector<std::string>> file_values;

  std::string header;
  std::getline(f, header);

  while (std::getline(f, line)){
    std::vector<std::string> line_values;
    std::istringstream linestream(line);
    std::string value;

    while (std::getline(linestream, value, ',')){
     line_values.push_back(value);
    }

    file_values.push_back(line_values);
  }

  return file_values;  
} 

// returns all queries of a given workload file 
std::vector<std::string> get_queries(const std::string workload_file) {
  auto workload = read_file(workload_file);
  std::vector<std::string> queries;

  for(auto const& value: workload) {
    queries.push_back(value[1]);
  }

  return queries;
}

/**
 * This function takes a file path to a CSV file containing the workload and returns the queries in form of LQP-based
 * queries together with their frequency. The reason to use LQPs is that we can later selectively apply optimizer rules
 * (most importantly the chunk pruning rule).
 * Alternatives are SQL-based queries, which either do not use the optimizer (no chunk pruning) or the complete
 * optimizer (we lose control over the predicate order), or creating PQPs (we would need to manually prune chunks).
 */
std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, size_t>> load_queries_from_csv(const std::string workload_file) {
  std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, size_t>> output_queries;

  const auto csv_lines = read_file(workload_file);

  auto previous_query_id = int64_t{-1};
  auto previous_predicate_selectivity = 17.0;
  std::shared_ptr<AbstractLQPNode> current_node;

  
  const auto& table = Hyrise::get().storage_manager.get_table(TABLE_NAME);
  const auto column_names = table->column_names();
  auto stored_table_node = StoredTableNode::make(TABLE_NAME);
  std::shared_ptr<AbstractLQPNode> previous_node = stored_table_node;
  auto query_frequency = size_t{0};
  for (const auto& csv_line : csv_lines) {
    const auto query_id = std::stol(csv_line[0]);
    const auto predicate_str = csv_line[1];
    const auto column_id = ColumnID{static_cast<uint16_t>(std::stoi(csv_line[2]))};
    const auto predicate_selectivity = stof(csv_line[3]);
    query_frequency = stoul(csv_line[5]);
    const auto search_value_0 = stoi(csv_line[6]);
    const auto search_value_1 = csv_line[7] != "" ? std::stoi(csv_line[7]) : int{-1};

    Assert(query_id >= previous_query_id,
           "Queries are expected to be sorted ascendingly by: query ID ascendingly.");
    Assert(query_id != previous_query_id || predicate_selectivity >= previous_predicate_selectivity,
           "Queries are expected to be sorted ascendingly by: query ID ascendingly & selectivity descendingly.");

    if (query_id > 0 && query_id != previous_query_id) {
      output_queries.emplace_back(current_node, query_frequency);
      stored_table_node = StoredTableNode::make(TABLE_NAME);
      previous_node = stored_table_node;
    }

    const auto lqp_column = stored_table_node->get_column(column_names[column_id]);
    if (predicate_str == "Between") {
      Assert(search_value_1 > -1, "Between predicate with missing second search value.");
      current_node = PredicateNode::make(between_inclusive_(lqp_column, search_value_0, search_value_1), previous_node);
    } else if (predicate_str == "LessThanEquals") {
      current_node = PredicateNode::make(less_than_equals_(lqp_column, search_value_0), previous_node);
    }

    previous_query_id = query_id;
    previous_predicate_selectivity = predicate_selectivity;
    previous_node = current_node;
  }
  output_queries.emplace_back(current_node, query_frequency);  // Store last query

  return output_queries;
}

std::shared_ptr<Table> get_table(const std::string tbl_file, const size_t chunk_size) {
  auto table = load_table(tbl_file, chunk_size);
  return table;
}

void build_and_execute_query(const std::string query) {
	const auto optimizer = Optimizer::create_default_optimizer();
  std::cout << query << std::endl;
	auto sql_pipeline = SQLPipelineBuilder{query}.with_optimizer(optimizer).disable_mvcc().create_pipeline_statement();

	sql_pipeline.get_result_table();
  const auto duration = sql_pipeline.metrics()->plan_execution_duration;
  std::cout << "Execution took " << format_duration(duration) << std::endl;
  std::cout << *sql_pipeline.get_physical_plan() << std::endl;
}

/**
 * Takes a pair of an LQP-based query and the frequency, partially optimizes the query (only chunk and column pruning
 * for now), translates the query, and executes the query (single-threaded).
 */
void partially_optimize_translate_and_execute_query(const std::pair<std::shared_ptr<AbstractLQPNode>, size_t>& workoad_item) {
  constexpr auto EXECUTION_COUNT = 17;
  const auto lqp_query = workoad_item.first;
  const auto frequency = workoad_item.second;

  // Run chunk and column pruning rules. Kept it quite simple for now. Take a look at Optimizer::optimize() in case
  // problems occur. The following code is taken from optimizer.cpp. In case the new root is confusing to you, take a
  // look there.
  const auto root_node = LogicalPlanRootNode::make(std::move(lqp_query));

  const auto chunk_pruning_rule = ChunkPruningRule();
  chunk_pruning_rule.apply_to(root_node);

  const auto column_pruning_rule = ColumnPruningRule();
  column_pruning_rule.apply_to(root_node);

  // Remove LogicalPlanRootNode
  const auto optimized_node = root_node->left_input();
  root_node->set_left_input(nullptr);

  std::cout << "Executing the following query " << EXECUTION_COUNT << " times (workload frequency " << frequency << "):\n" << *optimized_node << std::endl;
  std::vector<size_t> runtimes;
  for (auto count = size_t{0}; count < EXECUTION_COUNT; ++count) {
    Timer timer;
    const auto pqp = LQPTranslator{}.translate_node(optimized_node);
    const auto tasks = OperatorTask::make_tasks_from_operator(pqp);
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
    const auto runtime = static_cast<size_t>(timer.lap().count());
    runtimes.push_back(runtime);
  }

  auto runtime_accumulated = size_t{0};
  for (const auto runtime : runtimes) {
    runtime_accumulated += runtime;
  }

  std::cout << std::fixed << "Execution took in average " << static_cast<float>(runtime_accumulated) / static_cast<float>(runtimes.size()) << " ns" << std::endl;
}

int main() {
	auto& storage_manager = Hyrise::get().storage_manager;
  std::cout << "Loading PING table as PING_ORIGINAL for later copying ... ";
  Timer load_timer;
  const auto original_table = load_table(TBL_FILE, CHUNK_SIZE);
  Hyrise::get().storage_manager.add_table(TABLE_NAME_FOR_COPYING, original_table);
  std::cout << "done (" << format_duration(load_timer.lap()) << ")" << std::endl;

  const auto queries_sql = get_queries(WORKLOAD_FILE_SQL);

  for (const auto& entry : std::filesystem::directory_iterator(CONFIG_PATH)) {
    const auto conf_path = entry.path();
    const auto conf_name = conf_path.stem();
    const auto filename = conf_path.filename().string();

    // Create a copy of the PING table that is used for the actual benchmarking.
    Timer copy_timer;
    std::cout << "Creating new PING table ... ";
    auto context = Hyrise::get().transaction_manager.new_transaction_context();
    auto sql_pipeline = SQLPipelineBuilder{std::string{"CREATE TABLE "} + TABLE_NAME + " AS SELECT * FROM " + TABLE_NAME_FOR_COPYING}.with_transaction_context(context).create_pipeline_statement();
    sql_pipeline.get_result_table();
    context->commit();
    std::cout << " done (" << format_duration(copy_timer.lap()) << ")" << std::endl;

    // check that file name is csv file
    if (filename.find(".csv") == std::string::npos) {
      std::cout << "Skipping " << conf_path << std::endl;
      continue;
    }
    std::cout << "Benchmark for configuration: " << conf_name  << std::endl;

    Timer preparation_timer;
    std::cout << "Preparing table (encoding, sorting, ...) with given configuration: " << conf_name << " ... ";
    const auto& table = Hyrise::get().storage_manager.get_table(TABLE_NAME);
    const auto chunk_count = table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);
      chunk->finalize();
    }
    // TODO(krichly): encoding and stuff here
    ChunkEncoder::encode_all_chunks(Hyrise::get().storage_manager.get_table(TABLE_NAME));
    std::cout << " done (" << format_duration(preparation_timer.lap()) << ")" << std::endl;

    // We load queries here, as the construction of the queries needs the existing actual table
    const auto queries_csv = load_queries_from_csv(WORKLOAD_FILE_CSV);

    for (auto const& query : queries_sql) {
      build_and_execute_query(query);
    }

    for (auto const& query : queries_csv) {
      partially_optimize_translate_and_execute_query(query);
    }

    storage_manager.drop_table(TABLE_NAME);
  }
  /**
   *	Die folgenden Lines sind aus meinem Compression Playground. Ich lese alle vorhandenen Configs aus dem path und
   *	vermesse dafür nach und nach den TPC-H. Die CSV wird einfach line pro line gelesen und nach Kommata gesplittet.
   *	Nicht schön, aber ausreichend. :)
   */

  // for (const auto& entry : std::filesystem::directory_iterator(path)) {
  //   const auto conf_path = entry.path();
  //   const auto conf_name = conf_path.stem();
  //   const auto filename = conf_path.filename().string();

  //   if (filename.find("conf") != 0 || filename.find(".json") != std::string::npos) {
  //     std::cout << "Skipping " << conf_path << std::endl;
  //     continue;
  //   }

  //   std::cout << "Benchmarking " << conf_name << " ..." << std::endl;

  //   {
  //     auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  //     config->max_runs = 10;
  //     config->enable_visualization = false;
  //     config->output_file_path = conf_name.string() + ".json";
  //     config->chunk_size = 100'000;
  //     config->cache_binary_tables = true;

  //     auto context = BenchmarkRunner::create_context(*config);

  //     std::ifstream configuration_file(entry.path().string());
  //     std::string line;
  //     std::cout << "Reencoding: " << std::flush;
  //     while (std::getline(configuration_file, line))
  //     {
  //       std::vector<std::string> line_values;
  //       std::istringstream linestream(line);
  //       std::string value;
  //       while (std::getline(linestream, value, ','))
  //       {
  //         line_values.push_back(value);
  //       }

  //       const auto table_name = line_values[0];
  //       const auto column_name = line_values[1];
  //       const auto chunk_id = std::stoi(line_values[2]);
  //       const auto encoding_type_str = line_values[3];
  //       const auto vector_compression_type_str = line_values[4];

  //       const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  //       const auto& chunk = table->get_chunk(ChunkID{static_cast<uint32_t>(chunk_id)});
  //       const auto& column_id = table->column_id_by_name(column_name);
  //       const auto& segment = chunk->get_segment(column_id);
  //       const auto& data_type = table->column_data_type(column_id);

  //       const auto encoding_type = encoding_type_to_string.right.at(encoding_type_str);

  //       SegmentEncodingSpec spec = {encoding_type};
  //       if (vector_compression_type_str != "None") {
  //         const auto vector_compression_type = vector_compression_type_to_string.right.at(vector_compression_type_str);
  //         spec.vector_compression_type = vector_compression_type;
  //       }

  //       const auto& encoded_segment = ChunkEncoder::encode_segment(segment, data_type, spec);
  //       chunk->replace_segment(column_id, encoded_segment);
  //       std::cout << "." << std::flush;
  //     }
  //     std::cout << " done." << std::endl;
  //     configuration_file.close();
  //     // const std::vector<BenchmarkItemID> tpch_query_ids_benchmark = {BenchmarkItemID{1}};
}
