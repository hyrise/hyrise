#include <iostream>
#include<fstream>

#include "hyrise.hpp"
#include "optimizer/optimizer.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "utils/load_table.hpp"
#include "utils/format_duration.hpp"

using namespace opossum;  // NOLINT

constexpr auto TBL_FILE = "../../data/10mio_pings_int.tbl";
constexpr auto WORKLOAD_FILE = "../../data/workload_sql.csv";
constexpr auto CONFIG_PATH = "../../data/config";
constexpr auto CHUNK_SIZE = size_t{100'000};
constexpr auto TABLE = "PING";

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

std::shared_ptr<Table> get_table(const std::string tbl_file, const size_t chunk_size, const std::string config_file) {
  // load initial table 
  auto table = load_table(tbl_file, chunk_size);
  // load configuration
  auto conf = read_file(config_file);

  for(const auto& line : conf) {
     std::cout << line[1] << std::endl;
  }

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

int main() {
	auto& storage_manager = Hyrise::get().storage_manager;

  const auto queries = get_queries(WORKLOAD_FILE);

  for (const auto& entry : std::filesystem::directory_iterator(CONFIG_PATH)) {
    const auto conf_path = entry.path();
    const auto conf_name = conf_path.stem();
    const auto filename = conf_path.filename().string();

    // check that file name is csv file
    if (filename.find(".csv") == std::string::npos) {
      std::cout << "Skipping " << conf_path << std::endl;
      continue;
    }

    std::cout << "Benchmark for configuration: " << conf_name  << std::endl;
    
    const auto table = get_table(TBL_FILE, CHUNK_SIZE, conf_path);
    // Add table to storage manager create statistics.
    storage_manager.add_table(TABLE, table);

    for (auto const& query : queries) {
      build_and_execute_query(query);
    }

    storage_manager.drop_table(TABLE);
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
