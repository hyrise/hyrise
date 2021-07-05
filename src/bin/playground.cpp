#include <fstream>
#include <iostream>
#include <optional>

#include "../benchmarklib/tpcds/tpcds_table_generator.hpp"
#include "../lib/import_export/binary/binary_parser.hpp"
#include "../lib/import_export/binary/binary_writer.hpp"
#include "benchmark_config.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"
#include "synthetic_table_generator.hpp"
#include "tasks/dictionary_sharing_task.hpp"

using namespace opossum;  // NOLINT


int main() {
  std::cout << "Playground: Jaccard-Index" << std::endl;

  // Generate benchmark data
  const auto table_path = std::string{"/home/Halil.Goecer/imdb_bin/"};

  const auto table_generator = std::make_unique<FileBasedTableGenerator>(
      std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config()), table_path);
  table_generator->generate_and_store();

  auto output_file_stream = std::ofstream("/home/Halil.Goecer/hyrise/jaccard_index_log.csv", std::ofstream::out | std::ofstream::trunc);

  auto dictionary_sharing_task = DictionarySharingTask{};
  dictionary_sharing_task.do_segment_sharing(std::make_optional<std::ofstream>(std::move(output_file_stream)));
  output_file_stream.close();
  return 0;
}
