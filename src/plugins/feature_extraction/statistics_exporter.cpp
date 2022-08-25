#include "statistics_exporter.hpp"

#include "hyrise.hpp"
#include "import_export/csv/csv_writer.hpp"

namespace hyrise {

StatisticsExporter::StatisticsExporter()
    : _sub_directory{std::make_shared<SubDirectory>("StatisticsExporter.SubDirectory")} {
  _sub_directory->register_at_settings_manager();
}

StatisticsExporter::~StatisticsExporter() {
  _sub_directory->unregister_at_settings_manager();
}

void StatisticsExporter::export_statistics(const std::string& file_path) {
  const auto sub_directory = file_path + _sub_directory->get();
  std::cout << "export data statistics to " << sub_directory << std::endl;
  std::filesystem::create_directories(sub_directory);
  const auto statistics_meta_tables = {"tables", "chunks", "chunk_sort_orders", "columns", "segments_accurate"};
  const auto& meta_table_manager = Hyrise::get().meta_table_manager;

  for (const auto& meta_table_name : statistics_meta_tables) {
    const auto& meta_table = meta_table_manager.generate_table(meta_table_name);
    CsvWriter::write(*meta_table, sub_directory + "/" + meta_table_name + ".csv");
  }
}

StatisticsExporter::SubDirectory::SubDirectory(const std::string& init_name) : AbstractSetting(init_name) {}

const std::string& StatisticsExporter::SubDirectory::description() const {
  static const auto description = std::string{"Output file for the data statistics"};
  return description;
}

const std::string& StatisticsExporter::SubDirectory::get() {
  return _value;
}

void StatisticsExporter::SubDirectory::set(const std::string& value) {
  _value = value;
}

}  // namespace hyrise
