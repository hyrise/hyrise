#include "shared_dictionaries_plugin.hpp"
#include "resolve_type.hpp"
#include "shared_dictionaries_column_processor.hpp"
#include "shared_dictionaries_plugin/plugin_settings/jaccard_index_threshold_setting.hpp"

namespace opossum {

SharedDictionariesPlugin::SharedDictionariesPlugin()
    : _storage_manager(Hyrise::get().storage_manager),
      _log_manager(Hyrise::get().log_manager),
      _jaccard_index_threshold_setting(std::make_shared<JaccardIndexThresholdSetting>()) {
  _jaccard_index_threshold_setting->register_at_settings_manager();
}

std::string SharedDictionariesPlugin::description() const { return "Shared dictionaries plugin"; }

void SharedDictionariesPlugin::start() {
  stats = SharedDictionariesStats();

  _log_plugin_configuration();
  _process_for_every_column();
  _log_processing_result();
}

void SharedDictionariesPlugin::stop() { _jaccard_index_threshold_setting->unregister_at_settings_manager(); }

void SharedDictionariesPlugin::_process_for_every_column() {
  _log_manager.add_message(LOG_NAME, "Starting creation of shared dictionaries", LogLevel::Info);
  const auto jaccard_index_threshold = std::stod(_jaccard_index_threshold_setting->get());

  auto table_names = _storage_manager.table_names();
  std::sort(table_names.begin(), table_names.end());
  for (const auto& table_name : table_names) {
    {
      const auto log_message = "> creating shared dictionaries for table: " + table_name;
      _log_manager.add_message(LOG_NAME, log_message, LogLevel::Debug);
    }
    const auto table = _storage_manager.get_table(table_name);
    const auto column_count = table->column_count();

    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      const auto column_data_type = table->column_definitions()[column_id].data_type;
      const auto column_name = table->column_definitions()[column_id].name;
      {
        const auto log_message = "  - creating shared dictionaries for column: " + column_name;
        _log_manager.add_message(LOG_NAME, log_message, LogLevel::Debug);
      }
      resolve_data_type(column_data_type, [&](const auto type) {
        using ColumnDataType = typename decltype(type)::type;
        auto column_processor = SharedDictionariesColumnProcessor<ColumnDataType>{
            table, table_name, column_id, column_name, jaccard_index_threshold, stats};
        column_processor.process();
      });
    }
  }

  _log_manager.add_message(LOG_NAME, "Completed creation of shared dictionaries", LogLevel::Info);
}

void SharedDictionariesPlugin::_log_plugin_configuration() const {
  auto log_stream = std::stringstream();
  log_stream << "Plugin configuration:" << std::endl
             << "  - jaccard-index threshold = " << _jaccard_index_threshold_setting->get();
  _log_manager.add_message(LOG_NAME, log_stream.str(), LogLevel::Debug);
}

void SharedDictionariesPlugin::_log_processing_result() const {
  const auto total_save_percentage =
      stats.total_previous_bytes == 0
          ? 0.0
          : (static_cast<double>(stats.total_bytes_saved) / static_cast<double>(stats.total_previous_bytes)) * 100.0;
  const auto modified_save_percentage =
      stats.modified_previous_bytes == 0
          ? 0.0
          : (static_cast<double>(stats.total_bytes_saved) / static_cast<double>(stats.modified_previous_bytes)) * 100.0;

  auto log_stream = std::stringstream();
  log_stream << "Merged " << stats.num_merged_dictionaries << " dictionaries down to " << stats.num_shared_dictionaries
             << " shared dictionaries" << std::endl;
  log_stream << "Found " << stats.num_existing_shared_dictionaries << " existing shared dictionaries used in "
             << stats.num_existing_merged_dictionaries << " dictionary encoded segments" << std::endl;
  log_stream << "Saved " << stats.total_bytes_saved << " bytes (" << std::ceil(modified_save_percentage)
             << "% of modified, " << std::ceil(total_save_percentage) << "% of total)";
  _log_manager.add_message(LOG_NAME, log_stream.str(), LogLevel::Debug);
}

EXPORT_PLUGIN(SharedDictionariesPlugin)

}  // namespace opossum
