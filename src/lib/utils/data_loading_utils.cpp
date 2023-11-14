#include "data_loading_utils.hpp"

#include <iostream>
#include <memory>
#include <thread>

#include "hyrise.hpp"
#include "types.hpp"
#include "utils/settings/data_loading_setting.hpp"

namespace {

using namespace hyrise;

void wait_for_column(const std::string& success_log_message) {
  auto& log_manager = Hyrise::get().log_manager;
  auto sleep_time = std::chrono::microseconds{100};
  const auto timeout = std::chrono::seconds{600}.count();

  const auto begin = std::chrono::steady_clock::now();
  while (true) {
    if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - begin).count() > timeout) {
      Fail("Timed out while waiting for column generation (" + std::to_string(timeout) + " s time out).");
    }

    for (const auto& log_entry : log_manager.log_entries()) {
      const auto& message = log_entry.message;
      if (message == success_log_message) {
        return;
      }
    }

    std::this_thread::sleep_for(sleep_time);
    sleep_time *= 2;
  }
}

}  // namespace


namespace hyrise::data_loading_utils {
// namespace hyrise {

void load_column_when_necessary(const std::string& table_name, const ColumnID column_id) {
	static auto request_id = std::atomic<uint32_t>{0};
	if (column_id == INVALID_COLUMN_ID) {
	  // If column_id is not set, we assume that this is not a lazily loaded statistic and we landed here simply
	  // because the attribute statistic misses some statistics (e.g., min/max not built when range filter exists).
	  return;
	}

	Assert(column_id != INVALID_COLUMN_ID, "Cannot lazily create statistics if column ID is not set.");

	// First: check if table is already created.
	auto& log_manager = Hyrise::get().log_manager;
	const auto success_log_message = std::string{"dbgen_success__"} + table_name + "::" + std::to_string(column_id);
	for (const auto& log_entry : log_manager.log_entries()) {
	  const auto& message = log_entry.message;
	  if (message == success_log_message) {
	    return;
	  }
	}

	// Second: check if column has been already requested. We are copying the requests here as settings are not
	// suppposed to be concurrently spammed as we are doing it here.
	// If the column has already been requested, we wait for its construction.
	auto& settings_manager = Hyrise::get().settings_manager;
	const auto setting_names = settings_manager.setting_names();
	const auto request_key_prefix = std::string{"dbgen_request__"} + table_name + "::" + std::to_string(column_id);
	auto column_already_requested = false;
	for (const auto& setting_name : setting_names) {
	  if (setting_name.starts_with(request_key_prefix)) {
	    column_already_requested = true;
	  }
	}

	if (column_already_requested) {
	  // std::printf("Step 2: waiting for column.\n");
	  wait_for_column(success_log_message);
	  return;
	}

	// Third: the columns needs to be requested. We add an according setting and call the DataLoading method to load columns.
	auto& plugin_manager = Hyrise::get().plugin_manager;
	const auto& plugins = plugin_manager.loaded_plugins();
	Assert(std::binary_search(plugins.cbegin(), plugins.cend(), "hyriseDataLoadingPlugin"),
	       "Data Loading plugin is not loaded.");

	const auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	const auto request_key = request_key_prefix + "__" + std::to_string(++request_id) + "_" + std::to_string(timestamp);
	auto setting = std::make_shared<DataLoadingSetting>(request_key);
	setting->register_at_settings_manager();

	Assert(plugin_manager.user_executable_functions().contains({"hyriseDataLoadingPlugin", "LoadTableAndStatistics"}),
	       "Function 'LoadTableAndStatistics' not found.");

	plugin_manager.exec_user_function("hyriseDataLoadingPlugin", "LoadTableAndStatistics");

	wait_for_column(success_log_message);
}

}  // namespace hyrise::data_loading_utils
