#include "data_loading_utils.hpp"

#include <iostream>
#include <memory>
#include <thread>

#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/task_queue.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/settings/data_loading_setting.hpp"

namespace {

using namespace hyrise;

float get_scale_factor() {
  const auto* env_scale_factor = std::getenv("SCALE_FACTOR");
  Assert(env_scale_factor, "This branch required environment variable 'SCALE_FACTOR' to be set.");
  return std::strtof(env_scale_factor, nullptr);

  // return 0.6f;
}

void wait_for_column(const std::string& success_log_message) {
  const auto scale_factor = get_scale_factor();
  auto& log_manager = Hyrise::get().log_manager;
  auto sleep_time = std::chrono::microseconds{50};
  // const auto timeout = std::chrono::seconds{static_cast<size_t>(60 * scale_factor)}.count();
  const auto timeout = std::chrono::seconds{static_cast<size_t>(10 * scale_factor)}.count();

  const auto begin = std::chrono::system_clock::now();
  while (true) {
    // if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - begin).count() > std::chrono::seconds{1}.count()) {
    //  std::cerr << std::format("Waiting for '{}' for {} s.\n", success_log_message,
    //                           static_cast<size_t>(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - begin).count()));
    // }
    if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - begin).count() > timeout) {
      auto sstream = std::stringstream{};
      sstream << "I was looking for success_log_message >> " << success_log_message << " <<, but failed (sleeptime: " << static_cast<size_t>(sleep_time.count()) << " us).\n";
      std::time_t t0 = std::chrono::system_clock::to_time_t(begin);
      sstream << "Begin: " << std::put_time(std::localtime(&t0), "%FT%T%z");
      std::time_t t1 = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      sstream << " - Now: " << std::put_time(std::localtime(&t1), "%FT%T%z") << "\n";
      sstream << "Log entries:\n";
      const auto log_entries = log_manager.log_entries();
    for (const auto& log_entry : log_entries) {
        sstream << "\t" << log_entry.message << "\n";
      }

      auto& settings_manager = Hyrise::get().settings_manager;
      sstream << "Settings:\n";
      for (const auto& setting_name : settings_manager.setting_names()) {
        sstream << "\t" << setting_name << "\n";
      }

      // const auto& node_queue_scheduler = std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler());
      // std::cerr << "Scheduler load:\n";
      // for (const auto& queue : node_queue_scheduler->queues()) {
      //   std::cout << "Q: " << queue->estimate_load() << "\n";
      // }
      Fail("Timed out while waiting for column generation (" + std::to_string(timeout) + " s time out).\n\n" + sstream.str() + "\n\n");
    }

    const auto log_entries = log_manager.log_entries();
    for (const auto& log_entry : log_entries) {
      const auto& message = log_entry.message;
      if (message == success_log_message) {
        return;
      }
    }

    std::this_thread::sleep_for(std::min(std::chrono::microseconds{10'000}, sleep_time));
    sleep_time *= 2;
  }
}

}  // namespace


namespace hyrise::data_loading_utils {

void load_column_when_necessary(const std::string& table_name, const ColumnID column_id, const bool allow_wait) {
  static auto request_id = std::atomic<uint32_t>{0};

  Assert(column_id != INVALID_COLUMN_ID, "Cannot lazily create statistics if columnID is not set.");

  // First: check if table is already created.
  auto& log_manager = Hyrise::get().log_manager;
  const auto success_log_message = std::string{"dbgen_success__"} + table_name + "::" + std::to_string(column_id) + "__";
  const auto log_entries = log_manager.log_entries();
  for (const auto& log_entry : log_entries) {
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
  const auto request_key_prefix = std::string{"dbgen_request__"} + table_name + "::" + std::to_string(column_id) + "__";
  auto column_already_requested = false;
  for (const auto& setting_name : setting_names) {
    if (setting_name.starts_with(request_key_prefix)) {
      column_already_requested = true;
    }
  }

  if (column_already_requested) {
    if (allow_wait) {
      wait_for_column(success_log_message);
    }
    return;
  }

  // Third: the columns needs to be requested. We add an according setting and call the DataLoading method to load columns.
  auto& plugin_manager = Hyrise::get().plugin_manager;
  const auto& plugins = plugin_manager.loaded_plugins();
  Assert(std::binary_search(plugins.cbegin(), plugins.cend(), "hyriseDataLoadingPlugin"),
         "Data Loading plugin is not loaded.");

  const auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  const auto request_key = request_key_prefix + std::to_string(++request_id) + "_" + std::to_string(timestamp);
  auto setting = std::make_shared<DataLoadingSetting>(request_key);
  setting->register_at_settings_manager();

  Assert(plugin_manager.user_executable_functions().contains({"hyriseDataLoadingPlugin", "LoadTableAndStatistics"}),
         "Function 'LoadTableAndStatistics' not found.");

  plugin_manager.exec_user_function("hyriseDataLoadingPlugin", "LoadTableAndStatistics");

  if (allow_wait) {
    wait_for_column(success_log_message);
  }
}

void wait_for_table(const std::string& success_log_message) {
  const auto scale_factor = get_scale_factor();

  auto& log_manager = Hyrise::get().log_manager;
  auto sleep_time = std::chrono::microseconds{100};
  const auto timeout = std::chrono::seconds{static_cast<size_t>(100 * scale_factor)}.count();

  const auto begin = std::chrono::system_clock::now();
  while (true) {
    if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - begin).count() > timeout) {
      auto sstream = std::stringstream{};
      sstream << "I was looking for success_log_message >> " << success_log_message << " <<, but failed with a sleeptime now of " << sleep_time.count() << ").\n";
      sstream << "Log entries\n";
      const auto log_entries = log_manager.log_entries();
      for (const auto& log_entry : log_entries) {
        sstream << "\t" << log_entry.message << "\n";
      }

      auto& settings_manager = Hyrise::get().settings_manager;
      sstream << "Settings:\n";
      for (const auto& setting_name : settings_manager.setting_names()) {
        sstream << "\t" << setting_name << "\n";
      }
      Fail("Timed out while waiting for table generation (" + std::to_string(timeout) + " s time out).\n\n" + sstream.str() + "\n\n");
    }

    const auto log_entries = log_manager.log_entries();
    for (const auto& log_entry : log_entries) {
      const auto& message = log_entry.message;
      if (message.starts_with(success_log_message)) {
        return;
      }
    }

    std::this_thread::sleep_for(std::min(std::chrono::microseconds{1'000}, sleep_time));
    sleep_time *= 2;
  }
}

}  // namespace hyrise::data_loading_utils
