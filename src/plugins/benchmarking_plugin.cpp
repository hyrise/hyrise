#include "benchmarking_plugin.hpp"

namespace opossum {

std::string BenchmarkingPlugin::description() const { return "Benchmarking plugin"; }

void BenchmarkingPlugin::start() {
  Hyrise::get().log_manager.add_message(description(), "Initialized!", LogLevel::Info);
  _radix_cache_usage_ratio_setting = std::make_shared<RadixCacheUsageRatioSetting>();
  _radix_cache_usage_ratio_setting->register_at_settings_manager();

  _semi_join_ratio_setting = std::make_shared<SemiJoinRatioSetting>();
  _semi_join_ratio_setting->register_at_settings_manager();
  
}

void BenchmarkingPlugin::stop() {
  _radix_cache_usage_ratio_setting->unregister_at_settings_manager();
  _semi_join_ratio_setting->unregister_at_settings_manager();
}

EXPORT_PLUGIN(BenchmarkingPlugin)

}  // namespace opossum
