#pragma once

#include "hyrise.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/settings/abstract_setting.hpp"

namespace opossum {

class BenchmarkingPlugin : public AbstractPlugin {
 public:
  std::string description() const final;
  void start() final;
  void stop() final;

 private:
 	class RadixCacheUsageRatioSetting : public AbstractSetting {
   public:
    RadixCacheUsageRatioSetting() : AbstractSetting("Plugin::Benchmarking::RadixCacheUsageRatio") {}
    const std::string& description() const final {
      static const auto description = std::string{"Float in [0,1] denoting how much of the L2 cache is used"};
      return description;
    }
    const std::string& get() final { return _value; }
    void set(const std::string& value) final { _value = value; }

    std::string _value = "0.75";
  };

  class SemiJoinRatioSetting : public AbstractSetting {
   public:
    SemiJoinRatioSetting() : AbstractSetting("Plugin::Benchmarking::SemiJoinRatio") {}
    const std::string& description() const final {
      static const auto description = std::string{"Float in [0,1] denoting how much smaller the semi join probe data structures are."};
      return description;
    }
    const std::string& get() final { return _value; }
    void set(const std::string& value) final { _value = value; }

    std::string _value = "0.1";
  };

  std::shared_ptr<RadixCacheUsageRatioSetting> _radix_cache_usage_ratio_setting;
 	std::shared_ptr<SemiJoinRatioSetting> _semi_join_ratio_setting;
};

}  // namespace opossum
