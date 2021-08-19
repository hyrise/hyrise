#pragma once

#include "hyrise.hpp"

namespace opossum {

/**
 * Threshold for the similarity metric between dictionaries for merging
 */
class JaccardIndexThresholdSetting : public AbstractSetting {
 public:
  JaccardIndexThresholdSetting() : AbstractSetting("Plugin::SharedDictionaries.jaccard_index_threshold") {}
  const std::string& description() const final;
  const std::string& display_name() const;
  const std::string& get() final;
  void set(const std::string& value_string) final;

  std::string _value = "0.4";
  std::string _display_name = "Jaccard Index Threshold (0.0 - 1.0)";
};

}  // namespace opossum
