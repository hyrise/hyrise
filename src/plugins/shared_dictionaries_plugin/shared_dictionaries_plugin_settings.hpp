#pragma once

#include "hyrise.hpp"

namespace opossum {

/*
 * The following settings are currently hard coded for the plugin but could be made configurable:
 *   - option to disable auto start of compression
 *     The plugin currently starts the merging of dictionaries automatically once it is loaded.
 *   - option to disable tracking of compression statistics
 *     The plugin currently tracks memory usage statistics for merged data.
 *   - option to select all tables / include tables / exclude tables
 *     The plugin currently tries to merge dictionary segments of all available tables.
 */

/**
 * Threshold for the similarity metric between dictionaries for merging
 */
class JaccardIndexThresholdSetting : public AbstractSetting {
 public:
  JaccardIndexThresholdSetting() : AbstractSetting("Plugin::SharedDictionaries.jaccard_index_threshold") {}
  const std::string& description() const;
  const std::string& display_name() const;
  const std::string& get();
  void set(const std::string& value);

  std::string _value = "0.1";
  std::string _display_name = "Jaccard Index Threshold (0.0 - 1.0)";
};

}  // namespace opossum
