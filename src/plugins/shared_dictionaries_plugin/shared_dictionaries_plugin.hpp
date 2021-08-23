#pragma once

#include <gtest/gtest_prod.h>
#include "hyrise.hpp"
#include "shared_dictionaries_plugin/plugin_settings/jaccard_index_threshold_setting.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

template <typename T>
using SegmentChunkPair = std::pair<std::shared_ptr<DictionarySegment<T>>, std::shared_ptr<Chunk>>;

/**
 * The intention of this plugin is to save memory by using "dictionary sharing" (see below)
 * while trying to not decrease the performance of Hyrise.
 *
 * Per default every dictionary encoded segment in Hyrise has its own dictionary.
 * This plugin compares the dictionaries within a column for their similarity
 * using the jaccard-index. If the jaccard-index is equal or higher than the
 * specified threshold and additionally if the merging does not increase the width
 * of the attribute vector, this plugin creates a shared dictionary. Then, the plugin
 * replaces the dictionary segment with a new dictionary segment that has a shared
 * dictionary.
 *
 * With the start of the plugin, the dictionary sharing compressor is automatically
 * started for every table in the database.
 */
class SharedDictionariesPlugin : public AbstractPlugin {
 public:
  inline static const std::string LOG_NAME = "SharedDictionariesPlugin";

  struct SharedDictionariesStats {
    uint64_t total_bytes_saved = 0;
    uint64_t total_previous_bytes = 0;
    uint64_t modified_previous_bytes = 0;
    uint32_t num_merged_dictionaries = 0;
    uint32_t num_shared_dictionaries = 0;
    uint32_t num_existing_merged_dictionaries = 0;
    uint32_t num_existing_shared_dictionaries = 0;
  };

  SharedDictionariesPlugin();

  std::string description() const final;

  void start() final;

  void stop() final;

  SharedDictionariesStats stats = {};

 private:
  StorageManager& _storage_manager;
  LogManager& _log_manager;

  std::shared_ptr<JaccardIndexThresholdSetting> _jaccard_index_threshold_setting;

  void _process_for_every_column();

  void _log_plugin_configuration() const;
  void _log_processing_result() const;
};

}  // namespace opossum
