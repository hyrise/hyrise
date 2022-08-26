#include "physical_config.hpp"

namespace hyrise {

constexpr SegmentPhysicalConfig::SegmentPhysicalConfig()
    : encoding_spec{SegmentEncodingSpec{}}, tier{AbstractSegment::Tier::Memory}, sorted{false} {}

constexpr SegmentPhysicalConfig::SegmentPhysicalConfig(const SegmentEncodingSpec& init_encoding_spec,
                                             const AbstractSegment::Tier init_tier, const bool init_sorted)
    : encoding_spec{init_encoding_spec}, tier{init_tier}, sorted{init_sorted} {}

PhysicalConfig::PhysicalConfig() : fallback_config{SegmentPhysicalConfig{}} {}

PhysicalConfig::PhysicalConfig(const SegmentPhysicalConfig& init_fallback_config)
    : fallback_config{init_fallback_config} {}

PhysicalConfig::PhysicalConfig(const SegmentPhysicalConfig& init_fallback_config,
                               TableSegmentConfigMapping&& init_config_mapping)
    : fallback_config{init_fallback_config}, config_mapping{init_config_mapping} {}

const SegmentPhysicalConfig& PhysicalConfig::config_for_segment(const std::string& table_name,
                                                                const std::string& column_name,
                                                                const ChunkID chunk_id) const {
  for (const auto& [config_chunk_id, config] : configs_for_column(table_name, column_name)) {
    if (config_chunk_id == chunk_id) {
      return config;
    }
  }

  // fallback if segment not found
  return fallback_config;
}

const std::vector<std::pair<ChunkID, SegmentPhysicalConfig>>& PhysicalConfig::configs_for_column(
    const std::string& table_name, const std::string& column_name) const {
  static const auto empty_configs = std::vector<std::pair<ChunkID, SegmentPhysicalConfig>>{};

  const auto& table_mapping_it = config_mapping.find(table_name);
  // fallback if table not found
  if (table_mapping_it == config_mapping.cend()) {
    return empty_configs;
  }

  const auto& table_config_mapping = table_mapping_it->second;
  const auto& column_mapping_it = table_config_mapping.find(column_name);
  // fallback if column not found
  if (column_mapping_it == table_config_mapping.cend()) {
    return empty_configs;
  }

  return column_mapping_it->second;
}

nlohmann::json PhysicalConfig::to_json() const {
  // TODO
  return {};
}

}  // namespace hyrise
