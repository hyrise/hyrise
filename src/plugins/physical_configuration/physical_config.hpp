#pragma once

#include "nlohmann/json.hpp"

#include "storage/abstract_segment.hpp"
#include "storage/encoding_type.hpp"

namespace hyrise {

struct SegmentPhysicalConfig {
  constexpr SegmentPhysicalConfig();
  constexpr SegmentPhysicalConfig(const SegmentEncodingSpec& init_encoding_spec, const AbstractSegment::Tier init_tier,
                        const bool init_sorted);
  SegmentEncodingSpec encoding_spec;
  AbstractSegment::Tier tier;
  bool sorted;
};

// Map<TABLE_NAME, Map<COLUMN_NAME, Vector<Pair<ChunkID, SegmentPhysicalConfig>>>>
using TableSegmentConfigMapping =
    std::unordered_map<std::string,
                       std::unordered_map<std::string, std::vector<std::pair<ChunkID, SegmentPhysicalConfig>>>>;

class PhysicalConfig : public Noncopyable {
 public:
  PhysicalConfig();
  PhysicalConfig(const SegmentPhysicalConfig& init_fallback_config);
  PhysicalConfig(const SegmentPhysicalConfig& init_fallback_config, TableSegmentConfigMapping&& init_config_mapping);

  const SegmentPhysicalConfig& config_for_segment(const std::string& table_name, const std::string& column_name,
                                                  const ChunkID chunk_id) const;

  const std::vector<std::pair<ChunkID, SegmentPhysicalConfig>>& configs_for_column(
      const std::string& table_name, const std::string& column_name) const;

  const SegmentPhysicalConfig fallback_config;
  const TableSegmentConfigMapping config_mapping;

  nlohmann::json to_json() const;
};

}  // namespace hyrise
