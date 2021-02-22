#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

class HyriseEnvironmentRef;

/**
 * This is a class for showing information of all stored segments via a meta table.
 * Here, we do not provide the distinct value count per segment.
 * For slower results with that information use MetaSegmentsAccurateTable.
 */
class MetaSegmentsTable : public AbstractMetaTable {
 public:
  explicit MetaSegmentsTable(const std::shared_ptr<HyriseEnvironmentRef>& hyrise_env);

  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;

  const std::shared_ptr<HyriseEnvironmentRef> _hyrise_env;
};

}  // namespace opossum
