#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

class HyriseEnvironmentRef;

/**
 * This is a class for showing all stored chunks via a meta table.
 */
class MetaChunksTable : public AbstractMetaTable {
 public:
  explicit MetaChunksTable(const std::shared_ptr<HyriseEnvironmentRef>& hyrise_env);

  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;

  const std::shared_ptr<HyriseEnvironmentRef> _hyrise_env;
};

}  // namespace opossum
