#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace hyrise {

/**
 * This is a class for calling user executable functions provided by plugins.
 * Inserting plugin and function name calls a function, selecting from the table returns all executable functions.
 */
class MetaExecTable : public AbstractMetaTable {
 public:
  MetaExecTable();

  const std::string& name() const final;

  bool can_insert() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;

  void _on_insert(const std::vector<AllTypeVariant>& values) final;
};

}  // namespace hyrise
