#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is a class that mocks the behavior of a meta table.
 * It can be useful in tests as it provides how often methods were called.
 */
class MetaMockTable : public AbstractMetaTable {
 public:
  MetaMockTable();

  static bool can_insert();
  static bool can_remove();
  static bool can_update();

  const std::string& name() const final;

  size_t insert_calls() const;
  size_t remove_calls() const;

 protected:
  std::shared_ptr<Table> _on_generate() const;
  void _on_insert(const std::vector<AllTypeVariant>& values);
  void _on_remove(const std::vector<AllTypeVariant>& values);

  size_t _insert_calls;
  size_t _remove_calls;
};

}  // namespace opossum
