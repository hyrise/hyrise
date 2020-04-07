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

  bool can_insert() const;
  bool can_delete() const;
  bool can_update() const;

  const std::string& name() const final;

  size_t insert_calls() const;
  size_t remove_calls() const;
  size_t update_calls() const;

  const std::vector<AllTypeVariant> insert_values() const;
  const std::vector<AllTypeVariant> remove_values() const;
  const std::vector<AllTypeVariant> update_selected_values() const;
  const std::vector<AllTypeVariant> update_updated_values() const;

 protected:
  std::shared_ptr<Table> _on_generate() const;
  void _on_insert(const std::vector<AllTypeVariant>& values);
  void _on_remove(const std::vector<AllTypeVariant>& values);
  void _on_update(const std::vector<AllTypeVariant>& selected_values, const std::vector<AllTypeVariant>& update_values);

  size_t _insert_calls;
  size_t _remove_calls;
  size_t _update_calls;

  std::vector<AllTypeVariant> _insert_values;
  std::vector<AllTypeVariant> _remove_values;
  std::vector<AllTypeVariant> _update_selected_values;
  std::vector<AllTypeVariant> _update_updated_values;
};

}  // namespace opossum
