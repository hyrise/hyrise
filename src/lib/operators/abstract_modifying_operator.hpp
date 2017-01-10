#pragma once

#include <cstdint>
#include <memory>

#include "../storage/table.hpp"

namespace opossum {

class AbstractModifyingOperator {
 public:
  AbstractModifyingOperator(std::shared_ptr<Table> table) : _table(table) {}

  virtual void execute(const uint32_t tid) = 0;
  virtual void commit(const uint32_t cid) = 0;
  virtual void abort() = 0;

  virtual std::shared_ptr<const Table> get_output() const { return nullptr; };

  virtual const std::string name() const = 0;
  virtual uint8_t num_in_tables() const = 0;
  virtual uint8_t num_out_tables() const { return 0; };

 protected:
  std::shared_ptr<Table> _table;
  std::vector<RowID> _modified_rows;
};

}  // namespace opossum
