#pragma once

#include <cstdint>
#include <memory>

#include "storage/table.hpp"

namespace opossum {

class AbstractModifyingOperator {
 public:
  AbstractModifyingOperator(const std::shared_ptr<const AbstractOperator> & op) : _operator{op}, _succeeded{true} {}

  virtual void execute(const TransactionContext & context) = 0;
  virtual void commit(const uint32_t cid) = 0;
  virtual void abort() = 0;

  bool succeeded() const { return _succeeded; };

  std::shared_ptr<const Table> get_output() const override { return nullptr; };

  virtual const std::string name() const = 0;
  virtual uint8_t num_in_tables() const = 0;

  uint8_t num_out_tables() const override { return 0; };

 protected:
  std::shared_ptr<AbstractOperator> _operator;
  std::vector<RowID> _modified_rows;
  bool _succeeded; // false if transaction needs to be aborted
};

}  // namespace opossum
