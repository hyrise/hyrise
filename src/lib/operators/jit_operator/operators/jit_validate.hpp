#pragma once

#include "abstract_jittable.hpp" // NEEDEDINCLUDE
#include "utils/copyable_atomic.hpp"
#include "types.hpp" // NEEDEDINCLUDE

namespace opossum {

/* The JitValidate operator validates visibility of tuples
 * within the context of a given transaction
 */
class JitValidate : public AbstractJittable {
 public:
  explicit JitValidate(const TableType input_table_type = TableType::Data);

  std::string description() const final;

  void set_input_table_type(const TableType input_table_type);

 protected:
  void _consume(JitRuntimeContext& context) const final;

 private:
  // Function not optimized due to specialization issues with atomic
  __attribute__((optnone)) static TransactionID _load_atomic_value(
      const copyable_atomic<TransactionID>& transaction_id);

  TableType _input_table_type;
};

}  // namespace opossum
