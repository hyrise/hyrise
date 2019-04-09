#pragma once

#include "abstract_jittable.hpp"
#include "concurrency/transaction_context.hpp"
#include "types.hpp"

namespace opossum {

/* The JitValidate operator validates visibility of tuples
 * within the context of a given transaction
 */
class JitValidate : public AbstractJittable {
 public:
  explicit JitValidate(const TableType input_table_type = TableType::Data);

  void before_specialization(const Table& in_table) override;

  std::string description() const final;

  TableType input_table_type;

 protected:
  void _consume(JitRuntimeContext& context) const final;

 private:
  // Function not optimized due to specialization issues with atomic
  __attribute__((optnone)) static TransactionID _load_atomic_value(
      const copyable_atomic<TransactionID>& transaction_id);
};

}  // namespace opossum
