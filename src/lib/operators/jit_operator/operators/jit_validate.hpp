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
  void before_specialization(const Table& in_table, std::vector<bool>& tuple_non_nullable_information) override;

  std::string description() const final;

  TableType input_table_type() const;

 protected:
  void _consume(JitRuntimeContext& context) const final;

  TableType _input_table_type = TableType::Data;  // Correct value is set in before_specialization()

 private:
  // Function not optimized due to specialization issues with atomic
  __attribute__((optnone)) static TransactionID _load_atomic_value(
      const copyable_atomic<TransactionID>& transaction_id);
};

}  // namespace opossum
