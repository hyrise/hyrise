#pragma once

#include "abstract_log_formatter.hpp"

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

/*
 *  Log formatter that writes log entries as binary.
 */

class BinaryLogFormatter final : public AbstractLogFormatter {
 public:
  std::vector<char> commit_entry(const TransactionID transaction_id) final;

  std::vector<char> value_entry(const TransactionID transaction_id, const std::string& table_name, const RowID row_id,
                                const std::vector<AllTypeVariant>& values) final;

  std::vector<char> create_invalidation_entry(const TransactionID transaction_id, const std::string& table_name,
                                     const RowID row_id) final;

  std::vector<char> load_table_entry(const std::string& file_path, const std::string& table_name) final;

  AbstractRecoverer& get_recoverer() final;

  // returns the number of bytes needed for the null bitmap
  static uint32_t null_bitmap_size(uint32_t number_of_values);

 private:
  friend class Logger;
  BinaryLogFormatter() = default;
};

}  // namespace opossum
