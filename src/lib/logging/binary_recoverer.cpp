#include "binary_recoverer.hpp"

#include <fstream>

#include "binary_log_formatter.hpp"
#include "concurrency/transaction_manager.hpp"
#include "logged_item.hpp"
#include "logger.hpp"
#include "operators/insert.hpp"
#include "resolve_type.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
T BinaryRecoverer::_read(std::ifstream& file) {
  T result;
  file.read(reinterpret_cast<char*>(&result), sizeof(T));
  return result;
}

template <>
std::string BinaryRecoverer::_read(std::ifstream& file) {
  std::string result;
  std::getline(file, result, '\0');
  return result;
}

template <>
RowID BinaryRecoverer::_read(std::ifstream& file) {
  auto chunk_id = _read<ChunkID>(file);
  auto chunk_offset = _read<ChunkOffset>(file);
  return RowID(chunk_id, chunk_offset);
}

AllTypeVariant BinaryRecoverer::_read_all_type_variant(std::ifstream& file, DataType data_type) {
  AllTypeVariant value;

  resolve_data_type(data_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    value = AllTypeVariant{_read<ColumnDataType>(file)};
  });

  return value;
}

uint32_t BinaryRecoverer::recover() {
  for (auto& log_path : Logger::get_all_log_file_paths()) {
    std::ifstream log_file(log_path);
    DebugAssert(log_file.is_open(), "Recoverer: could not open logfile " + log_path);

    std::map<TransactionID, std::vector<std::unique_ptr<LoggedItem>>> transactions;

    auto log_type = _read<char>(log_file);

    while (!log_file.eof()) {
      switch (log_type) {
        // load entry
        case 'l': {
          auto table_path = _read<std::string>(log_file);
          auto table_name = _read<std::string>(log_file);
          _redo_load_table(table_path, table_name);
          break;
        }

        // commit entry
        case 'c': {
          auto transaction_id = _read<TransactionID>(log_file);
          _redo_transaction(transactions, transaction_id);
          break;
        }

        // invalidation entry
        case 'i': {
          auto transaction_id = _read<TransactionID>(log_file);
          auto table_name = _read<std::string>(log_file);
          auto row_id = _read<RowID>(log_file);
          transactions[transaction_id].emplace_back(
              std::make_unique<LoggedInvalidation>(LoggedInvalidation(transaction_id, table_name, row_id)));
          break;
        }

        // value entry
        case 'v': {
          auto transaction_id = _read<TransactionID>(log_file);
          auto table_name = _read<std::string>(log_file);
          auto row_id = _read<RowID>(log_file);

          auto data_types = StorageManager::get().get_table(table_name)->column_data_types();
          auto null_bitmap_number_of_bytes = BinaryLogFormatter::null_bitmap_size(data_types.size());
          std::vector<char> null_bitmap(null_bitmap_number_of_bytes);
          log_file.read(&null_bitmap[0], null_bitmap_number_of_bytes);

          std::vector<AllTypeVariant> values;
          uint32_t bitmap_index = 0;
          uint8_t bit_pos = 0;
          for (auto& data_type : data_types) {
            if (null_bitmap[bitmap_index] & (1u << bit_pos)) {
              values.emplace_back(NullValue());
            } else {
              values.emplace_back(_read_all_type_variant(log_file, data_type));
            }

            bit_pos = (bit_pos + 1) % 8;
            if (bit_pos == 0) {
              ++bitmap_index;
            }
          }

          transactions[transaction_id].emplace_back(
              std::make_unique<LoggedValue>(LoggedValue(transaction_id, table_name, row_id, values)));
          break;
        }

        default:
          throw("Binary recovery: invalid log type token");
      }

      log_file.read(&log_type, sizeof(char));
    }
  }

  return _number_of_loaded_tables;
}

}  // namespace opossum
