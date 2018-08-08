#include "binary_recovery.hpp"

#include <fstream>

#include "../../operators/insert.hpp"
#include "../../storage/storage_manager.hpp"
#include "../../storage/table.hpp"
#include "../transaction_manager.hpp"
#include "logger.hpp"
#include "types.hpp"

namespace opossum {

BinaryRecovery& BinaryRecovery::getInstance() {
  static BinaryRecovery instance;
  return instance;
}

template <>
std::string BinaryRecovery::_read(std::ifstream& file) {
  std::string result;
  std::getline(file, result, '\0');
  return result;
}

AllTypeVariant BinaryRecovery::_read_AllTypeVariant(std::ifstream& file, DataType data_type) {
  AllTypeVariant value;
  switch (data_type) {
    case DataType::Int: {
      return AllTypeVariant{_read<int32_t>(file)};
      break;
    }
    case DataType::Long: {
      return AllTypeVariant{_read<int64_t>(file)};
      break;
    }
    case DataType::Float: {
      return AllTypeVariant{_read<float>(file)};
      break;
    }
    case DataType::Double: {
      return AllTypeVariant{_read<double>(file)};
      break;
    }
    case DataType::String: {
      return AllTypeVariant{_read<std::string>(file)};
      break;
    }
    default:
      DebugAssert(false, "recovery: read unknown type");
  }

  return value;
}

void BinaryRecovery::recover() {
  TransactionID last_transaction_id{0};
  for (auto& log_path : Logger::get_all_log_file_paths()) {
    std::ifstream log_file(log_path);
    DebugAssert(log_file.is_open(), "Recovery: could not open logfile " + log_path);

    std::vector<LoggedItem> transactions;

    while (true) {
      // Begin of all Entries:
      //   - log entry type       : sizeof(char)
      //   - transaction_id       : sizeof(TransactionID)

      char log_type;
      log_file.read(&log_type, sizeof(char));

      if (log_file.eof()) {
        break;
      }

      DebugAssert(log_type == 't' || log_type == 'i' || log_type == 'v' || log_type == 'l',
                  "Recovery: invalid log type token");

      // if load entry
      if (log_type == 'l') {
        auto table_path = _read<std::string>(log_file);
        auto table_name = _read<std::string>(log_file);

        _recover_table(table_path, table_name);
        continue;
      }

      auto transaction_id = _read<TransactionID>(log_file);

      // if commit entry
      if (log_type == 't') {
        _redo_transactions(transaction_id, transactions);
        last_transaction_id = std::max(transaction_id, last_transaction_id);
        continue;
      }

      // else invalidation or value
      DebugAssert(log_type == 'v' || log_type == 'i', "Recovery: First token of new entry is not handled properly.");

      // Invalidation and begin of value entries:
      //   - log entry type ('v') : sizeof(char)
      //   - transaction_id       : sizeof(transaction_id_t)
      //   - table_name           : table_name.size() + 1, terminated with \0
      //   - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset)

      auto table_name = _read<std::string>(log_file);

      auto chunk_id = _read<ChunkID>(log_file);

      auto chunk_offset = _read<ChunkOffset>(log_file);

      RowID row_id(chunk_id, chunk_offset);

      // if invalidation entry
      if (log_type == 'i') {
        transactions.emplace_back(LoggedItem(LogType::Invalidation, transaction_id, table_name, row_id));
        continue;
      }

      // else value entry

      // Remainder of value entries:
      //   - NULL bitmap          : ceil(values.size() / 8.0)
      //   - value                : length(value)
      //   - any optional values

      auto data_types = StorageManager::get().get_table(table_name)->column_data_types();

      uint16_t null_bitmap_number_of_bytes = ceil(data_types.size() / 8.0);  //  supports 2^16 * 8 > 500,000 values
      std::vector<char> null_bitmap(null_bitmap_number_of_bytes);
      log_file.read(&null_bitmap[0], null_bitmap_number_of_bytes);

      std::vector<AllTypeVariant> values;
      uint16_t bitmap_index = 0;
      uint8_t bit_pos = 0;
      for (auto& data_type : data_types) {
        if (null_bitmap[bitmap_index] & (0b00000001 << bit_pos)) {
          values.emplace_back(NullValue());
        } else {
          values.emplace_back(_read_AllTypeVariant(log_file, data_type));
        }

        bit_pos = (bit_pos + 1) % 8;
        if (bit_pos == 0) {
          ++bitmap_index;
        };
      }

      transactions.emplace_back(LoggedItem(LogType::Value, transaction_id, table_name, row_id, values));

    }  // while not end of file
  }    // for every logfile

  _update_transaction_id(last_transaction_id);
}

}  // namespace opossum
