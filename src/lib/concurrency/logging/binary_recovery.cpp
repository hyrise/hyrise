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

// read datatype from file
AllTypeVariant _read(std::ifstream& file, DataType data_type) {
  AllTypeVariant value;
  switch (data_type) {
    case DataType::Int: {
      int32_t v;
      file.read(reinterpret_cast<char*>(&v), sizeof(int32_t));
      value = v;
      break;
    }
    case DataType::Long: {
      int64_t v;
      file.read(reinterpret_cast<char*>(&v), sizeof(int64_t));
      value = v;
      break;
    }
    case DataType::Float: {
      float v;
      file.read(reinterpret_cast<char*>(&v), sizeof(float));
      value = v;
      break;
    }
    case DataType::Double: {
      double v;
      file.read(reinterpret_cast<char*>(&v), sizeof(double));
      value = v;
      break;
    }
    case DataType::String: {
      std::string v;
      std::getline(file, v, '\0');
      value = v;
      break;
    }
    default:
      DebugAssert(false, "recovery: read unknown type");
  }

  return value;
}

void BinaryRecovery::recover() {
  TransactionID last_transaction_id{0};
  for (auto& path: Logger::get_all_log_file_paths()) {
    std::ifstream log_file(path);
    DebugAssert(log_file.is_open(), "Recovery: could not open logfile " + path);

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

      TransactionID transaction_id;
      log_file.read(reinterpret_cast<char*>(&transaction_id), sizeof(TransactionID));

      // if commit entry
      if (log_type == 't') {
        _redo_transactions(transaction_id, transactions);
        last_transaction_id = std::max(transaction_id, last_transaction_id);
        continue;
      } 

      // else invalidation or value
      DebugAssert(log_type == 'v' || log_type == 'i', "recovery: first token of new entry is neither t, v nor i");
      
      // Invalidation and begin of value entries:
      //   - log entry type ('v') : sizeof(char)
      //   - transaction_id       : sizeof(transaction_id_t)
      //   - table_name           : table_name.size() + 1, terminated with \0
      //   - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset)

      std::string table_name;
      std::getline(log_file, table_name, '\0');

      ChunkID chunk_id;
      log_file.read(reinterpret_cast<char*>(&chunk_id), sizeof(ChunkID));

      ChunkOffset chunk_offset;
      log_file.read(reinterpret_cast<char*>(&chunk_offset), sizeof(ChunkOffset));

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

      uint16_t null_bitmap_number_of_bytes = ceil(data_types.size() / 8.0); //  supports 2^16 * 8 > 500,000 values
      std::vector<char> null_bitmap(null_bitmap_number_of_bytes);
      log_file.read(&null_bitmap[0], null_bitmap_number_of_bytes);

      std::vector<AllTypeVariant> values;
      uint16_t bitmap_index = 0;
      uint8_t bit_pos = 0;
      for (auto& data_type : data_types) {
        if ((null_bitmap[bitmap_index] << bit_pos) & 0b10000000) {
          values.emplace_back(NullValue());
        } else {
          values.emplace_back(_read(log_file, data_type));
        }

        bit_pos = (bit_pos + 1) % 8;
        if (bit_pos == 0) { 
          ++bitmap_index;
        };
      }

      transactions.emplace_back(LoggedItem(LogType::Value, transaction_id, table_name, row_id, values));

    }  // while not end of file
  }  // for every logfile

  _update_transaction_id(last_transaction_id);
}

}  // namespace opossum
