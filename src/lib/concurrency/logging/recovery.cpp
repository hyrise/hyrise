#include "recovery.hpp"

#include "logger.hpp"
#include "types.hpp"
#include "../../storage/storage_manager.hpp"

#include <fstream>
#include <sstream>

namespace opossum {

Recovery& Recovery::getInstance() {
  static Recovery instance;
  return instance;
}

void Recovery::recover() {
  std::ifstream log_file(Logger::directory + Logger::filename);

  std::string line;
  while (std::getline(log_file, line))
  {
    char log_type = line[1];

    // commit
    if (log_type == 't'){
      TransactionID transaction_id = std::stoull(line.substr(3, line.length() - 4));
      Logger::getInstance().commit(transaction_id);
    } else {  // 'v' or 'i'

      // transaction_id_end  rowID_end
      //     |  rowID_position  |
      //     v        v         v
      // (v,12,table1,RowID(0,25),(25,LAND_O,9001,asdf))
      // (i,86,TABLE2,RowID(....))                  

      auto transaction_id_end = line.find(',', 3) - 1;
      TransactionID transaction_id = std::stoull(line.substr(3, transaction_id_end - 2));

      auto rowID_position = line.find(",RowID(") + 1;
      auto rowID_end = line.find(")", rowID_position);
      // ...,RowID(x,y)...  ->   x,y
      auto rowID_substring = line.substr(rowID_position + 6, rowID_end - rowID_position - 6);
      std::istringstream rowID_stream(rowID_substring); 
      ChunkID chunk_id;
      rowID_stream >> chunk_id;
      rowID_stream.ignore();  // ignore ','
      ChunkOffset chunk_offset;
      rowID_stream >> chunk_offset;
      RowID row_id(chunk_id, chunk_offset);

      std::string table_name = line.substr(transaction_id_end + 2, rowID_position - transaction_id_end - 3);

      if (log_type == 'i'){
        Logger::getInstance().invalidate(transaction_id, table_name, row_id);
        continue;
      }

      // for value inserts 'v' only
      auto data_types = StorageManager::get().get_table(_target_table_name).column_data_types();

    }
  }
}

}  // namespace opossum
