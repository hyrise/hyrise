#include "recovery.hpp"

#include "logger.hpp"
#include "types.hpp"

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
    } else if (log_type == 'v'){
      continue;
    } else if (log_type == 'i'){
      // (i,1234123,...)
      auto transaction_id_end = line.find(',', 3) - 1;
      TransactionID transaction_id = std::stoull(line.substr(3, transaction_id_end - 2));

      auto rowID_position = line.rfind("RowID(");
      // ...,RowID(x,y))  ->   x,y
      auto rowID_substring = line.substr(rowID_position + 6, line.length() - rowID_position - 8);
      std::istringstream rowID_stream(rowID_substring); 
      ChunkID chunk_id;
      rowID_stream >> chunk_id;
      rowID_stream.ignore();  // ignore ','
      ChunkOffset chunk_offset;
      rowID_stream >> chunk_offset;
      RowID row_id(chunk_id, chunk_offset);

      // (i,2424,TABLE_NAME,ROW(...))
      std::string table_name = line.substr(transaction_id_end + 2, rowID_position - 3 - transaction_id_end);

      Logger::getInstance().invalidate(transaction_id, table_name, row_id);
    }
  }
}

}  // namespace opossum
