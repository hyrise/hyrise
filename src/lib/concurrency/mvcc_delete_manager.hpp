#pragma once

#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class MvccDeleteManager {
 public:
  // for testing purposes
  static void run_logical_delete(const std::string& table_name, ChunkID chunk_id);

 private:
  static bool _delete_logically(const std::string& table_name, ChunkID chunk_id);
  static void _delete_physically(const std::string& tableName, ChunkID chunkID);
  static std::shared_ptr<const Table> _get_referencing_table(const std::string& table_name, ChunkID chunk_id);
};

}  // namespace opossum
