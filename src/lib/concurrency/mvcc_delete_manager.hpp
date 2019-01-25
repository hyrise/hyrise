#pragma once

#include <queue>
#include <string>

#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class MvccDelete {
 public:
  static bool delete_chunk_logically(const std::string& table_name, ChunkID chunk_id);
  static bool delete_chunk_physically(const std::string& table_name, ChunkID chunk_id);

 protected:
 private:
  static std::shared_ptr<const Table> _get_referencing_table(const std::string& table_name, ChunkID chunk_id);
};

}  // namespace opossum
