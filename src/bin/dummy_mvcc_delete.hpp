#include <queue>

#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class DummyMvccDelete {

 public:
  explicit DummyMvccDelete(double threshold) : DELETE_THRESHOLD(threshold), sm(StorageManager::get()) {}

  void start();

 private:
  struct ChunkSpecifier {
    std::string table_name;
    ChunkID chunk_id;
    ChunkSpecifier(std::string table_name, ChunkID chunk_id) : table_name(std::move(table_name)), chunk_id(chunk_id) { }
  };

  void _clean_up_chunk(const std::string &table_name, ChunkID chunk_id);

  static bool _delete_chunk_logically(const std::string& table_name, ChunkID chunk_id);
  static bool _delete_chunk_physically(const std::string& table_name, ChunkID chunk_id);
  void _process_physical_delete_queue();
  static std::shared_ptr<const Table> _get_referencing_table(const std::string& table_name, ChunkID chunk_id);


  double DELETE_THRESHOLD;
  StorageManager& sm;
  std::queue<ChunkSpecifier> _physical_delete_queue;
};

}  // namespace opossum
