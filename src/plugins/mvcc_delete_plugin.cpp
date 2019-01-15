#include "mvcc_delete_plugin.hpp"

#include "storage/table.hpp"

namespace opossum {

const std::string MvccDeletePlugin::description() const { return "This is the Hyrise TestPlugin"; }

void MvccDeletePlugin::start() {
  for (const auto& table : sm.tables()) {
    const auto& chunks = table.second->chunks();

    for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunks.size(); chunk_id++) {
      const auto& chunk = chunks[chunk_id];

      if (chunk) {
        const double invalid_row_amount = static_cast<double>(chunk->invalid_row_count()) / chunk->size();
        if (invalid_row_amount == 1) {
          // TODO(anyone) Implement update if e.g. 0.9 < invalid_row_amaount < 1

          table.second->delete_chunk(chunk_id);
        }
      }
    }
  }
}

void MvccDeletePlugin::stop() {
  //TODO: Implement if necessary
}

EXPORT_PLUGIN(MvccDeletePlugin)

}  // namespace opossum
