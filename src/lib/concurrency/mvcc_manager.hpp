#pragma once

#include "types.hpp"

namespace opossum {

class MvccManager {
public:

    void testDeleteChunkLogically(const std::string &tableName, ChunkID chunkID);

private:
    void deleteChunkLogically(const std::string &tableName, const ChunkID chunkID);

    void deleteChunkPhysically(const std::string &tableName, const ChunkID chunkID);
};

}  // namespace opossum