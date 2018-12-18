#pragma once

#include "types.hpp"

namespace opossum {

class MvccDeleteManager {
public:

    // for testing purposes
    void run_logical_delete(const std::string &tableName, ChunkID chunkID);

private:
    void _delete_logically(const std::string &tableName, const ChunkID chunkID);

    void _delete_physically(const std::string &tableName, const ChunkID chunkID);
};

}  // namespace opossum