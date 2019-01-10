#pragma once

#include "types.hpp"

namespace opossum {

class MvccDeleteManager {
public:

    // for testing purposes
    static void run_logical_delete(const std::string &tableName, ChunkID chunkID);

private:
    static bool _delete_logically(const std::string &tableName, ChunkID chunkID);

    void _delete_physically(const std::string &tableName, ChunkID chunkID);
};

}  // namespace opossum