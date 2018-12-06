#pragma once

#include "types.hpp"
#include "storage/chunk.hpp"
#include "encoding_config.hpp"

namespace opossum {

class AbstractTableGenerator {
 public:
  void generate_and_store(const EncodingConfig& encoding_config, const ChunkOffset chunk_size = Chunk::DEFAULT_SIZE)
};

}
