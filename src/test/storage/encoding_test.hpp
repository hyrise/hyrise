#pragma once

#include <memory>

#include "base_test.hpp"

#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"

namespace opossum {

// Base Class for tests that should be run with various encodings
class EncodingTest : public BaseTestWithParam<SegmentEncodingSpec> {
 public:
  std::shared_ptr<Table> load_table_with_encoding(const std::string& path,
                                                  ChunkOffset max_chunk_size = Chunk::MAX_SIZE) {
    const auto table = load_table(path, max_chunk_size);

    auto chunk_encoding_spec = ChunkEncodingSpec{table->column_count(), EncodingType::Unencoded};

    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      if (encoding_supports_data_type(GetParam().encoding_type, table->column_data_type(column_id))) {
        chunk_encoding_spec[column_id] = GetParam();
      }
    }

    ChunkEncoder::encode_all_chunks(table, chunk_encoding_spec);
    return table;
  }
};

}  // namespace opossum
