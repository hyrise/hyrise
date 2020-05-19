#pragma once

#include <memory>

#include <boost/algorithm/string/classification.hpp>
#include <boost/range/algorithm_ext/erase.hpp>

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
                                                  ChunkOffset target_chunk_size = Chunk::DEFAULT_SIZE) {
    const auto table = load_table(path, target_chunk_size);

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

void assert_chunk_encoding(const std::shared_ptr<Chunk>& chunk, const ChunkEncodingSpec& spec);

inline std::string all_segment_encoding_specs_formatter(
    const testing::TestParamInfo<EncodingTest::ParamType>& param_info) {
  std::stringstream stringstream;
  stringstream << param_info.param;
  auto string = stringstream.str();
  boost::remove_erase_if(string, boost::is_any_of("() -"));
  return string;
}

}  // namespace opossum
