#include "dictionary_compression.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "chunk.hpp"
#include "dictionary_column.hpp"
#include "resolve_type.hpp"
#include "table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_column.hpp"

#include "null_suppression/ns_utils.hpp"

namespace opossum {

class ColumnCompressorBase {
 public:
  virtual std::shared_ptr<BaseColumn> compress_column(const std::shared_ptr<BaseColumn>& column) = 0;

 protected:
  static std::unique_ptr<BaseNsEncoder> _create_ns_encoder(size_t unique_values_count, size_t size) {
    if (unique_values_count <= std::numeric_limits<uint8_t>::max()) {
      return create_encoder_from_ns_type(NsType::FixedSize8ByteAligned);
    } else if (unique_values_count <= std::numeric_limits<uint16_t>::max()) {
      return create_encoder_from_ns_type(NsType::FixedSize16ByteAligned);
    } else {
      return create_encoder_from_ns_type(NsType::FixedSize32ByteAligned);
    }
  }
};

template <typename T>
class ColumnCompressor : public ColumnCompressorBase {
 public:
  std::shared_ptr<BaseColumn> compress_column(const std::shared_ptr<BaseColumn>& column) override {
    auto value_column = std::dynamic_pointer_cast<const ValueColumn<T>>(column);

    Assert(value_column != nullptr, "Column is either already compressed or type mismatches.");

    // See: https://goo.gl/MCM5rr
    // Create dictionary (enforce uniqueness and sorting)
    const auto& values = value_column->values();
    auto dictionary = pmr_vector<T>{values.cbegin(), values.cend()};

    // Remove null values from value vector
    if (value_column->is_nullable()) {
      const auto& null_values = value_column->null_values();

      // Swap values to back if value is null
      auto erase_from_here_it = dictionary.end();
      auto null_it = null_values.crbegin();
      for (auto dict_it = dictionary.rbegin(); dict_it != dictionary.rend(); ++dict_it, ++null_it) {
        if (*null_it) {
          std::swap(*dict_it, *(--erase_from_here_it));
        }
      }

      // Erase null values
      dictionary.erase(erase_from_here_it, dictionary.end());
    }

    std::sort(dictionary.begin(), dictionary.end());
    dictionary.erase(std::unique(dictionary.begin(), dictionary.end()), dictionary.end());
    dictionary.shrink_to_fit();

    // We need to increment the dictionary size here because of possible null values.
    auto ns_encoder = _create_ns_encoder(dictionary.size() + 1u, values.size());
    ns_encoder->init(values.size());

    const auto null_value_id = ValueID{dictionary.size()};

    if (value_column->is_nullable()) {
      const auto& null_values = value_column->null_values();

      /**
       * Iterators are used because values and null_values are of
       * type tbb::concurrent_vector and thus index-based access isn’t in O(1)
       */
      auto value_it = values.cbegin();
      auto null_value_it = null_values.cbegin();
      for (; value_it != values.cend(); ++value_it, ++null_value_it) {
        if (*null_value_it) {
          ns_encoder->append(null_value_id);
          continue;
        }

        auto value_id = get_value_id(dictionary, *value_it);
        ns_encoder->append(value_id);
      }
    } else {
      auto value_it = values.cbegin();
      for (; value_it != values.cend(); ++value_it) {
        auto value_id = get_value_id(dictionary, *value_it);
        ns_encoder->append(value_id);
      }
    }

    ns_encoder->finish();

    auto dictionary_ptr = std::make_shared<const pmr_vector<T>>(std::move(dictionary));
    auto attribute_vector_sptr = std::shared_ptr<const BaseNsVector>{std::move(ns_encoder->get_vector())};
    return std::make_shared<DictionaryColumn<T>>(dictionary_ptr, attribute_vector_sptr, null_value_id);
  }

  ValueID get_value_id(const pmr_vector<T>& dictionary, const T& value) {
    return static_cast<ValueID>(
        std::distance(dictionary.cbegin(), std::lower_bound(dictionary.cbegin(), dictionary.cend(), value)));
  }
};

std::shared_ptr<BaseColumn> DictionaryCompression::compress_column(const std::string& column_type,
                                                                   const std::shared_ptr<BaseColumn>& column) {
  auto compressor = make_shared_by_column_type<ColumnCompressorBase, ColumnCompressor>(column_type);
  return compressor->compress_column(column);
}

void DictionaryCompression::compress_chunk(const std::vector<std::string>& column_types, Chunk& chunk) {
  DebugAssert((column_types.size() == chunk.column_count()),
              "Number of column types does not match the chunk’s column count.");

  for (ColumnID column_id{0}; column_id < chunk.column_count(); ++column_id) {
    auto value_column = chunk.get_mutable_column(column_id);
    auto dict_column = compress_column(column_types[column_id], value_column);
    chunk.replace_column(column_id, dict_column);
  }

  if (chunk.has_mvcc_columns()) {
    chunk.shrink_mvcc_columns();
  }
}

void DictionaryCompression::compress_chunks(Table& table, const std::vector<ChunkID>& chunk_ids) {
  for (auto chunk_id : chunk_ids) {
    Assert(chunk_id < table.chunk_count(), "Chunk with given ID does not exist.");

    compress_chunk(table.column_types(), table.get_chunk(chunk_id));
  }
}

void DictionaryCompression::compress_table(Table& table) {
  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    auto& chunk = table.get_chunk(chunk_id);

    compress_chunk(table.column_types(), chunk);
  }
}

}  // namespace opossum
