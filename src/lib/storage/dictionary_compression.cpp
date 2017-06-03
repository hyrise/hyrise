#include "dictionary_compression.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/fitted_attribute_vector.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class ColumnCompressorBase {
 public:
  virtual std::shared_ptr<BaseColumn> compress_column(const std::shared_ptr<BaseColumn>& column) = 0;

 protected:
  static std::shared_ptr<BaseAttributeVector> _create_fitted_attribute_vector(size_t unique_values_count, size_t size) {
    if (unique_values_count <= std::numeric_limits<uint8_t>::max()) {
      return std::make_shared<FittedAttributeVector<uint8_t>>(size);
    } else if (unique_values_count <= std::numeric_limits<uint16_t>::max()) {
      return std::make_shared<FittedAttributeVector<uint16_t>>(size);
    } else {
      return std::make_shared<FittedAttributeVector<uint32_t>>(size);
    }
  }
};

template <typename T>
class ColumnCompressor : public ColumnCompressorBase {
 public:
  std::shared_ptr<BaseColumn> compress_column(const std::shared_ptr<BaseColumn>& column) override {
    auto value_column = std::dynamic_pointer_cast<const ValueColumn<T>>(column);

    if (!value_column) {
      throw std::logic_error("Column is either already compressed or type mismatches.");
    }

    // See: https://goo.gl/MCM5rr
    // Create dictionary (enforce unqiueness and sorting)
    const auto& values = value_column->values();
    auto dictionary = std::vector<T>{values.cbegin(), values.cend()};

    // Remove null values from value vector
    if (value_column->can_be_null()) {
      const auto& null_values = value_column->null_values();

      // Swap values to back if value is null
      auto remove_from_here_it = dictionary.end();
      for (auto record_id = dictionary.size() - 1; record_id >= 0; ++record_id) {
        if (null_values[record_id]) {
          std::swap(dictionary[record_id], *(--remove_from_here_it));
        }
      }

      // Erase all values
      dictionary.erase(remove_from_here_it, dictionary.end());
    }

    std::sort(dictionary.begin(), dictionary.end());
    dictionary.erase(std::unique(dictionary.begin(), dictionary.end()), dictionary.end());
    dictionary.shrink_to_fit();

    auto attribute_vector = _create_fitted_attribute_vector(dictionary.size(), values.size());

    if (value_column->can_be_null()) {
      const auto& null_values = value_column->null_values();

      /**
       * Iterators are used because values and null_values are of
       * type tbb::concurrent_vector and thus index-based access isn’t in O(1)
       */
      auto value_it = values.cbegin();
      auto null_value_it = null_values.cbegin();
      auto index = 0u;
      for (; value_it != values.cend(); ++value_it, ++null_value_it, ++index) {
        if (*null_value_it) {
          attribute_vector->set(index, NULL_VALUE_ID);
          continue;
        }

        auto value_id = find_value_id(dictionary, *value_it);
        attribute_vector->set(index, value_id);
      }
    } else {
      auto value_it = values.cbegin();
      auto index = 0u;
      for (; value_it != values.cend(); ++value_it, ++index) {
        auto value_id = find_value_id(dictionary, *value_it);
        attribute_vector->set(index, value_id);
      }
    }

    return std::make_shared<DictionaryColumn<T>>(std::move(dictionary), attribute_vector, value_column->can_be_null());
  }

  ValueID find_value_id(const std::vector<T>& dictionary, const T& value) {
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
  DebugAssert((column_types.size() == chunk.col_count()),
              "Number of column types does not match the chunk’s column count.");

  for (auto column_id = 0u; column_id < chunk.col_count(); ++column_id) {
    auto value_column = chunk.get_column(column_id);
    auto dict_column = compress_column(column_types[column_id], value_column);
    chunk.replace_column(column_id, dict_column);
  }

  chunk.shrink_mvcc_columns();
}

void DictionaryCompression::compress_chunks(Table& table, const std::vector<ChunkID>& chunk_ids) {
  for (auto chunk_id : chunk_ids) {
    if (chunk_id >= table.chunk_count()) {
      throw std::logic_error("Chunk with given ID does not exist.");
    }

    compress_chunk(table.column_types(), table.get_chunk(chunk_id));
  }
}

void DictionaryCompression::compress_table(Table& table) {
  for (auto chunk_id = 0u; chunk_id < table.chunk_count(); ++chunk_id) {
    auto& chunk = table.get_chunk(chunk_id);

    compress_chunk(table.column_types(), chunk);
  }
}

}  // namespace opossum
