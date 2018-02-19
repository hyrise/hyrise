#include "dictionary_compression.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "chunk.hpp"
#include "optimizer/chunk_statistics.hpp"
#include "dictionary_column.hpp"
#include "fitted_attribute_vector.hpp"
#include "resolve_type.hpp"
#include "table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_column.hpp"

namespace opossum {

class ColumnCompressorBase {
 public:
  virtual std::tuple<std::shared_ptr<BaseColumn>, std::shared_ptr<ChunkColumnStatistics>> compress_column(
      const std::shared_ptr<BaseColumn>& column) = 0;

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
  std::tuple<std::shared_ptr<BaseColumn>, std::shared_ptr<ChunkColumnStatistics>> compress_column(
      const std::shared_ptr<BaseColumn>& column) override {
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
    auto attribute_vector = _create_fitted_attribute_vector(dictionary.size() + 1u, values.size());

    if (value_column->is_nullable()) {
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

        auto value_id = get_value_id(dictionary, *value_it);
        attribute_vector->set(index, value_id);
      }
    } else {
      auto value_it = values.cbegin();
      auto index = 0u;
      for (; value_it != values.cend(); ++value_it, ++index) {
        auto value_id = get_value_id(dictionary, *value_it);
        attribute_vector->set(index, value_id);
      }
    }

    // only create statistics when the compressed dictionary is not empty
    auto statistics = std::make_shared<ChunkColumnStatistics>();
    if(!dictionary.empty()) {
      auto min_max_filter = std::make_shared<MinMaxFilter<T>>(dictionary.front(), dictionary.back());
      statistics->add_filter(min_max_filter);

      // calculate distances by taking the difference between two neighbouring elements
      std::vector<std::pair<T, size_t>> distances;
      auto dict_it = dictionary.cbegin();
      auto dict_it_offset = dictionary.cbegin();
      ++dict_it_offset;
      while(dict_it_offset != dictionary.cend()) {
        distances.emplace_back({*dict_it - *dict_it_offset, std::distance(dictionary.cbegin(), dict_it)});
      }

      std::sort(distances.begin(), distances.end(), [](auto pair1, auto pair2){ return pair1.first > pair2.first; });

      // select how many ranges we want in the filter
      // make this customizable?
      size_t max_ranges_count = 10;

      if(max_ranges_count - 1 < distances.size()) {
        distances.erase(dictionary.begin() + (max_ranges_count -1));
      }

      std::sort(distances.begin(), distances.end(), [](auto pair1, auto pair2){ return pair1.second > pair2.second; });
      distances.push_back({T{}, dictionary.size() - 1});

      // derive intervals where items exists from distances
      //
      // start   end  next_startpoint                
      // v       v    v                
      // 1 2 3 4 5    10 11     15 16
      //         ^
      //       distance 5, index 4
      //
      // next_startpoint is the start of the next range

      std::vector<std::tuple<T,T>> ranges;
      size_t next_startpoint = 0u;
      for(const auto& [distance, index] : distances) {
        ranges.push_back({dictionary[next_startpoint], dictionary[index]});
        next_startpoint = index + 1;
      }

      auto range_filter = std::make_shared<RangeFilter<T>>(ranges);
      statistics->add_filter(range_filter);
    }

    // auto statistics = dictionary.empty()
    //                  ? std::shared_ptr<ChunkColumnStatistics>()
    //                  : std::dynamic_pointer_cast<ChunkColumnStatistics>(
    //                        std::make_shared<ChunkColumnStatistics<T>>(dictionary.front(), dictionary.back()));
    auto out_column = std::make_shared<DictionaryColumn<T>>(std::move(dictionary), attribute_vector);

    return std::make_tuple(out_column, statistics);
  }

  ValueID get_value_id(const pmr_vector<T>& dictionary, const T& value) {
    return static_cast<ValueID>(
        std::distance(dictionary.cbegin(), std::lower_bound(dictionary.cbegin(), dictionary.cend(), value)));
  }
};

std::tuple<std::shared_ptr<BaseColumn>, std::shared_ptr<ChunkColumnStatistics>>
DictionaryCompression::compress_column(DataType data_type, const std::shared_ptr<BaseColumn>& column) {
  auto compressor = make_shared_by_data_type<ColumnCompressorBase, ColumnCompressor>(data_type);
  return compressor->compress_column(column);
}

std::shared_ptr<ChunkStatistics> DictionaryCompression::compress_chunk(const std::vector<DataType>& column_types,
                                                                       const std::shared_ptr<Chunk>& chunk) {
  DebugAssert((column_types.size() == chunk->column_count()),
              "Number of column types does not match the chunk’s column count.");

  std::vector<std::shared_ptr<ChunkColumnStatistics>> column_statistics;

  for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
    auto value_column = chunk->get_mutable_column(column_id);
    auto[dict_column, statistics] = compress_column(column_types[column_id], value_column);
    chunk->replace_column(column_id, dict_column);
    column_statistics.push_back(statistics);
  }

  if (chunk->has_mvcc_columns()) {
    chunk->shrink_mvcc_columns();
  }

  auto statistics = std::make_shared<ChunkStatistics>(column_statistics);
  chunk->set_statistics(statistics);

  return statistics;
}

std::vector<std::shared_ptr<ChunkStatistics>> DictionaryCompression::compress_chunks(
    Table& table, const std::vector<ChunkID>& chunk_ids) {
  std::vector<std::shared_ptr<ChunkStatistics>> chunk_statistics;
  for (auto chunk_id : chunk_ids) {
    Assert(chunk_id < table.chunk_count(), "Chunk with given ID does not exist.");

    chunk_statistics.push_back(compress_chunk(table.column_types(), table.get_chunk(chunk_id)));
  }

  return chunk_statistics;
}

std::vector<std::shared_ptr<ChunkStatistics>> DictionaryCompression::compress_table(Table& table) {
  std::vector<std::shared_ptr<ChunkStatistics>> chunk_statistics;
  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    auto chunk = table.get_chunk(chunk_id);
    chunk_statistics.push_back(compress_chunk(table.column_types(), chunk));
  }
  return chunk_statistics;
}

}  // namespace opossum
