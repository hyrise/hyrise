#include "chunk_compression.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/dictionary_column.hpp"
#include "storage/fitted_attribute_vector.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

namespace opossum {

class AbstractColumnCompressor {
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
class ColumnCompressor : public AbstractColumnCompressor {
 public:
  std::shared_ptr<BaseColumn> compress_column(const std::shared_ptr<BaseColumn>& column) override {
    auto value_column = std::dynamic_pointer_cast<ValueColumn<T>>(column);

    if (!value_column) {
      throw std::logic_error("Column is either already compressed or type mismatches.");
    }

    // See: https://goo.gl/MCM5rr
    // Create dictionary (enforce unqiueness and sorting)
    const auto& values = value_column->values();
    auto dictionary = std::vector<T>{values.cbegin(), values.cend()};

    std::sort(dictionary.begin(), dictionary.end());
    dictionary.erase(std::unique(dictionary.begin(), dictionary.end()), dictionary.end());
    dictionary.shrink_to_fit();

    auto attribute_vector = _create_fitted_attribute_vector(dictionary.size(), values.size());

    for (ChunkOffset offset = 0; offset < values.size(); ++offset) {
      auto value_id = static_cast<ValueID>(
          std::distance(dictionary.cbegin(), std::lower_bound(dictionary.cbegin(), dictionary.cend(), values[offset])));
      attribute_vector->set(offset, value_id);
    }

    return std::make_shared<DictionaryColumn<T>>(std::move(dictionary), attribute_vector);
  }
};

ChunkCompression::ChunkCompression(const std::string& table_name, const ChunkID chunk_id, bool check_completion)
    : ChunkCompression{table_name, std::vector<ChunkID>{chunk_id}, check_completion} {}

ChunkCompression::ChunkCompression(const std::string& table_name, const std::vector<ChunkID>& chunk_ids,
                                   bool check_completion)
    : _check_completion{check_completion}, _table_name{table_name}, _chunk_ids{chunk_ids} {}

const std::string ChunkCompression::name() const { return "ChunkCompression"; }
uint8_t ChunkCompression::num_in_tables() const { return 0u; }
uint8_t ChunkCompression::num_out_tables() const { return 0u; }

std::shared_ptr<BaseColumn> ChunkCompression::compress_column(const std::string& column_type,
                                                              const std::shared_ptr<BaseColumn>& column) {
  auto compressor = make_shared_by_column_type<AbstractColumnCompressor, ColumnCompressor>(column_type);
  return compressor->compress_column(column);
}

std::shared_ptr<const Table> ChunkCompression::on_execute(std::shared_ptr<TransactionContext> /* context */) {
  auto table = StorageManager::get().get_table(_table_name);

  if (!table) {
    throw std::logic_error("Table does not exist.");
  }

  for (auto chunk_id : _chunk_ids) {
    if (chunk_id >= table->chunk_count()) {
      throw std::logic_error("Chunk with given ID does not exist.");
    }

    auto& chunk = table->get_chunk(chunk_id);

    if (_check_completion && !chunk_is_completed(chunk, table->chunk_size())) {
      throw std::logic_error("Chunk is not completed and thus can’t be compressed.");
    }

    for (auto column_id = 0u; column_id < chunk.col_count(); ++column_id) {
      auto value_column = chunk.get_column(column_id);
      auto dict_column = compress_column(table->column_type(column_id), value_column);
      chunk.set_column(column_id, dict_column);
    }

    chunk.shrink_mvcc_columns();
  }

  return nullptr;
}

bool ChunkCompression::chunk_is_completed(const Chunk& chunk, const uint32_t max_chunk_size) {
  if (chunk.size() != max_chunk_size) return false;

  auto mvcc_columns = chunk.mvcc_columns();

  for (const auto begin_cid : mvcc_columns->begin_cids) {
    if (begin_cid == Chunk::MAX_COMMIT_ID) return false;
  }

  return true;
}

}  // namespace opossum
