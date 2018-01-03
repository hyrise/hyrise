#pragma once

#include <memory>
#include <string>

#include "base_encoded_column.hpp"
#include "types.hpp"

namespace opossum {

class BaseZeroSuppressionVector;

/**
 * @brief Column implementing run-length encoding
 */
template <typename T>
class RunLengthColumn : public BaseEncodedColumn {
 public:
  explicit RunLengthColumn(const std::shared_ptr<const pmr_vector<T>>& values,
                           const std::shared_ptr<const pmr_vector<ChunkOffset>>& end_positions, const T null_value);

  std::shared_ptr<const pmr_vector<T>> values() const;
  std::shared_ptr<const pmr_vector<ChunkOffset>> end_positions() const;

  // column specific null_value
  const T null_value() const;

  /**
   * @defgroup BaseColumn interface
   * @{
   */

  const AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  size_t size() const final;

  void write_string_representation(std::string& row_string, const ChunkOffset chunk_offset) const final;

  void copy_value_to_value_column(BaseColumn& value_column, ChunkOffset chunk_offset) const final;

  std::shared_ptr<BaseColumn> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  /**@}*/

  /**
   * @defgroup BaseEncodedColumn interface
   * @{
   */

  EncodingType encoding_type() const final;

  /**@}*/

 protected:
  const std::shared_ptr<const pmr_vector<T>> _values;
  const std::shared_ptr<const pmr_vector<ChunkOffset>> _end_positions;
  const T _null_value;
};

struct RunLengthColumnInfo {
  template <typename T>
  using ColumnTemplate = RunLengthColumn<T>;
};

}  // namespace opossum
