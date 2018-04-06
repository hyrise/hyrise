
#pragma once

#include <memory>

#include "base_encoded_column.hpp"
#include "types.hpp"

namespace opossum {

class BaseCompressedVector;

/**
 * @brief Column implementing run-length encoding
 *
 * Run-length encoding takes advantage of long sequences
 * of the same value, called runs. Each run is represented
 * by the value and the number of occurences.
 *
 * Instead of storing run lengths, this implementation
 * stores the end positions of each run. The resulting
 * sorted list can be traversed via binary search, which
 * makes randomly accessing elements much faster.
 *
 * As in value columns, null values are represented as an
 * additional boolean vector.
 */
template <typename T>
class RunLengthColumn : public BaseEncodedColumn {
 public:
  explicit RunLengthColumn(const std::shared_ptr<const pmr_vector<T>>& values,
                           const std::shared_ptr<const pmr_vector<bool>>& null_values,
                           const std::shared_ptr<const pmr_vector<ChunkOffset>>& end_positions);

  std::shared_ptr<const pmr_vector<T>> values() const;
  std::shared_ptr<const pmr_vector<bool>> null_values() const;
  std::shared_ptr<const pmr_vector<ChunkOffset>> end_positions() const;

  /**
   * @defgroup BaseColumn interface
   * @{
   */

  const AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  size_t size() const final;

  std::shared_ptr<BaseColumn> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  size_t estimate_memory_usage() const final;

  /**@}*/

  /**
   * @defgroup BaseEncodedColumn interface
   * @{
   */

  EncodingType encoding_type() const final;

  /**@}*/

 protected:
  const std::shared_ptr<const pmr_vector<T>> _values;
  const std::shared_ptr<const pmr_vector<bool>> _null_values;
  const std::shared_ptr<const pmr_vector<ChunkOffset>> _end_positions;
};

}  // namespace opossum
