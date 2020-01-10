#pragma once

#include <memory>

#include "base_encoded_segment.hpp"
#include "types.hpp"

namespace opossum {

class BaseCompressedVector;

/**
 * @brief Segment implementing run-length encoding
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
 * As in value segments, null values are represented as an
 * additional boolean vector. Note, NULLs are also stored in
 * runs. When a NULL run covers multiple value runs, the
 * first value is kept as a place holder and the following
 * values (which are also NULL) are merged into this value
 * run.
 *
 * Example:
 *  values:          1 1 1 2 2 2 3 3 3
 *  nulls:           0 0 0 0 0 1 1 1 0
 *  value runs:     |1    |2    |3    |
 *  null runs:      |0        |1    |0|
 *
 * Actually stored data:
 *  values:          1 2 2 3  (note, repeating values)
 *  null values:     0 0 1 0
 *  end positions:   2 4 6 8
 */
template <typename T>
class RunLengthSegment : public BaseEncodedSegment {
 public:
  explicit RunLengthSegment(const std::shared_ptr<const pmr_vector<T>>& values,
                            const std::shared_ptr<const pmr_vector<bool>>& null_values,
                            const std::shared_ptr<const pmr_vector<ChunkOffset>>& end_positions);

  std::shared_ptr<const pmr_vector<T>> values() const;
  std::shared_ptr<const pmr_vector<bool>> null_values() const;
  std::shared_ptr<const pmr_vector<ChunkOffset>> end_positions() const;

  /**
   * @defgroup BaseSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const {
    // performance critical - not in cpp to help with inlining
    const auto end_position_it = std::lower_bound(_end_positions->cbegin(), _end_positions->cend(), chunk_offset);
    const auto index = std::distance(_end_positions->cbegin(), end_position_it);

    const auto is_null = (*_null_values)[index];
    if (is_null) {
      return std::nullopt;
    }

    return (*_values)[index];
  }

  ChunkOffset size() const final;

  std::shared_ptr<BaseSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  size_t estimate_memory_usage() const final;

  /**@}*/

  /**
   * @defgroup BaseEncodedSegment interface
   * @{
   */

  EncodingType encoding_type() const final;
  std::optional<CompressedVectorType> compressed_vector_type() const final;

  /**@}*/

 protected:
  const std::shared_ptr<const pmr_vector<T>> _values;
  const std::shared_ptr<const pmr_vector<bool>> _null_values;
  const std::shared_ptr<const pmr_vector<ChunkOffset>> _end_positions;
};

}  // namespace opossum
