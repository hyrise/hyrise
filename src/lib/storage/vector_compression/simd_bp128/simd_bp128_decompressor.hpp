#pragma once

#include <array>
#include <memory>
#include <numeric>
#include <utility>

#include "storage/vector_compression/base_vector_decompressor.hpp"

#include "oversized_types.hpp"
#include "simd_bp128_packing.hpp"

#include "types.hpp"

namespace opossum {

class SimdBp128Vector;

/**
 * @brief Implements point-access into a SIMD-BP128 compressed vector
 *
 * The decompressor caches the last decoded block and meta-block.
 * Performs best if the values are accessed in a cache-sensitive manner, i.e.,
 * sequentially without too many jumps backwards.
 */
class SimdBp128Decompressor : public BaseVectorDecompressor {
 public:
  using Packing = SimdBp128Packing;

 public:
  explicit SimdBp128Decompressor(const SimdBp128Vector& vector);
  SimdBp128Decompressor(const SimdBp128Decompressor& other);

  SimdBp128Decompressor(SimdBp128Decompressor&& other) = default;
  ~SimdBp128Decompressor() = default;

  uint32_t get(size_t i) final {
    if (_is_index_within_cached_block(i)) {
      return _get_within_cached_block(i);
    }

    if (_is_index_within_cached_meta_block(i)) {
      return _get_within_cached_meta_block(i);
    }

    if (_is_index_after_or_within_cached_meta_block(i)) {
      const auto relative_index = _index_relative_to_cached_meta_block(i);
      const auto relative_meta_block_index = relative_index / Packing::meta_block_size;

      _read_meta_info_from_offset(relative_meta_block_index);
      return _get_within_cached_meta_block(i);
    }

    _clear_meta_block_cache();

    /**
     * The decoder wasn’t able to use its caches.
     * We need to load the first meta info and
     * sequentially run through the encoded data
     * up to the meta block in which the requested element is located.
     */

    _read_meta_info(_cached_meta_info_offset);
    const auto meta_block_index = i / Packing::meta_block_size;
    _read_meta_info_from_offset(meta_block_index);
    return _get_within_cached_meta_block(i);
  }

  size_t size() const final { return _size; }

 private:
  bool _is_index_within_cached_block(size_t index) const {
    const auto begin = _cached_block_first_index;
    const auto end = _cached_block_first_index + Packing::block_size;
    return begin <= index && index < end;
  }

  size_t _index_within_cached_block(size_t index) { return index - _cached_block_first_index; }

  bool _is_index_within_cached_meta_block(size_t index) const {
    const auto begin = _cached_meta_block_first_index;
    const auto end = _cached_meta_block_first_index + Packing::meta_block_size;
    return begin <= index && index < end;
  }

  size_t _index_relative_to_cached_meta_block(size_t index) const { return index - _cached_meta_block_first_index; }

  bool _is_index_after_or_within_cached_meta_block(size_t index) const {
    return _cached_meta_block_first_index <= index;
  }

  uint32_t _get_within_cached_block(size_t index) { return (*_cached_block)[_index_within_cached_block(index)]; }

  uint32_t _get_within_cached_meta_block(size_t index) {
    const auto block_index = _index_relative_to_cached_meta_block(index) / Packing::block_size;
    _unpack_block(block_index);

    return (*_cached_block)[_index_within_cached_block(index)];
  }

  /**
   * Starting from the cached meta info offset,
   * jumps to the meta block with the relative
   * index `meta_block_index`
   */
  void _read_meta_info_from_offset(size_t relative_meta_block_index) {
    auto meta_info_offset = _cached_meta_info_offset;
    for (auto i = 0u; i < relative_meta_block_index; ++i) {
      static const auto meta_info_data_size = 1u;  // One 128 bit block
      const auto meta_block_data_size =
          meta_info_data_size + std::accumulate(_cached_meta_info.begin(), _cached_meta_info.end(), 0u);
      meta_info_offset += meta_block_data_size;
      _read_meta_info(meta_info_offset);
    }

    _cached_meta_info_offset = meta_info_offset;
    _cached_meta_block_first_index += relative_meta_block_index * Packing::meta_block_size;
  }

  void _clear_meta_block_cache() {
    _cached_meta_info_offset = 0u;
    _cached_meta_block_first_index = 0u;
  }

  /**
   * @brief reads meta info at a given absolute position
   *
   * @param meta_info_offset an absolute position within the encoded vector
   */
  void _read_meta_info(size_t meta_info_offset);

  /**
   * @brief unpacks a block in the current meta block
   *
   * @param block_index relative block index within the current meta block
   */
  void _unpack_block(uint8_t block_index);

 private:
  const pmr_vector<uint128_t>* _data;
  const size_t _size;

  // Cached meta info’s offset into the encoded vector
  size_t _cached_meta_info_offset;

  // Index of the first element within the cached meta block
  size_t _cached_meta_block_first_index;

  // Index of the first element within the cached block
  size_t _cached_block_first_index;

  std::array<uint8_t, Packing::blocks_in_meta_block> _cached_meta_info{};
  const std::unique_ptr<std::array<uint32_t, Packing::block_size>> _cached_block;
};

}  // namespace opossum
