#pragma once

#include <boost/iterator/iterator_facade.hpp>

#include <memory>

#include "base_vector_decompressor.hpp"
#include "compressed_vector_type.hpp"

#include "types.hpp"

namespace opossum {

/**
 * @brief Base class of all compressed vectors
 *
 * A compressed vector stores uint32_t
 *
 * Every compression scheme consists of four parts:
 * - the encoder, which encapsulates the encoding algorithm (base class: BaseVectorCompressor)
 * - the vector, which is returned by the encoder and contains the encoded data (base class: BaseCompressedVector)
 * - the iterator, for sequentially decoding the vector (base class: BaseCompressedVectorIterator)
 * - the decompressor, which implements point access into the vector (base class: BaseVectorDecompressor)
 *
 * The iterators and decompressors are created via virtual and non-virtual methods of the vector interface.
 *
 * Sub-classes must be added in compressed_vector_type.hpp
 */
class BaseCompressedVector : private Noncopyable {
 public:
  virtual ~BaseCompressedVector() = default;

  /**
   * @brief Returns the number of elements in the vector
   */
  virtual size_t size() const = 0;

  /**
   * @brief Returns the physical size of the vector
   */
  virtual size_t data_size() const = 0;

  virtual CompressedVectorType type() const = 0;

  virtual std::unique_ptr<BaseVectorDecompressor> create_base_decompressor() const = 0;

  virtual std::unique_ptr<const BaseCompressedVector> copy_using_allocator(
      const PolymorphicAllocator<size_t>& alloc) const = 0;
};

/**
 * You may use this iterator facade to implement iterators returned
 * by CompressedVector::cbegin() and CompressedVector::cend()
 */
template <typename Derived>
using BaseCompressedVectorIterator =
    boost::iterator_facade<Derived, uint32_t, boost::random_access_traversal_tag, uint32_t>;

/**
 * @brief Implements the non-virtual interface of all vectors
 *
 * Sub-classes must implement all method starting with `on_`.
 */
template <typename Derived>
class CompressedVector : public BaseCompressedVector {
 public:
  /**
   * @defgroup Non-virtual interface
   * @{
   */

  /**
   * @brief Returns a vector specific decompressor
   * @return a unique_ptr of subclass of BaseVectorDecompressor
   */
  auto create_decompressor() const { return _self().on_create_decompressor(); }

  /**
   * @brief Returns an iterator to the beginning
   * @return a constant input iterator returning uint32_t
   */
  auto begin() const { return _self().on_begin(); }
  auto cbegin() const { return begin(); }

  /**
   * @brief Returns an iterator to the end
   * @return a constant input iterator returning uint32_t
   */
  auto end() const { return _self().on_end(); }
  auto cend() const { return end(); }
  /**@}*/

 public:
  /**
   * @defgroup Virtual interface implementation
   * @{
   */

  size_t size() const final { return _self().on_size(); }
  size_t data_size() const final { return _self().on_data_size(); }

  CompressedVectorType type() const final { return get_compressed_vector_type<Derived>(); }

  std::unique_ptr<BaseVectorDecompressor> create_base_decompressor() const final {
    return _self().on_create_base_decompressor();
  }

  std::unique_ptr<const BaseCompressedVector> copy_using_allocator(
      const PolymorphicAllocator<size_t>& alloc) const final {
    return _self().on_copy_using_allocator(alloc);
  }

  /**@}*/

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

}  // namespace opossum
