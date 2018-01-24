#pragma once

#include <boost/iterator/iterator_facade.hpp>

#include <memory>

#include "base_zero_suppression_decoder.hpp"
#include "zs_vector_type.hpp"

#include "types.hpp"

namespace opossum {

/**
 * @brief Base class of all zero suppression vectors
 *
 * A zero suppression vector is a compressed uint32_t vector.
 *
 * Every zero suppression scheme consists of four parts:
 * - the encoder, which encapsulates the encoding algorithm (base class: BaseZeroSuppressionEncoder)
 * - the vector, which is returned by the encoder and contains the encoded data (base class: BaseZeroSuppressionVector)
 * - the iterator, for sequentially decoding the vector (base class: BaseZeroSuppressionIterator)
 * - the decoder, which implements point access into the vector (base class: BaseZeroSuppressionDecoder)
 *
 * The iterators and decoders are created via virtual and non-virtual methods of the vector interface.
 */
class BaseZeroSuppressionVector : private Noncopyable {
 public:
  virtual ~BaseZeroSuppressionVector() = default;

  /**
   * @brief Returns the number of elements in the vector
   */
  virtual size_t size() const = 0;

  /**
   * @brief Returns the physical size of the vector
   */
  virtual size_t data_size() const = 0;

  virtual ZsVectorType type() const = 0;

  virtual std::unique_ptr<BaseZeroSuppressionDecoder> create_base_decoder() const = 0;

  virtual std::shared_ptr<BaseZeroSuppressionVector> copy_using_allocator(
      const PolymorphicAllocator<size_t>& alloc) const = 0;
};

/**
 * You may use this iterator facade to implement iterators returned
 * by ZeroSuppressionVector::cbegin() and ZeroSuppressionVector::cend()
 */
template <typename Derived>
using BaseZeroSuppressionIterator = boost::iterator_facade<Derived, uint32_t, boost::forward_traversal_tag, uint32_t>;

/**
 * @brief Implements the non-virtual interface of all vectors
 *
 * Sub-classes must implement all method starting with `_on_`.
 */
template <typename Derived>
class ZeroSuppressionVector : public BaseZeroSuppressionVector {
 public:
  /**
   * @defgroup Non-virtual interface
   * @{
   */

  /**
   * @brief Returns a vector specific decoder
   * @return a unique_ptr of subclass of BaseZeroSuppressionDecoder
   */
  auto create_decoder() const { return _self()._on_create_decoder(); }

  /**
   * @brief Returns an iterator to the beginning
   * @return a constant forward iterator returning uint32_t
   */
  auto cbegin() const { return _self()._on_cbegin(); }

  /**
   * @brief Returns an iterator to the end
   * @return a constant forward iterator returning uint32_t
   */
  auto cend() const { return _self()._on_cend(); }
  /**@}*/

 public:
  /**
   * @defgroup Virtual interface implementation
   * @{
   */

  size_t size() const final { return _self()._on_size(); }
  size_t data_size() const final { return _self()._on_data_size(); }

  ZsVectorType type() const final { return get_zs_vector_type<Derived>(); }

  std::unique_ptr<BaseZeroSuppressionDecoder> create_base_decoder() const final {
    return _self()._on_create_base_decoder();
  }

  std::shared_ptr<BaseZeroSuppressionVector> copy_using_allocator(
      const PolymorphicAllocator<size_t>& alloc) const final {
    return _self()._on_copy_using_allocator(alloc);
  }

  /**@}*/

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

}  // namespace opossum
