#pragma once

#include <boost/iterator/iterator_facade.hpp>
#include <cstdint>

namespace opossum {

template <typename Derived>
using BaseNsIterator = boost::iterator_facade<Derived, uint32_t, boost::forward_traversal_tag>;

/**
 * Implements the non-virtual interface of all decoders
 */
template <typename Derived>
class NsDecoder {
 public:
  /**
   * @brief Returns an iterator to the beginning
   * @return a constant forward iterator that inherits from BaseNsIterator
   */
  auto cbegin() {
    return _self()._on_cbegin();
  }

  /**
   * @brief Returns an iterator to the end
   * @return a constant forward iterator that inherits from BaseNsIterator
   */
  auto cend() {
    return _self()._on_cend();
  }

  uint32_t get(size_t i) {
    return _self()._on_get(i);
  }

  size_t size() {
    return _self()._on_size();
  }

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

}  // namespace opossum
