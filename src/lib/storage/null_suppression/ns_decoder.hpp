#pragma once

#include <boost/iterator/iterator_facade.hpp>
#include <cstdint>

#include "types.hpp"

namespace opossum {

/**
 * @brief Implements the virtual interface of all decoders
 */
class BaseNsDecoder {
 public:
  virtual uint32_t get(size_t i) const = 0;
  virtual size_t size() const = 0;
  virtual pmr_vector<uint32_t> decode() const = 0;
};

template <typename Derived>
using BaseNsIterator = boost::iterator_facade<Derived, uint32_t, boost::forward_traversal_tag, uint32_t>;

/**
 * @brief Implements the non-virtual interface of all decoders
 */
template <typename Derived>
class NsDecoder : public BaseNsDecoder {
 public:
  uint32_t get(size_t i) const final {
    return _self()._on_get(i);
  }

  size_t size() const final {
    return _self()._on_size();
  }

  pmr_vector<uint32_t> decode() const final {
    auto decoded_vector = pmr_vector<uint32_t>{};
    decoded_vector.reserve(size());

    auto it = cbegin();
    const auto end = cend();
    for (; it != end; ++it) {
      decoded_vector.push_back(*it);
    }

    return decoded_vector;
  }

  /**
   * @brief Returns an iterator to the beginning
   * @return a constant forward iterator that inherits from BaseNsIterator
   */
  auto cbegin() const {
    return _self()._on_cbegin();
  }

  /**
   * @brief Returns an iterator to the end
   * @return a constant forward iterator that inherits from BaseNsIterator
   */
  auto cend() const {
    return _self()._on_cend();
  }

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

}  // namespace opossum
