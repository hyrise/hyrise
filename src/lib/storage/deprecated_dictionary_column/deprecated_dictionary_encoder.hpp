#pragma once

#include <algorithm>
#include <limits>
#include <memory>

#include "storage/base_column_encoder.hpp"
#include "storage/deprecated_dictionary_column.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

#include "fitted_attribute_vector.hpp"

namespace opossum {

class DeprecatedDictionaryEncoder : public ColumnEncoder<DeprecatedDictionaryEncoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::DeprecatedDictionary>;
  static constexpr auto _uses_vector_compression = false;

  template <typename T>
  std::shared_ptr<BaseEncodedColumn> _on_encode(const std::shared_ptr<const ValueColumn<T>>& value_column) {
    // See: https://goo.gl/MCM5rr
    // Create dictionary (enforce uniqueness and sorting)
    const auto& values = value_column->values();
    const auto alloc = values.get_allocator();

    auto dictionary = pmr_vector<T>{values.cbegin(), values.cend(), alloc};

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
    auto attribute_vector = _create_fitted_attribute_vector(dictionary.size() + 1u, values.size(), alloc);

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

        auto value_id = _get_value_id(dictionary, *value_it);
        attribute_vector->set(index, value_id);
      }
    } else {
      auto value_it = values.cbegin();
      auto index = 0u;
      for (; value_it != values.cend(); ++value_it, ++index) {
        auto value_id = _get_value_id(dictionary, *value_it);
        attribute_vector->set(index, value_id);
      }
    }

    return std::allocate_shared<DeprecatedDictionaryColumn<T>>(alloc, std::move(dictionary), attribute_vector);
  }

 private:
  template <typename T>
  static ValueID _get_value_id(const pmr_vector<T>& dictionary, const T& value) {
    return static_cast<ValueID>(
        std::distance(dictionary.cbegin(), std::lower_bound(dictionary.cbegin(), dictionary.cend(), value)));
  }

  static std::shared_ptr<BaseAttributeVector> _create_fitted_attribute_vector(
      size_t unique_values_count, size_t size, const PolymorphicAllocator<size_t>& alloc) {
    if (unique_values_count <= std::numeric_limits<uint8_t>::max()) {
      return std::make_shared<FittedAttributeVector<uint8_t>>(size, alloc);
    } else if (unique_values_count <= std::numeric_limits<uint16_t>::max()) {
      return std::make_shared<FittedAttributeVector<uint16_t>>(size, alloc);
    } else {
      return std::make_shared<FittedAttributeVector<uint32_t>>(size, alloc);
    }
  }
};

}  // namespace opossum
