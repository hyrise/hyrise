#pragma once

#include <algorithm>
#include <limits>
#include <memory>

#include "storage/base_column_encoder.hpp"

#include "storage/dictionary_column.hpp"
#include "storage/fixed_string_dictionary_column/fixed_string_column.hpp"
#include "storage/value_column.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"

#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

/**
 * @brief Encodes a column using dictionary encoding and compresses its attribute vector using vector compression.
 *
 * The algorithm first creates an attribute vector of standard size (uint32_t) and then compresses it
 * using fixed-size byte-aligned encoding.
 */
class DictionaryEncoder : public ColumnEncoder<DictionaryEncoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::Dictionary>;
  static constexpr auto _uses_vector_compression = true;  // see base_column_encoder.hpp for details

  template <typename T>
  std::shared_ptr<BaseEncodedColumn> _on_encode(const std::shared_ptr<const ValueColumn<T>>& value_column) {
    // See: https://goo.gl/MCM5rr
    // Create dictionary (enforce uniqueness and sorting)
    const auto& values = value_column->values();
    const auto alloc = values.get_allocator();

    if constexpr (false && std::is_same<T, std::string>::value) {
      auto dictionary = FixedStringVector{values.cbegin(), values.cend(), 111};
      auto encoded_attribute_vector = _encode_dictionary(dictionary, value_column);

      auto dictionary_sptr = std::allocate_shared<FixedStringVector>(alloc, std::move(dictionary));
      auto attribute_vector_sptr = std::shared_ptr<const BaseCompressedVector>(std::move(encoded_attribute_vector));
      return std::allocate_shared<FixedStringColumn<std::string>>(alloc, dictionary_sptr, attribute_vector_sptr,
                                                                  ValueID{static_cast<uint32_t>(dictionary.size())});
    } else {
      std::cout << " " << std::endl;
      auto dictionary = pmr_vector<T>{values.cbegin(), values.cend(), alloc};
      auto encoded_attribute_vector = _encode_dictionary(dictionary, value_column);
      const auto null_value_id = static_cast<uint32_t>(dictionary.size());

      std::cout << "dictionary size: " << dictionary.size() << std::endl;
      for (const auto& d : dictionary) {
        std::cout << d << std::endl;
      }

      auto dictionary_sptr = std::allocate_shared<pmr_vector<T>>(alloc, std::move(dictionary));
      auto attribute_vector_sptr = std::shared_ptr<const BaseCompressedVector>(std::move(encoded_attribute_vector));
      return std::allocate_shared<DictionaryColumn<T>>(alloc, dictionary_sptr, attribute_vector_sptr,
                                                       ValueID{null_value_id});
    }
  }

 private:
  template <typename U, typename T>
  static ValueID _get_value_id(const U& dictionary, const T& value) {
    return static_cast<ValueID>(
        std::distance(dictionary.cbegin(), std::lower_bound(dictionary.cbegin(), dictionary.cend(), value)));
  }

  template <typename U, typename T>
  std::unique_ptr<const BaseCompressedVector> _encode_dictionary(
      U& dictionary, const std::shared_ptr<const ValueColumn<T>>& value_column) {
    const auto& values = value_column->values();
    const auto alloc = values.get_allocator();

    std::cout << "values:" << std::endl;
    for (const auto& v : values) {
      std::cout << v << std::endl;
    }

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

    auto attribute_vector = pmr_vector<uint32_t>{values.get_allocator()};
    attribute_vector.reserve(values.size());

    const auto null_value_id = static_cast<uint32_t>(dictionary.size());

    if (value_column->is_nullable()) {
      const auto& null_values = value_column->null_values();

      /**
       * Iterators are used because values and null_values are of
       * type tbb::concurrent_vector and thus index-based access isn’t in O(1)
       */
      auto value_it = values.cbegin();
      auto null_value_it = null_values.cbegin();
      for (; value_it != values.cend(); ++value_it, ++null_value_it) {
        if (*null_value_it) {
          attribute_vector.push_back(null_value_id);
          continue;
        }

        const auto value_id = _get_value_id(dictionary, *value_it);
        attribute_vector.push_back(value_id);
      }
    } else {
      auto value_it = values.cbegin();
      for (; value_it != values.cend(); ++value_it) {
        const auto value_id = _get_value_id(dictionary, *value_it);
        attribute_vector.push_back(value_id);
      }
    }

    std::cout << "attribute_vector:" << std::endl;
    for (const auto& v : attribute_vector) {
      std::cout << v << std::endl;
    }

    // We need to increment the dictionary size here because of possible null values.
    const auto max_value = dictionary.size() + 1u;

    return compress_vector(attribute_vector, vector_compression_type(), alloc, {max_value});
  }
};

}  // namespace opossum
