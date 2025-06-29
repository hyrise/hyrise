#pragma once

#include <type_traits>
#include <vector>
#include <string>
#include <optional>
#include <cstdint>
#include <algorithm>
#include <cstring>

namespace hyrise {

// Portable byte swap implementation for 32-bit integer
inline uint32_t portable_bswap_32(const uint32_t val) {
  return ((val & 0xFF000000) >> 24) |
         ((val & 0x00FF0000) >>  8) |
         ((val & 0x0000FF00) <<  8) |
         ((val & 0x000000FF) << 24);
}

// Portable byte swap implementation for 64-bit integer
inline uint64_t portable_bswap_64(const uint64_t val) {
  return ((val & 0xFF00000000000000) >> 56) |
         ((val & 0x00FF000000000000) >> 40) |
         ((val & 0x0000FF0000000000) >> 24) |
         ((val & 0x000000FF00000000) >>  8) |
         ((val & 0x00000000FF000000) <<  8) |
         ((val & 0x0000000000FF0000) << 24) |
         ((val & 0x000000000000FF00) << 40) |
         ((val & 0x00000000000000FF) << 56);
}

template<typename T>
T portable_bswap(T val) {
  if constexpr (sizeof(T) == 4) {
    return portable_bswap_32(val);
  } else if constexpr (sizeof(T) == 8) {
    return portable_bswap_64(val);
  }
  return val;
}

enum class NormalizedSortMode : uint8_t {
    Ascending,
    Descending
};

enum class NullsMode : uint8_t {
  NullsFirst,
  NullsLast
};

/**
 * @brief Creates a binary-sortable key for one or more values.
 *
 * This class implements the key normalization technique described in the DuckDB paper
 * "These Rows Are Made for Sorting". It takes values of different types and appends them
 * to a byte buffer in a specific, "normalized" format. The resulting byte array has a
 * crucial property: when two such keys are compared using `memcmp`, the result reflects
 * the desired sort order of the original values (respecting ASC/DESC and NULLS FIRST/LAST).
 * This avoids expensive, type-aware comparison logic within the hot loop of a sort algorithm.
 */
class KeyNormalizer {
public:
    explicit KeyNormalizer(std::vector<unsigned char>& buffer) : _buffer(buffer) {}

    void append(const std::optional<int32_t>& value, const NormalizedSortMode desc, const NullsMode nulls_first) {
        append_integral(value, desc, nulls_first);
    }

    void append(const std::optional<int64_t>& value, const NormalizedSortMode desc, const NullsMode nulls_first) {
        append_integral(value, desc, nulls_first);
    }

    void append(const std::optional<float>& value, const NormalizedSortMode desc, const NullsMode nulls_first) {
        append_floating_point(value, desc, nulls_first);
    }

    void append(const std::optional<double>& value, const NormalizedSortMode desc, const NullsMode nulls_first) {
        append_floating_point(value, desc, nulls_first);
    }

    void append(const std::optional<std::string>& value, const NormalizedSortMode desc, const NullsMode nulls_first, const size_t prefix_size = 12) {
        append_null_prefix(value.has_value(), nulls_first);
        if (!value.has_value()) {
            _buffer.resize(_buffer.size() + prefix_size, 0x00);
            return;
        }

        const auto& str = value.value();
        const auto len_to_copy = std::min(str.length(), prefix_size);

        const auto current_size = _buffer.size();
        _buffer.resize(current_size + prefix_size);

        std::memcpy(_buffer.data() + current_size, str.data(), len_to_copy);

       // Pad with 0s if the string is shorter than the prefix to ensure fixed width.
        if (len_to_copy < prefix_size) {
            std::memset(_buffer.data() + current_size + len_to_copy, 0, prefix_size - len_to_copy);
        }

      // For descending order, we simply invert all bits of the value's representation.
        if (desc == NormalizedSortMode::Descending) {
            for (size_t i = 0; i < prefix_size; ++i) {
                _buffer[current_size + i] = ~_buffer[current_size + i];
            }
        }
    }

    void append_row_id(uint64_t row_id) {
        append_integral(std::optional(row_id), NormalizedSortMode::Ascending, NullsMode::NullsFirst);
    }

private:
    std::vector<unsigned char>& _buffer;

    /**
     * @brief Appends a single byte prefix to handle NULL ordering.
     * This byte ensures that NULLs are sorted correctly before or after non-NULL values,
     * regardless of the ASC/DESC sort order of the values themselves.
     */
    void append_null_prefix(const bool has_value, const NullsMode nulls_first) const {
        unsigned char null_byte;
        if (nulls_first == NullsMode::NullsFirst) {
            null_byte = has_value ? 1 : 0;
        } else {
            null_byte = has_value ? 0 : 1;
        }
        _buffer.push_back(null_byte);
    }


    template <typename T>
    void append_integral(const std::optional<T>& value, const NormalizedSortMode desc, const NullsMode nulls_first) {
        append_null_prefix(value.has_value(), nulls_first);
        if (!value.has_value()) {
          // If the value is NULL, we just pad with zeros to maintain a fixed key width.
            _buffer.resize(_buffer.size() + sizeof(T), 0x00);
            return;
        }

        T val = value.value();

      // For signed integers, the sign bit must be flipped. This maps the range of signed
      // values (e.g., -128 to 127) to an unsigned range (0 to 255) in a way that
      // preserves their order for a lexicographical byte comparison.
        if constexpr (std::is_signed_v<T>) {
            val ^= (T(1) << (sizeof(T) * 8 - 1));
        }

      // Ensure the byte order is big-endian before writing to the buffer. If not, we swap.
        if constexpr (std::endian::native == std::endian::little) {
            if constexpr (sizeof(T) == 4) val = portable_bswap_32(val);
            if constexpr (sizeof(T) == 8) val = portable_bswap_64(val);
        }

        const size_t current_size = _buffer.size();
        _buffer.resize(current_size + sizeof(T));
        std::memcpy(_buffer.data() + current_size, &val, sizeof(T));

      // For descending order, we simply invert all bits of the value's representation.
        if (desc == NormalizedSortMode::Descending) {
            for (size_t i = 0; i < sizeof(T); ++i) {
                _buffer[current_size + i] = ~_buffer[current_size + i];
            }
        }
    }

    template <typename T>
    void append_floating_point(const std::optional<T>& value, NormalizedSortMode desc, NullsMode nulls_first) {
        static_assert(std::is_floating_point_v<T>, "T must be a floating point type");
        using I = std::conditional_t<sizeof(T) == 4, uint32_t, uint64_t>;

        std::optional<I> int_value;
        if (value.has_value()) {
            I reinterpreted_val;
            std::memcpy(&reinterpreted_val, &(*value), sizeof(T));

            // If the float is negative (sign bit is 1), we flip all bits to reverse the sort order.
            // If the float is positive (sign bit is 0), we flip only the sign bit to make it sort after all negatives.
            if (reinterpreted_val & (I(1) << (sizeof(I) * 8 - 1))) {
                reinterpreted_val = ~reinterpreted_val;
            } else {
                reinterpreted_val ^= (I(1) << (sizeof(I) * 8 - 1));
            }
            int_value = reinterpreted_val;
        }

        // Now, call append_integral with the correctly transformed bits. Since `I` is unsigned,
        // the signed-integer logic inside append_integral will be skipped.
        append_integral(int_value, desc, nulls_first);
    }
};
} // namespace hyrise


