#include <string.h>

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <mutex>

#include "all_type_variant.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "resolve_type.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"

#define NULLS_FIRST 1
#define STRING_PREFIX 8

using namespace hyrise;  // NOLINT(build/namespaces)

using NormalizedKey = std::vector<uint8_t>;

/**
 * Encodes a signed integer value into a byte array for sorting purposes.
 * Prepends the integer with a null byte to indicate, whether the value is null or not.
 * Bytes will be inverted when sorting in descending order.
 */
template <typename T>
void encode_int_value(uint8_t* dest, const std::optional<T>& val, const SortColumnDefinition& def) {
  // Determine if value is null. Replace this with your actual null check logic.
  T value;
  if (val.has_value()) {
    value = val.value();
  } else {
    value = T();  // Default value for the type, e.g., 0 for int32_t
  }

  // First byte encodes null presence (and null order preference)
  uint8_t null_byte = val.has_value() ? 0x00 : 0xFF;
  if (def.sort_mode == SortMode::AscendingNullsFirst || def.sort_mode == SortMode::DescendingNullsFirst) {
    null_byte = ~null_byte;
  }
  dest[0] = null_byte;

  // Bias the value to get a lexicographically sortable encoding
  using UnsignedT = typename std::make_unsigned<T>::type;
  UnsignedT biased = static_cast<UnsignedT>(value) ^ (UnsignedT(1) << (sizeof(T) * 8 - 1));  // flip sign bit

  // Store bytes in big-endian order starting at dest[1]
  for (size_t i = 0; i < sizeof(T); ++i) {
    dest[1 + i] = static_cast<uint8_t>(biased >> ((sizeof(T) - 1 - i) * 8));
  }

  // Invert for descending order (excluding the null byte)
  if (def.sort_mode == SortMode::DescendingNullsFirst || def.sort_mode == SortMode::DescendingNullsLast) {
    for (size_t i = 1; i <= sizeof(T); ++i) {
      dest[i] = ~dest[i];
    }
  }
}

template void encode_int_value<int32_t>(uint8_t* dest, const std::optional<int32_t>& val,
                                        const SortColumnDefinition& def);
template void encode_int_value<int64_t>(uint8_t* dest, const std::optional<int64_t>& val,
                                        const SortColumnDefinition& def);

void encode_double_value(uint8_t* dest, const std::optional<double>& val, const SortColumnDefinition& def) {
  uint8_t null_byte = val.has_value() ? 0x00 : 0xFF;
  if (def.sort_mode == SortMode::AscendingNullsFirst || def.sort_mode == SortMode::DescendingNullsFirst) {
    null_byte = ~null_byte;
  }
  dest[0] = null_byte;

  double value;
  if (val.has_value()) {
    value = val.value();
  } else {
    value = double();
  }

  // Reinterpret double as raw 64-bit bits
  uint64_t bits;
  static_assert(sizeof(double) == sizeof(uint64_t), "Size mismatch");
  memcpy(&bits, &value, sizeof(bits));

  // Flip the bits to ensure lexicographic order matches numeric order
  if (std::signbit(value)) {
    bits = ~bits;  // Negative values are bitwise inverted
  } else {
    bits ^= 0x8000000000000000ULL;  // Flip the sign bit for positive values
  }

  // Write to buffer in big-endian order (MSB first)
  for (int i = 0; i < 8; ++i) {
    dest[1 + i] = static_cast<uint8_t>(bits >> ((7 - i) * 8));
  }

  // Descending? Invert all 8 bytes (excluding null byte)
  if (def.sort_mode == SortMode::DescendingNullsFirst || def.sort_mode == SortMode::DescendingNullsLast) {
    for (size_t i = 1; i <= 8; ++i) {
      dest[i] = ~dest[i];
    }
  }
}

void encode_string_value(uint8_t* dest, const std::optional<pmr_string>& val, const SortColumnDefinition& def) {
  if (!val.has_value()) {
    *dest = static_cast<uint8_t>(0xFF);  // set null byte to indicate NULL value
  } else {
    *dest = static_cast<uint8_t>(0x00);  // not NULL
  }
  if (def.sort_mode == SortMode::AscendingNullsFirst || def.sort_mode == SortMode::DescendingNullsFirst) {
    *dest = ~(*dest);  // Nulls first
  }

  pmr_string value;
  if (val.has_value()) {
    value = val.value();
  } else {
    value = pmr_string();  // Default empty string
  }

  size_t copy_len = std::min(value.size(), size_t(STRING_PREFIX));
  memcpy(dest + 1, value.data(), copy_len);
  memset(dest + 1 + copy_len, 0, STRING_PREFIX - copy_len);  // pad with zeroes

  // Invert the bytes for descending order
  if (def.sort_mode == SortMode::DescendingNullsFirst || def.sort_mode == SortMode::DescendingNullsLast) {
    for (size_t i = 1; i < STRING_PREFIX + 1; ++i) {
      dest[i] = ~dest[i];
    }
  }
}

void printKey(uint8_t* dest, size_t key_width, const ChunkOffset& row_idx) {
  std::cout << "[Row " << row_idx << "]: ";
  for (size_t i = 0; i < key_width; ++i) {
    uint8_t byte = dest[i];
    std::cout << static_cast<int>(byte) << " ";
  }
  std::cout << std::endl;
}

void setup_table(std::string table_name) {
  auto column_definitions = std::vector<TableColumnDefinition>{
      TableColumnDefinition{"index", DataType::Int, false},
      TableColumnDefinition{"string_col", DataType::String, false},
      TableColumnDefinition{"int_col", DataType::Int, false},
      TableColumnDefinition{"int_col_nullable", DataType::Int, true},
      TableColumnDefinition{"double_col", DataType::Double, false},
  };
  auto output = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{1024});
  Hyrise::get().storage_manager.add_table(table_name, output);
}

void fill_table(std::string table_name) {
  auto tab = Hyrise::get().storage_manager.get_table(table_name);
  auto column_count = tab->column_count();
  constexpr auto desired_chunk_count = int32_t{3};
  auto target_chunk_size = tab->target_chunk_size();
  for (int32_t i = 0; i < static_cast<int32_t>(target_chunk_size) * desired_chunk_count; ++i) {
    std::vector<AllTypeVariant> row_values;
    row_values.reserve(column_count);  // reserve space

    row_values.push_back(i);  // index
    row_values.push_back(pmr_string{"Country" + std::to_string(i % 10)});
    row_values.push_back(i < 5 ? 1 : 2);
    if (i % 2 == 0)                               // simulate nullable int
      row_values.push_back(hyrise::NullValue{});  // nullable int
    else
      row_values.push_back(i + 10000);       // nullable int
    row_values.push_back(double{i * 1.23});  // nullable double

    tab->append(row_values);
  }
}

void print_table(std::string table_name) {
  auto gt = std::make_shared<GetTable>(table_name);
  gt->never_clear_output();
  gt->execute();

  const auto print = std::make_shared<Print>(gt, PrintFlags::None);
  print->execute();
}

int main() {
  // define
  std::vector<SortColumnDefinition> sort_definitions = {
      // SortColumnDefinition{ColumnID{0}, SortMode::AscendingNullsFirst},   // col1
      SortColumnDefinition{ColumnID{2}, SortMode::AscendingNullsLast},    // col4
      SortColumnDefinition{ColumnID{3}, SortMode::DescendingNullsFirst},  // col2
      SortColumnDefinition{ColumnID{4}, SortMode::AscendingNullsLast},    // col2
  };

  const std::string table_name = "test_table";
  setup_table(table_name);
  fill_table(table_name);

  auto tab = Hyrise::get().storage_manager.get_table(table_name);

  // precompute field_width, key_width and offsets

  // based on the sizes of the columns to be sorted by, e.g. if sorting by int, string it should be [4, 8]
  std::vector<size_t> field_width;
  field_width.reserve(sort_definitions.size());
  for (const auto& def : sort_definitions) {
    const auto sort_col = def.column;
    resolve_data_type(tab->column_data_type(sort_col), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        field_width.push_back(STRING_PREFIX);  // store size of the string prefix
      } else {
        field_width.push_back(
            sizeof(ColumnDataType));  // store size of the column type, e.g. 4 for int, 8 for double, etc.
      }
    });
  }

  size_t key_width = 0;  // total width of each normalized key (width of all columns to be sorted by plus null bytes)
  for (const auto& col : field_width) {
    key_width += col + 1;  // +1 for null byte
  }
  std::vector<size_t> offsets(sort_definitions.size());  // offsets for each column in the key
  offsets[0] = 0;                                        // first column starts at offset 0
  for (size_t i = 1; i < sort_definitions.size(); ++i) {
    offsets[i] = offsets[i - 1] + field_width[i - 1] + 1;  // +1 for null byte
  }

  std::mutex print_mutex;
  std::vector<std::thread> threads;  // vector to hold threads

  // for each chunk in table
  const auto chunk_count = tab->chunk_count();
  for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    // spawn thread to sort the chunk
    threads.emplace_back([&, chunk_id]() {
      auto chunk = tab->get_chunk(chunk_id);
      auto row_count_for_chunk = chunk->size();
      std::cout << "Sorting chunk " << chunk_id << " with " << row_count_for_chunk << " rows..." << std::endl;

      // create a buffer to hold the normalized keys, like:   [key1 + key2 + key3 + ...]
      std::vector<uint8_t> buffer;
      buffer.reserve(row_count_for_chunk * key_width);

      // create key for each row in the chunk
      std::vector<ChunkOffset> row_indices(row_count_for_chunk);
      for (ChunkOffset row = ChunkOffset{0}; row < row_count_for_chunk; ++row) {
        row_indices[row] = ChunkOffset{row};  // build array of row indices

        uint8_t* key_ptr = &buffer[row * key_width];  // pointer to the start of the key for this row

        for (size_t i = 0; i < sort_definitions.size(); ++i) {
          auto sort_col = sort_definitions[i].column;
          resolve_data_type(tab->column_data_type(sort_col), [&](auto type) {
            using ColumnDataType = typename decltype(type)::type;

            const auto segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(chunk->get_segment(sort_col));
            if (!segment) {
              throw std::runtime_error("Segment type mismatch for column " + std::to_string(sort_col));
            }

            const auto value = segment->get_typed_value(ChunkOffset{row});

            // encode the value into the key based on the data type
            if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
              encode_string_value(key_ptr + offsets[i], value, sort_definitions[i]);
            } else if constexpr (std::is_same_v<ColumnDataType, double>) {
              encode_double_value(key_ptr + offsets[i], value, sort_definitions[i]);
            } else if constexpr (std::is_integral<ColumnDataType>::value && std::is_signed<ColumnDataType>::value) {
              encode_int_value(key_ptr + offsets[i], value, sort_definitions[i]);
            } else {
              throw std::logic_error("Unsupported data type for sorting: " +
                                     std::string(typeid(ColumnDataType).name()));
            }
          });
        }
      }

      // print each key
      // std::cout << "Keys for chunk " << chunk_id << ":" << std::endl;
      // for (ChunkOffset row = ChunkOffset{0}; row < row_count_for_chunk; ++row) {
      //   printKey(&buffer[row * key_width], key_width, row);
      // }
      // std::cout << "Total buffer size: " << buffer.size() << " bytes." << std::endl;

      // Sort the buffer
      auto compare_rows = [&](ChunkOffset a, ChunkOffset b) {
        uint8_t* key_a = &buffer[a * key_width];
        uint8_t* key_b = &buffer[b * key_width];

        int cmp = memcmp(key_a, key_b, key_width);
        if (cmp != 0)
          return cmp < 0;

        // std::cout << "Tie-breaker for equal keys: " << std::endl;
        // printKey(key_a, key_width, a);

        return false;

        // full fallback tie-breaker for equal keys
        auto string_segment = std::dynamic_pointer_cast<ValueSegment<pmr_string>>(chunk->get_segment(ColumnID{0}));
        const auto& val1 = string_segment->get(a);
        const auto& val2 = string_segment->get(b);

        if (val1 != val2) {
          if (sort_definitions[0].sort_mode == SortMode::AscendingNullsLast ||
              sort_definitions[0].sort_mode == SortMode::AscendingNullsFirst) {
            return val1 < val2;
          } else {
            return val1 > val2;
          }
        }

        // real fallback would look something like this:
        /*
    for (size_t i = 0; i < sort_definitions.size(); ++i) {
        const auto& def = sort_definitions[i];
        const auto& col = chunk.columns[def.column_id];
        const Value& val_a = col[a];
        const Value& val_b = col[b];

        int full_cmp = val_a.compare(val_b); // must handle string comparison, nulls, etc.
        if (full_cmp != 0)
            return def.ascending ? (full_cmp < 0) : (full_cmp > 0);
    }
    */

        return false;  // completely equal
      };

      // std::sort(row_indices.begin(), row_indices.end(), [&](ChunkOffset a, ChunkOffset b) {
      //   return memcmp(&buffer[a * key_width], &buffer[b * key_width], key_width) < 0;
      // });
      std::sort(row_indices.begin(), row_indices.end(), compare_rows);

      // Print sorted keys
      {
        std::lock_guard<std::mutex> lock(print_mutex);  // lock before printing to prevent garbled output
        std::cout << "Sorted chunk " << chunk_id << ": " << std::endl;
        auto column_count = chunk->column_count();
        for (ChunkOffset i = ChunkOffset{0}; i < 7; ++i) {
          const auto& row_idx = row_indices[i];

          // print each row in the chunk
          for (ColumnID col = ColumnID{0}; col < column_count; ++col) {
            auto segment = chunk->get_segment(col);
            std::cout << segment->operator[](row_idx) << ", ";
          }
          std::cout << std::endl;
        }
      }
    });
  }

  // Join all threads
  for (auto& t : threads) {
    t.join();
  }

  return 0;
}
