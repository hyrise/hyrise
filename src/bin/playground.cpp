#include <string.h>

#include <algorithm>
#include <chrono>
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

void encode_float_value(uint8_t* dest, const std::optional<float>& val, const SortColumnDefinition& def) {
  double value;
  if (val.has_value()) {
    value = static_cast<double>(val.value());
  }

  // Use the same encoding as for double, but with float value
  encode_double_value(dest, std::optional<double>(value), def);
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
      TableColumnDefinition{"float_col", DataType::Float, false},
      TableColumnDefinition{"long_col", DataType::Long, false},
  };
  auto output = std::make_shared<Table>(column_definitions, TableType::Data);
  Hyrise::get().storage_manager.add_table(table_name, output);
}

void fill_table(const std::string table_name) {
  const auto tab = Hyrise::get().storage_manager.get_table(table_name);
  const auto column_count = tab->column_count();
  constexpr auto desired_chunk_count = int32_t{100};
  const auto target_chunk_size = static_cast<int32_t>(tab->target_chunk_size());
  const auto max_int = static_cast<int64_t>(std::numeric_limits<int32_t>::max());
  for (int32_t i = 0; i < target_chunk_size * desired_chunk_count; ++i) {
    std::vector<AllTypeVariant> row_values;
    row_values.reserve(column_count);  // reserve space

    row_values.push_back(i);  // index
    row_values.push_back(pmr_string{"Country" + std::to_string(i % 10)});
    row_values.push_back(!(i % target_chunk_size) ? 1 : 2);
    if (i % 2 == 0)                               // simulate nullable int
      row_values.push_back(hyrise::NullValue{});  // nullable int
    else
      row_values.push_back(i + 10000);                  // nullable int
    row_values.push_back(double{i * 1.23});             // nullable double
    row_values.push_back(float{i + 0.1f});              // nullable float
    row_values.push_back(int64_t{i + 1000} + max_int);  // long

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

void merge(RowIDPosList& data, const std::function<bool(const RowID&, const RowID&)>& compare, int left, int mid,
           int right) {
  std::vector<RowID> temp;
  temp.reserve(right - left);

  int i = left, j = mid;

  while (i < mid && j < right) {
    if (compare(data[i], data[j])) {
      temp.push_back(std::move(data[i++]));
    } else {
      temp.push_back(std::move(data[j++]));
    }
  }
  while (i < mid)
    temp.push_back(std::move(data[i++]));
  while (j < right)
    temp.push_back(std::move(data[j++]));

  std::move(temp.begin(), temp.end(), data.begin() + left);
}

void merge_sort(RowIDPosList& data, const std::function<bool(const RowID&, const RowID&)>& compare, int left, int right,
                int depth = 0) {
  if (right - left <= 1)
    return;

  int mid = left + (right - left) / 2;

  // Limit parallelism depth to avoid oversubscription
  if (depth < static_cast<int>(std::log2(std::thread::hardware_concurrency()))) {
    std::thread left_thread(merge_sort, std::ref(data), compare, left, mid, depth + 1);
    merge_sort(data, compare, mid, right, depth + 1);
    left_thread.join();
  } else {
    merge_sort(data, compare, left, mid, depth + 1);
    merge_sort(data, compare, mid, right, depth + 1);
  }

  merge(data, compare, left, mid, right);
}

void sort_test(int run_name = 0, bool use_merge_sort = true, bool print_result = false) {
  // define
  std::vector<SortColumnDefinition> sort_definitions = {
      // SortColumnDefinition{ColumnID{0}, SortMode::DescendingNullsFirst},
      SortColumnDefinition{ColumnID{2}, SortMode::AscendingNullsLast},
      SortColumnDefinition{ColumnID{3}, SortMode::AscendingNullsLast},
      // SortColumnDefinition{ColumnID{4}, SortMode::AscendingNullsLast},
      // SortColumnDefinition{ColumnID{5}, SortMode::DescendingNullsFirst},
      // SortColumnDefinition{ColumnID{6}, SortMode::DescendingNullsFirst},
  };

  const std::string table_name = "test_table" + std::to_string(run_name);
  setup_table(table_name);
  fill_table(table_name);

  auto tab = Hyrise::get().storage_manager.get_table(table_name);
  const auto chunk_count = tab->chunk_count();
  const auto row_count = tab->row_count();
  // std::cout << "Table '" << table_name << "' has " << row_count << " rows over " << chunk_count << " chunks."
  //           << std::endl;

  // === precompute field_width, key_width and key_offsets ===

  // based on the sizes of the columns to be sorted by, e.g. if sorting by int, string it should be [4, 8]
  std::vector<size_t> field_width;
  field_width.reserve(sort_definitions.size());
  for (const auto& def : sort_definitions) {
    const auto sort_col = def.column;
    resolve_data_type(tab->column_data_type(sort_col), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        field_width.push_back(STRING_PREFIX);  // store size of the string prefix
      } else if constexpr (std::is_same_v<ColumnDataType, float>) {
        field_width.push_back(sizeof(double));  // encode float as double for sorting
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

  /**
   * Offsets for each column in the key, i.e. `key_offsets[i]` is the offset of the i-th column in the key.
   * This means, that `buffer[key_offsets[i]]` is the location of the i-th column's value in the key.
  */
  std::vector<size_t> key_offsets(sort_definitions.size());
  key_offsets[0] = 0;  // first column starts at offset 0
  for (size_t i = 1; i < sort_definitions.size(); ++i) {
    key_offsets[i] = key_offsets[i - 1] + field_width[i - 1] + 1;  // +1 for null byte
  }

  std::mutex print_mutex;
  std::vector<std::thread> threads;  // vector to hold threads

  auto key_buffer = std::vector<uint8_t>();  // buffer to hold all keys for sorting
  auto chunk_sizes = std::vector<size_t>();  // sizes of each chunk in the table

  for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = tab->get_chunk(chunk_id);
    auto row_count_for_chunk = chunk->size();

    chunk_sizes.push_back(row_count_for_chunk);
  }
  auto total_buffer_size = size_t{0};  // total size of the key buffer
  for (size_t i = 0; i < chunk_sizes.size(); ++i) {
    auto chunk_size = chunk_sizes[i];
    total_buffer_size += chunk_size * key_width;  // total size of the keys for this chunk
  }
  key_buffer.reserve(total_buffer_size);  // reserve space for all keys

  RowIDPosList row_ids(row_count);  // vector to hold row IDs for each row in the table

  auto row_id_offsets = std::vector<size_t>();  // offsets for each chunk's row IDs
  row_id_offsets.reserve(chunk_count);          // reserve space for offsets
  row_id_offsets[0] = 0;
  for (ChunkID chunk_id = ChunkID{1}; chunk_id < chunk_count; ++chunk_id) {
    auto offset = row_id_offsets[chunk_id - 1] + chunk_sizes[chunk_id - 1];
    row_id_offsets[chunk_id] = offset;
  }

  // for each chunk in table
  for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    // spawn thread to generate keys
    threads.emplace_back([&, chunk_id]() {
      auto chunk = tab->get_chunk(chunk_id);
      auto row_count_for_chunk = chunk_sizes[chunk_id];

      // buffer points to the start of this chunk's keys in the global key_buffer
      uint8_t* buffer = &key_buffer[row_id_offsets[chunk_id] * key_width];

      // create key for each row in the chunk
      for (ChunkOffset row = ChunkOffset{0}; row < row_count_for_chunk; ++row) {
        row_ids[row_id_offsets[chunk_id] + row] = RowID{chunk_id, row};  // build array of row ids

        uint8_t* key_ptr = &buffer[row * key_width];  // pointer to the start of the key for this row

        // TODO: How to handle huge number of keycolumns? maybe max. number for key generation
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
              encode_string_value(key_ptr + key_offsets[i], value, sort_definitions[i]);
            } else if constexpr (std::is_same_v<ColumnDataType, double>) {
              encode_double_value(key_ptr + key_offsets[i], value, sort_definitions[i]);
            } else if constexpr (std::is_same_v<ColumnDataType, float>) {
              encode_float_value(key_ptr + key_offsets[i], value, sort_definitions[i]);
            } else if constexpr (std::is_integral<ColumnDataType>::value && std::is_signed<ColumnDataType>::value) {
              encode_int_value(key_ptr + key_offsets[i], value, sort_definitions[i]);
            } else {
              throw std::logic_error("Unsupported data type for sorting: " +
                                     std::string(typeid(ColumnDataType).name()));
            }
          });
        }
      }

      /*
      {
        // print the key for debugging
        std::lock_guard<std::mutex> lock(print_mutex);  // lock mutex
        // print each key
        std::cout << "Keys for chunk " << chunk_id << ":" << std::endl;
        for (ChunkOffset row = ChunkOffset{0}; row < row_count_for_chunk; ++row) {
          printKey(&buffer[row * key_width], key_width, row);
        }
      }
      */
    });
  }

  // Join all threads
  for (auto& t : threads) {
    t.join();
  }

  // Sort the buffer
  // TODO: if two different strings generate the same keypart, we have to compare the actual stings
  // e.g. (AAAAAAAAA, 10) should be before (AAAAAAAAB, 4) but would not be, if don't handle this case correctly
  auto compare_rows = [&](const RowID a, const RowID b) {
    uint8_t* key_a = &key_buffer[(row_id_offsets[a.chunk_id] + a.chunk_offset) * key_width];
    uint8_t* key_b = &key_buffer[(row_id_offsets[b.chunk_id] + b.chunk_offset) * key_width];

    int cmp = memcmp(key_a, key_b, key_width);
    if (cmp != 0)
      return cmp < 0;

    // std::cout << "Tie-breaker for equal keys: " << std::endl;
    // printKey(key_a, key_width, a);

    return false;

    // full fallback tie-breaker for equal keys
    // auto string_segment = std::dynamic_pointer_cast<ValueSegment<pmr_string>>(chunk->get_segment(ColumnID{0}));
    // const auto& val1 = string_segment->get(a);
    // const auto& val2 = string_segment->get(b);

    // if (val1 != val2) {
    //   if (sort_definitions[0].sort_mode == SortMode::AscendingNullsLast ||
    //       sort_definitions[0].sort_mode == SortMode::AscendingNullsFirst) {
    //     return val1 < val2;
    //   } else {
    //     return val1 > val2;
    //   }
    // }

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

  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  if (use_merge_sort) {
      merge_sort(row_ids, compare_rows, 0, row_ids.size());
  } else {
    std::sort(row_ids.begin(), row_ids.end(), compare_rows);
  }
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

  // std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count()
  //           << "[µs]" << std::endl;
  // std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count()
  //           << "[ns]" << std::endl;
  double ms = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(end - begin).count();
  std::cout << "Time difference = " << std::fixed << std::setprecision(2) << ms << "[ms]" << std::endl;

  if (print_result) {
    // Print sorted keys
    auto column_count = tab->get_chunk(ChunkID{0})->column_count();
    // print each column once to show PERF warning
    for (ColumnID col = ColumnID{0}; col < column_count; ++col) {
      std::cout << "Dummy print: " << tab->get_chunk(ChunkID{0})->get_segment(col)->operator[](ChunkOffset{0})
                << std::endl;
    }

    for (ChunkOffset i = ChunkOffset{0}; i < 9; ++i) {
      const auto& row_idx = row_ids[i];

      // print each row in the chunk
      std::cout << "[" << row_idx.chunk_id << "." << row_idx.chunk_offset << "]: ";
      for (ColumnID col = ColumnID{0}; col < column_count; ++col) {
        auto segment = tab->get_chunk(row_idx.chunk_id)->get_segment(col);
        std::cout << segment->operator[](row_idx.chunk_offset) << ", ";
      }
      std::cout << std::endl;
    }
  }
}

int main() {
  std::cout << "=== Playground ===" << std::endl;

  // Run the sort test
  int run_name = 0;
  size_t num_runs = 5;

  std::cout << "=== merge sort ===" << std::endl;
  for (size_t i = 0; i < num_runs; ++i) {
    sort_test(run_name, true);
    run_name++;
  }

  std::cout << "\n=== std::sort ===" << std::endl;
  for (size_t i = 0; i < num_runs; ++i) {
    sort_test(run_name, false);
    run_name++;
  }

  return 0;
}
