/*
 *  Formatter that creates binary log entries.
 * 
 *  The log entries have following format:
 *         
 *         Value                : Number of Bytes                         : Description
 * 
 *     Commit Entries:
 *       - log entry type ('c') : sizeof(char)
 *       - transaction_id       : sizeof(TransactionID)
 * 
 *     Value Entries:
 *       - log entry type ('v') : sizeof(char)
 *       - transaction_id       : sizeof(TransactionID)
 *       - table_name           : table_name.size() + 1                   : string terminated with \0
 *       - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset)
 *       - NULL bitmap          : ceil(values.size() / 8.0)               : Bitmap indicating NullValues with 1
 *       - value                : length(value)
 *       - value ...
 * 
 *     Invalidation Entries:
 *       - log entry type ('i') : sizeof(char)
 *       - transaction_id       : sizeof(TransactionID)
 *       - table_name           : table_name.size() + 1                   : string terminated with \0
 *       - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset) 
 *
 *     Load Table Entries:
 *       - log entry type ('l') : sizeof(char)
 *       - file_path            : file_path.size() + 1                    : string terminated with \0
 *       - table_name           : table_name.size() + 1                   : string terminated with \0
 * 
 * 
 *  Potential Improvements:
 *    1.  For each log entry a vector<char> is allocated to create that entry and then copy it into the buffer.
 *        Maybe allocate a big memory block once.
 *    2.  The entry vector gets resized for each value. Maybe .reserve() beforehand or calculate the number of bytes for
 *        all values by iterating over them before putting them into the entry.
 *        Then the vector needs to be resized just once.
 */

#include "binary_log_formatter.hpp"

#include "all_type_variant.hpp"
#include "binary_recoverer.hpp"
#include "types.hpp"

namespace opossum {

// LogEntry is used to create a single log entry.
// It keeps track of the cursor position while writing values into a buffer.
class LogEntry {
 private:
  friend class EntryWriter;
  uint32_t cursor{0};
  std::vector<char> data;

  explicit LogEntry(uint32_t count) { data.resize(count); }

  void resize(size_t size) {
    auto double_size = 2 * data.size();
    if (double_size >= size) {
      data.resize(double_size);
    } else {
      data.resize(size);
    }
  }

  uint32_t size() const { return cursor; }

  // this operator has an out of class specialization for strings
  template <typename T>
  LogEntry& operator<<(const T& value) {
    DebugAssert(cursor + sizeof(T) <= data.size(), "Logger: value does not fit into vector, call resize() beforehand");
    *reinterpret_cast<T*>(&data[cursor]) = value;
    cursor += sizeof(T);
    return *this;
  }
};

template <>
// clang-format off
LogEntry& LogEntry::operator<< <std::string>(const std::string& value) {  // NOLINT
  // clang-format on
  DebugAssert(cursor + value.size() < data.size(), "Logger: value does not fit into vector, call resize() beforehand");

  value.copy(&data[cursor], value.size());

  cursor += value.size() + 1;

  return *this;
}

// EntryWriter is used to create a log entry.
// The log information then gets written successively.
// Except for value insertions, all data is written by the << operator.
// AllTypeVariants from insertions are written by applying a boost visitor.
// The current implementation resizes the entry for every value.
// It might improve performance to iterate twice over all values:
// Accumulate the bytes needed for all values in the first pass,
// then resize the vector and finally write the values in the second pass.
class EntryWriter : public boost::static_visitor<void> {
 public:
  explicit EntryWriter(uint32_t size) : _entry(LogEntry(size)) {}

  // The () operator is needed to apply a boost visitor. Therefore the mixing of << and () operators.
  // This operator has an out of class specialization for strings and NullValues.
  template <typename T>
  void operator()(T v) {
    _entry.resize(_entry.size() + sizeof(T));
    _entry << v;
    _set_bit_in_null_bitmap(true);
  }

  template <typename T>
  EntryWriter& operator<<(const T& v) {
    _entry << v;
    return *this;
  }

  LogEntry& entry() { return _entry; }

  void create_null_bitmap(size_t number_of_values) {
    auto number_of_bitmap_bytes = BinaryLogFormatter::null_bitmap_size(number_of_values);
    _entry.data.resize(_entry.data.size() + number_of_bitmap_bytes);
    _null_bitmap_pos = _entry.cursor;
    _bit_pos = 0;
    _entry.cursor += number_of_bitmap_bytes;
  }

  void _set_bit_in_null_bitmap(bool has_content) {
    // Set corresponding bit in bitmap to 1 if the value is a NullValue
    if (!has_content) {
      _entry.data[_null_bitmap_pos] |= 1u << _bit_pos;
    }

    // Increase bit_pos for next value and increase null_bitmap_pos every eighth values to address the next byte
    if (_bit_pos == 7) {
      ++_null_bitmap_pos;
    }
    _bit_pos = (_bit_pos + 1) % 8;
  }

  std::vector<char>& release_data() {
    _entry.data.resize(_entry.cursor);
    return _entry.data;
  }

 private:
  LogEntry _entry;
  uint32_t _null_bitmap_pos;
  uint32_t _bit_pos;
};

template <>
void EntryWriter::operator()(std::string v) {
  _entry.data.resize(_entry.size() + v.size() + 1);
  _entry << v;
  _set_bit_in_null_bitmap(true);
}

template <>
void EntryWriter::operator()(NullValue v) {
  _set_bit_in_null_bitmap(false);
}

// uint32_t resolves to ~ 4 Billion values
uint32_t BinaryLogFormatter::null_bitmap_size(uint32_t number_of_values) { return ceil(number_of_values / 8.0); }

std::vector<char> BinaryLogFormatter::create_commit_entry(const TransactionID transaction_id) {
  constexpr auto entry_length = sizeof(char) + sizeof(TransactionID);
  EntryWriter writer(entry_length);

  writer << 'c' << transaction_id;

  return writer.release_data();
}

std::vector<char> BinaryLogFormatter::create_value_entry(const TransactionID transaction_id,
                                                         const std::string& table_name, const RowID row_id,
                                                         const std::vector<AllTypeVariant>& values) {
  // This is the entry length up to the ChunkOffset.
  // The entry then gets resized for the null value bitmap and each value
  auto entry_length =
      sizeof(char) + sizeof(TransactionID) + (table_name.size() + 1) + sizeof(ChunkID) + sizeof(ChunkOffset);

  EntryWriter writer(entry_length);

  writer << 'v' << transaction_id << table_name << row_id;

  writer.create_null_bitmap(values.size());

  for (auto& value : values) {
    boost::apply_visitor(writer, value);
  }

  return writer.release_data();
}

std::vector<char> BinaryLogFormatter::create_invalidation_entry(const TransactionID transaction_id,
                                                                const std::string& table_name, const RowID row_id) {
  const auto entry_length =
      sizeof(char) + sizeof(TransactionID) + (table_name.size() + 1) + sizeof(ChunkID) + sizeof(ChunkOffset);
  EntryWriter writer(entry_length);

  writer << 'i' << transaction_id << table_name << row_id;

  return writer.release_data();
}

std::vector<char> BinaryLogFormatter::create_load_table_entry(const std::string& file_path,
                                                              const std::string& table_name) {
  const auto entry_length = sizeof(char) + (file_path.size() + 1) + (table_name.size() + 1);
  EntryWriter writer(entry_length);

  writer << 'l' << file_path << table_name;

  return writer.release_data();
}

AbstractRecoverer& BinaryLogFormatter::get_recoverer() { return BinaryRecoverer::get(); }

}  // namespace opossum
