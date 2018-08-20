/*
 *  The GroupCommitLogger gathers log entries in its buffer and flushes them to disk in a binary format:
 *    1.  every LOG_INTERVAL
 *    2.  when buffer hits half its capacity
 *  Both are represented by magic numbers, which are not tested or evaluated yet.
 * 
 * 
 *  The log entries have following format:
 *         
 *         Value                : Number of Bytes                         : Description
 * 
 *     Commit Entries:
 *       - log entry type ('t') : sizeof(char)
 *       - transaction_id       : sizeof(TransactionID)
 * 
 *     Value Entries:
 *       - log entry type ('v') : sizeof(char)
 *       - transaction_id       : sizeof(TransactionID)
 *       - table_name           : table_name.size() + 1                   : string terminated with \0
 *       - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset)
 *       - NULL bitmap          : ceil(values.size() / 8.0)               : Bitmap indicating NullValues with 1
 *       - value                : length(value)
 *       - any optional values
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
 *  Possible improvements:
 *    1.  For each log entry a vector<char> is allocated to create that entry and then copy it into the buffer.
 *        Maybe allocate a big memory block once.
 *    2.  The entry vector gets resized for each value. Maybe .reserve() beforehand or calculate the number of bytes for
 *        all values by iterating over them before putting them into the entry.
 *        Then the vector needs to be resized just once.
 *    3.  While writing to disk the buffer is locked with a mutex. A second buffer could be introduced, so log calls can
 *        be processed in the second buffer while writing the first one to disk.
 */

#include "group_commit_logger.hpp"

#include <boost/serialization/variant.hpp>
#include <boost/variant/apply_visitor.hpp>

#include <fcntl.h>
#include <math.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <chrono>
#include <fstream>
#include <future>
#include <string>
#include <thread>

#include "binary_recoverer.hpp"
#include "logger.hpp"
#include "types.hpp"

namespace opossum {

// Magic number: buffer size. Buffer is flushed to disk if half of its capacity is exceeded.
// Therefore this should not be too small, since any log entry needs to fit into half of the buffer.
constexpr uint32_t LOG_BUFFER_CAPACITY = 16384;

// Magic number: time interval that triggers a flush to disk.
constexpr auto LOG_INTERVAL = std::chrono::milliseconds(1);

// LogEntry is used to create a single log entry.
// It keeps track of the cursor position while writing values into a buffer.
class LogEntry {
 public:
  uint32_t cursor{0};
  std::vector<char> data;

  explicit LogEntry(uint32_t count) { data.resize(count); }

  void resize(size_t size) {
    auto double_size = 2 * data.size();
    if (double_size >= size ) {
      data.resize(double_size);
    } else {
      data.resize(size);
    }
  }

  uint32_t size() const {
    return cursor;
  }

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

// EntryWriter is used to write multiple AllTypeVariants into an entry successively.
// It returns a boolean to indicate if something has been written into entry.
// Therefore the visitation returns false if the AllTypeVariant is a NullValue.
// This boolean then is used to set the corresponding bit in the null_value_bitmap.
// The current implementation resizes the entry for every value.
// It might improve performance to iterate twice over all values:
// Accumulate the bytes needed for all values in the first pass,
// then resize the vector and finally write the values in the second pass.
class EntryWriter : public boost::static_visitor<void> {
 public:
  explicit EntryWriter(uint32_t size) : _entry(LogEntry(size)) {}

  // The () operator is needed to apply a boost visitor. Therefore the mixing of << and () operators.
  template <typename T>
  void operator()(T v) {
    _entry.resize(_entry.data.size() + sizeof(T));
    _entry << v;
    _set_bit_in_null_bitmap(true);
  }

  template <typename T>
  EntryWriter& operator<<(const T& v) {
    _entry << v;
    return *this;
  }

  LogEntry& entry() {
    return _entry;
  }

  void create_null_bitmap(size_t number_of_values) {
    uint32_t number_of_bitmap_bytes = ceil(number_of_values / 8.0);  // uint32_t resolves to ~ 34 Billion values
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

 private:
  LogEntry _entry;
  uint32_t _null_bitmap_pos;
  uint32_t _bit_pos;
};

template <>
void EntryWriter::operator()(std::string v) {
  _entry.data.resize(_entry.data.size() + v.size() + 1);
  _entry << v;
  _set_bit_in_null_bitmap(true);
}

template <>
void EntryWriter::operator()(NullValue v) {
  _set_bit_in_null_bitmap(false);
}

void GroupCommitLogger::log_value(const TransactionID transaction_id, const std::string& table_name, const RowID row_id,
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



  auto& entry = writer.entry();
  entry.data.resize(entry.cursor);
  
  _write_to_buffer(entry.data);
}

void GroupCommitLogger::log_commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) {
  constexpr auto entry_length = sizeof(char) + sizeof(TransactionID);
  EntryWriter writer(entry_length);

  writer << 't' << transaction_id;

  _commit_callbacks.emplace_back(std::make_pair(callback, transaction_id));

  auto& entry = writer.entry();
  entry.data.resize(entry.cursor);
  
  _write_to_buffer(entry.data);
}

void GroupCommitLogger::log_load_table(const std::string& file_path, const std::string& table_name) {
  const auto entry_length = sizeof(char) + (file_path.size() + 1) + (table_name.size() + 1);
  EntryWriter writer(entry_length);

  writer << 'l' << file_path << table_name;

  auto& entry = writer.entry();
  entry.data.resize(entry.cursor);
  
  _write_to_buffer(entry.data);
}

void GroupCommitLogger::log_invalidate(const TransactionID transaction_id, const std::string& table_name,
                                       const RowID row_id) {
  const auto entry_length =
      sizeof(char) + sizeof(TransactionID) + (table_name.size() + 1) + sizeof(ChunkID) + sizeof(ChunkOffset);
  EntryWriter writer(entry_length);

  writer << 'i' << transaction_id << table_name << row_id;

  auto& entry = writer.entry();
  entry.data.resize(entry.cursor);
  
  _write_to_buffer(entry.data);
}

void GroupCommitLogger::_write_to_buffer(std::vector<char>& data) {
  // Assume that there is always enough space in the buffer, since it is flushed on hitting half its capacity
  DebugAssert(_buffer_position + data.size() < _buffer_capacity, "logging: entry does not fit into buffer");

  _buffer_mutex.lock();

  memcpy(_buffer + _buffer_position, &data[0], data.size());

  _buffer_position += data.size();
  _has_unflushed_buffer = true;

  _buffer_mutex.unlock();

  if (_buffer_position > _buffer_capacity / 2) {
    log_flush();
  }
}

void GroupCommitLogger::_write_buffer_to_logfile() {
  DebugAssert(_log_file.is_open(), "Logger: Log file not open.");
  _file_mutex.lock();
  _buffer_mutex.lock();

  _log_file.write(_buffer, _buffer_position);
  _log_file.sync();

  _buffer_position = 0u;
  _has_unflushed_buffer = false;

  for (auto& callback_tuple : _commit_callbacks) {
    callback_tuple.first(callback_tuple.second);
  }
  _commit_callbacks.clear();

  _buffer_mutex.unlock();
  _file_mutex.unlock();
}

void GroupCommitLogger::log_flush() {
  if (_has_unflushed_buffer) {
    _write_buffer_to_logfile();
  }
}

uint32_t GroupCommitLogger::recover() { return BinaryRecoverer::getInstance().recover(); }

void GroupCommitLogger::_open_logfile() {
  DebugAssert(!_log_file.is_open(), "Logger: Log file not closed before opening another one.");
  _file_mutex.lock();

  auto path = Logger::get_new_log_path();
  _log_file.open(path, std::ios::out | std::ios::binary);

  _file_mutex.unlock();
}

GroupCommitLogger::GroupCommitLogger()
    : AbstractLogger(), _buffer_capacity(LOG_BUFFER_CAPACITY), _buffer_position(0), _has_unflushed_buffer(false) {
  _buffer = reinterpret_cast<char*>(malloc(_buffer_capacity));
  memset(_buffer, 0, _buffer_capacity);

  _open_logfile();

  _flush_thread = std::make_unique<PausableLoopThread>(LOG_INTERVAL, [this](size_t count) { GroupCommitLogger::log_flush(); });
  _flush_thread->resume();
}

}  // namespace opossum
