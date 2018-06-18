/*
 *     Commit Entries:
 *       - log entry type ('t') : sizeof(char)
 *       - transaction_id       : sizeof(TransactionID)
 * 
 *     Value Entries:
 *       - log entry type ('v') : sizeof(char)
 *       - transaction_id       : sizeof(transaction_id_t)
 *       - table_name           : table_name.size() + 1, terminated with \0
 *       - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset)
 *       - NULL bitmap          : ceil(values.size() / 8.0)
 *       - value                : length(value)
 *       - any optional values
 * 
 *     Invalidation Entries:
 *       - log entry type ('i') : sizeof(char)
 *       - transaction_id       : sizeof(transaction_id_t)
 *       - table_name           : table_name.size() + 1, terminated with \0
 *       - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset) 
 *
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

#include <iostream>

#include "binary_recovery.hpp"
#include "logger.hpp"
#include "types.hpp"

namespace opossum {

// Magic number: buffer size. Buffer is flushed to disk if half of its capacity is exceeded.
// Therefore this should not be too small, since any log entry needs to fit into half the buffer.
constexpr size_t LOG_BUFFER_CAPACITY = 16384;

// Magic number: time interval that triggers a flush to disk.
constexpr auto LOG_INTERVAL = std::chrono::seconds(5);

template <>
void GroupCommitLogger::_write_value<std::string>(char*& cursor, const std::string& value) {
  value.copy(cursor, value.size());
  cursor[value.size()] = '\0';
  cursor += value.size() + 1;
}

void GroupCommitLogger::_put_into_entry(char*& entry_cursor, const char& type, const TransactionID& transaction_id) {
  _write_value<char>(entry_cursor, type);
  _write_value<TransactionID>(entry_cursor, transaction_id);
}

void GroupCommitLogger::_put_into_entry(char*& entry_cursor, const char& type, const TransactionID& transaction_id,
                                        const std::string& table_name, const RowID& row_id) {
  _put_into_entry(entry_cursor, type, transaction_id);
  _write_value<std::string>(entry_cursor, table_name);
  _write_value<ChunkID>(entry_cursor, row_id.chunk_id);
  _write_value<ChunkOffset>(entry_cursor, row_id.chunk_offset);
}

void GroupCommitLogger::commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) {
  constexpr auto entry_length = sizeof(char) + sizeof(TransactionID);
  std::vector<char> entry(entry_length);
  auto cursor = &entry[0];

  _put_into_entry(cursor, 't', transaction_id);

  _commit_callbacks.emplace_back(std::make_pair(callback, transaction_id));

  _write_to_buffer(entry);
}

// perhaps use reserve()
class value_visitor : public boost::static_visitor<bool> {
 public:
  value_visitor(std::vector<char>& entry, size_t& cursor, size_t number_of_values)
      : _entry_vector(entry), _cursor(cursor) {}

  template <typename T>
  bool operator()(T v) {
    _entry_vector.resize(_entry_vector.size() + sizeof(T));
    *reinterpret_cast<T*>(&_entry_vector[_cursor]) = v;
    _cursor += sizeof(T);

    // return that value has content
    return true;
  }

 private:
  std::vector<char>& _entry_vector;
  size_t& _cursor;
};

template <>
bool value_visitor::operator()(std::string v) {
  _entry_vector.resize(_entry_vector.size() + v.size() + 1);
  v.copy(&_entry_vector[_cursor], v.size());

  // Assume that memory is NULL, so the next byte terminates the string
  DebugAssert(_entry_vector[_cursor + v.size()] == '\0', "logging: memory is not NULL");

  _cursor += v.size() + 1;

  // return that value has content
  return true;
}

template <>
bool value_visitor::operator()(NullValue v) {
  // return that value has no content
  return false;
}

/*     Value Entries:
 *       - log entry type ('v') : sizeof(char)
 *       - transaction_id       : sizeof(transaction_id_t)
 *       - table_name           : table_name.size() + 1, terminated with \0
 *       - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset)
 *       - NULL bitmap          : ceil(values.size() / 8.0)
 *       - value                : length(value)
 *       - any optional values
 */
void GroupCommitLogger::value(const TransactionID transaction_id, const std::string table_name, const RowID row_id,
                              const std::vector<AllTypeVariant> values) {
  auto entry_length =
      sizeof(char) + sizeof(TransactionID) + (table_name.size() + 1) + sizeof(ChunkID) + sizeof(ChunkOffset);
  // TODO: use mmap ?

  // While operating on the underlying char* : Before each write into entry, resize the vector if necessary.
  std::vector<char> entry(entry_length);
  auto current_pos = &entry[0];

  _put_into_entry(current_pos, 'v', transaction_id, table_name, row_id);
  size_t cursor = current_pos - &entry[0];

  uint number_of_bitmap_bytes = ceil(values.size() / 8.0);
  entry.resize(entry.size() + number_of_bitmap_bytes);
  auto null_bitmap_pos = cursor;
  cursor += number_of_bitmap_bytes;

  value_visitor visitor(entry, cursor, values.size());
  auto bit_pos = 0u;
  --null_bitmap_pos;  // decreased, because it will be increased in the for loop
  for (auto& value : values) {
    auto has_content = boost::apply_visitor(visitor, value);

    if (bit_pos == 0) {
      ++null_bitmap_pos;
      DebugAssert(entry[null_bitmap_pos] == '\0', "logger: memory is not NULL");
    }

    if (!has_content) {
      entry[null_bitmap_pos] |= 1u << bit_pos;
    }

    bit_pos = (bit_pos + 1) % 4;
  }

  _write_to_buffer(entry);
}

void GroupCommitLogger::invalidate(const TransactionID transaction_id, const std::string table_name,
                                   const RowID row_id) {
  const auto entry_length =
      sizeof(char) + sizeof(TransactionID) + (table_name.size() + 1) + sizeof(ChunkID) + sizeof(ChunkOffset);
  std::vector<char> entry(entry_length);
  auto cursor = &entry[0];

  _put_into_entry(cursor, 'i', transaction_id, table_name, row_id);

  _write_to_buffer(entry);
}

void GroupCommitLogger::_write_to_buffer(std::vector<char>& entry) {
  // Assume that there is always enough space in the buffer, since it is flushed on hitting half its capacity
  DebugAssert(_buffer_position + entry.size() < _buffer_capacity, "logging: entry does not fit into buffer");

  _buffer_mutex.lock();

  memcpy(_buffer + _buffer_position, &entry[0], entry.size());

  _buffer_position += entry.size();
  _has_unflushed_buffer = true;

  _buffer_mutex.unlock();

  if (_buffer_position > _buffer_capacity / 2) {
    flush();
  }
}

void GroupCommitLogger::_write_buffer_to_logfile() {
  // TODO: perhaps second buffer, then buffer has not to be locked
  _file_mutex.lock();
  _buffer_mutex.lock();

  _log_file.write(_buffer, _buffer_position);
  _log_file.sync();

  _file_mutex.unlock();

  _buffer_position = 0u;
  _has_unflushed_buffer = false;

  for (auto& callback_tuple : _commit_callbacks) {
    callback_tuple.first(callback_tuple.second);
  }
  _commit_callbacks.clear();

  _buffer_mutex.unlock();
}

void GroupCommitLogger::flush() {
  if (_has_unflushed_buffer) {
    _write_buffer_to_logfile();
  }
}

void GroupCommitLogger::recover() { BinaryRecovery::getInstance().recover(); }

GroupCommitLogger::GroupCommitLogger()
    : AbstractLogger(), _buffer_capacity(LOG_BUFFER_CAPACITY), _buffer_position(0u), _has_unflushed_buffer(false) {
  _buffer = reinterpret_cast<char*>(malloc(_buffer_capacity));
  memset(_buffer, 0, _buffer_capacity);

  std::fstream last_log_number_file(Logger::directory + Logger::last_log_filename, std::ios::in);
  uint log_number;
  last_log_number_file >> log_number;
  last_log_number_file.close();
  ++log_number;

  _log_file.open(Logger::directory + Logger::filename + std::to_string(log_number), std::ios::out | std::ios::binary);

  last_log_number_file.open(Logger::directory + Logger::last_log_filename, std::ios::out | std::ofstream::trunc);
  last_log_number_file << std::to_string(log_number);
  last_log_number_file.close();

  _flush_thread = std::make_unique<PausableLoopThread>(
    LOG_INTERVAL, [this](size_t) { GroupCommitLogger::flush(); });
  _flush_thread->resume();
}

GroupCommitLogger::~GroupCommitLogger() {
  _log_file.close();
  free(_buffer);
}

}  // namespace opossum
