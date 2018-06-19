/*
 *  TODO: use mmap ?
 * 
 * 
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
void GroupCommitLogger::_write_value<std::string>(std::vector<char>& vector, size_t& cursor, const std::string& value) {
  value.copy(&vector[cursor], value.size());

  // Assume that the next byte is NULL so the string gets terminated
  DebugAssert(vector[cursor + value.size()] == '\0', "Logger: Byte is not NULL initiated");

  cursor += value.size() + 1;
}

void GroupCommitLogger::_put_into_entry(std::vector<char>& entry, size_t& entry_cursor, const char& type,
                                        const TransactionID& transaction_id) {
  _write_value<char>(entry, entry_cursor, type);
  _write_value<TransactionID>(entry, entry_cursor, transaction_id);
}

void GroupCommitLogger::_put_into_entry(std::vector<char>& entry, const char& type, 
                                        const TransactionID& transaction_id) {
  size_t cursor{0};
  _put_into_entry(entry, cursor, type, transaction_id);
}

void GroupCommitLogger::_put_into_entry(std::vector<char>& entry, size_t& entry_cursor, const char& type,
                                        const TransactionID& transaction_id, const std::string& table_name,
                                        const RowID& row_id) {
  _put_into_entry(entry, entry_cursor, type, transaction_id);
  _write_value<std::string>(entry, entry_cursor, table_name);
  _write_value<ChunkID>(entry, entry_cursor, row_id.chunk_id);
  _write_value<ChunkOffset>(entry, entry_cursor, row_id.chunk_offset);
}

void GroupCommitLogger::_put_into_entry(std::vector<char>& entry, const char& type, const TransactionID& transaction_id,
                                        const std::string& table_name, const RowID& row_id) {
  size_t cursor{0};
  _put_into_entry(entry, cursor, type, transaction_id, table_name, row_id);
}

void GroupCommitLogger::commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) {
  constexpr auto entry_length = sizeof(char) + sizeof(TransactionID);
  std::vector<char> entry(entry_length);

  _put_into_entry(entry, 't', transaction_id);

  _commit_callbacks.emplace_back(std::make_pair(callback, transaction_id));

  _write_to_buffer(entry);
}

// ValueVisitor is used to write multiple AllTypeVariants into an entry successively.
// Returns boolean to indicate if something has been written into entry.
// Therefore the visitation returns false if the AllTypeVariant is a NullValue. 
// This is used to set the corresponding bit in the null_value_bitmap.
// The current implementation resizes the entry for every value.
// It might improve performance to iterate twice over all values: 
// Acumulate the bytes needed for all values in the first pass,
// resize the vector and write the values in the second pass.
class ValueVisitor : public boost::static_visitor<bool> {
 public:
  ValueVisitor(std::vector<char>& entry, size_t& cursor)
  : _entry_vector(entry), _cursor(cursor) {}

  template <typename T>
  bool operator()(T v) {
    _entry_vector.resize(_entry_vector.size() + sizeof(T));
    *reinterpret_cast<T*>(&_entry_vector[_cursor]) = v;
    _cursor += sizeof(T);
    return true;
  }

  std::vector<char>& _entry_vector;
  size_t& _cursor;
};

template <>
bool ValueVisitor::operator()(std::string v) {
  _entry_vector.resize(_entry_vector.size() + v.size() + 1);
  v.copy(&_entry_vector[_cursor], v.size());

  // Assume that the next byte is NULL so the string gets terminated
  DebugAssert(_entry_vector[_cursor + v.size()] == '\0', "Logger: Byte is not NULL initiated");

  _cursor += v.size() + 1;

  return true;
}

template <>
bool ValueVisitor::operator()(NullValue v) {
  return false;
}

void GroupCommitLogger::value(const TransactionID transaction_id, const std::string table_name, const RowID row_id,
                              const std::vector<AllTypeVariant> values) {
  // This is the entry length up to the ChunkOffset,
  // the entry then gets resized for the null value bitmap and each value
  auto entry_length =
      sizeof(char) + sizeof(TransactionID) + (table_name.size() + 1) + sizeof(ChunkID) + sizeof(ChunkOffset);

  std::vector<char> entry(entry_length);
  size_t cursor = 0;
  
  _put_into_entry(entry, cursor, 'v', transaction_id, table_name, row_id);

  uint number_of_bitmap_bytes = ceil(values.size() / 8.0);
  entry.resize(entry.size() + number_of_bitmap_bytes);
  auto null_bitmap_pos = cursor;
  cursor += number_of_bitmap_bytes;

  DebugAssert(entry[null_bitmap_pos] == '\0', "Logger: memory is not NULL");

  ValueVisitor visitor(entry, cursor);
  auto bit_pos = 0u;
  for (auto& value : values) {
    auto has_content = boost::apply_visitor(visitor, value);

    // Set corresponding bit in bitmap to 1 if the value is a NullValue
    if (!has_content) {
      entry[null_bitmap_pos] |= 1u << bit_pos;
    }

    // increase bit_pos for next value and increase null_bitmap_pos every eigth values 
    if (bit_pos == 7) {
      ++null_bitmap_pos;
      DebugAssert(entry[null_bitmap_pos] == '\0', "Logger: memory is not NULL");
    }
    bit_pos = (bit_pos + 1) % 8;
  }

  _write_to_buffer(entry);
}

void GroupCommitLogger::invalidate(const TransactionID transaction_id, const std::string table_name,
                                   const RowID row_id) {
  const auto entry_length =
      sizeof(char) + sizeof(TransactionID) + (table_name.size() + 1) + sizeof(ChunkID) + sizeof(ChunkOffset);
  std::vector<char> entry(entry_length);

  _put_into_entry(entry, 'i', transaction_id, table_name, row_id);

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
    : AbstractLogger(), _buffer_capacity(LOG_BUFFER_CAPACITY), _buffer_position(0), _has_unflushed_buffer(false) {
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

  _flush_thread = std::make_unique<LoopThread>(LOG_INTERVAL, [this]() { GroupCommitLogger::flush(); });
}

GroupCommitLogger::~GroupCommitLogger() {
  _log_file.close();
  free(_buffer);
}

}  // namespace opossum
