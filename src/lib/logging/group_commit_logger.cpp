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

void GroupCommitLogger::log_value(const TransactionID transaction_id, const std::string& table_name, const RowID row_id,
                                  const std::vector<AllTypeVariant>& values) {
  const auto& data = _formatter->value_entry(transaction_id, table_name, row_id, values);
  _write_to_buffer(data);
}

void GroupCommitLogger::log_commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) {
  _commit_callbacks.emplace_back(std::make_pair(callback, transaction_id));

  const auto& data = _formatter->commit_entry(transaction_id);
  _write_to_buffer(data);
}

void GroupCommitLogger::log_load_table(const std::string& file_path, const std::string& table_name) {
  const auto& data = _formatter->load_table_entry(file_path, table_name);
  _write_to_buffer(data);
  log_flush();
}

void GroupCommitLogger::log_invalidate(const TransactionID transaction_id, const std::string& table_name,
                                       const RowID row_id) {
  const auto& data = _formatter->invalidate_entry(transaction_id, table_name, row_id); 
  _write_to_buffer(data);
}

void GroupCommitLogger::_write_to_buffer(const std::vector<char>& data) {
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

void GroupCommitLogger::log_flush() {
  if (_has_unflushed_buffer) {
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
}

void GroupCommitLogger::_open_logfile() {
  DebugAssert(!_log_file.is_open(), "Logger: Log file not closed before opening another one.");
  _file_mutex.lock();

  auto path = Logger::get_new_log_path();
  _log_file.open(path, std::ios::out | std::ios::binary);

  _file_mutex.unlock();
}

GroupCommitLogger::GroupCommitLogger(std::unique_ptr<AbstractFormatter> formatter)
    : AbstractLogger(std::move(formatter)), _buffer_capacity(LOG_BUFFER_CAPACITY), _buffer_position(0)
    , _has_unflushed_buffer(false) {
  _buffer = reinterpret_cast<char*>(malloc(_buffer_capacity));
  memset(_buffer, 0, _buffer_capacity);

  _open_logfile();

  _flush_thread = std::make_unique<PausableLoopThread>(LOG_INTERVAL, [this](size_t count) { GroupCommitLogger::log_flush(); });
  _flush_thread->resume();
}

}  // namespace opossum
