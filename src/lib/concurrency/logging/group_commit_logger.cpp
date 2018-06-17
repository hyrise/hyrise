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
#include "logger.hpp"
#include "binary_recovery.hpp"
#include "types.hpp"

#include <chrono>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <thread>
#include <fstream>
#include <future>
#include <string>
#include <math.h>

#include <iostream>

#include <boost/serialization/variant.hpp>
#include <boost/variant/apply_visitor.hpp>

namespace opossum {

constexpr size_t LOG_BUFFER_CAPACITY = 16384;

constexpr auto LOG_INTERVAL = std::chrono::seconds(5);

template <>
void GroupCommitLogger::_write_value<std::string>(char*& cursor, const std::string value) {
  value.copy(cursor, value.size());
  cursor[value.size()] = '\0';
  cursor += value.size() + 1;
}

void GroupCommitLogger::commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback){
  constexpr auto entry_length = sizeof(char) + sizeof(TransactionID);
  std::vector<char> entry(entry_length);
  auto cursor = &entry[0];

  _write_value<char>(cursor, 't');
  _write_value<TransactionID>(cursor, transaction_id);

  _commit_callbacks.emplace_back(std::make_pair(callback, transaction_id));

  _write_to_buffer(entry);
}

void GroupCommitLogger::_put_into_entry(char*& entry_cursor, const char &type, const TransactionID &transaction_id, const std::string &table_name, const RowID &row_id) {
  _write_value<char>(entry_cursor, type);
  _write_value<TransactionID>(entry_cursor, transaction_id);
  _write_value<std::string>(entry_cursor, table_name);
  _write_value<ChunkID>(entry_cursor, row_id.chunk_id);
  _write_value<ChunkOffset>(entry_cursor, row_id.chunk_offset);
}

class value_visitor : public boost::static_visitor<std::pair<char*, size_t>>
{
public:
  template <typename T>
  std::pair<char*, size_t> operator()(T v) const {
    auto bytes = (char*) malloc(sizeof(T));
    *(T*) bytes = v;
    return std::make_pair(bytes, sizeof(T));
  }
};

template <>
std::pair<char*, size_t> value_visitor::operator()(std::string v) const {
  auto bytes = (char*) malloc(v.size() + 1);
  v.copy(bytes, v.size());
  bytes[v.size()] = '\0';
  return std::make_pair(bytes, v.size() + 1); 
}

/*
TODO:
*  use vecotr, don't foget resize(). perhaps use reserve()
*  vec.resize(vec.size()+sizeof(T))
*  &vec[i]   isse char*

  buffer an visitor übergeben
*
*/


 /*     Value Entries:
 *       - log entry type ('v') : sizeof(char)
 *       - transaction_id       : sizeof(transaction_id_t)
 *       - table_name           : table_name.size() + 1, terminated with \0
 *       - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset)
 *       - NULL bitmap          : ceil(values.size() / 8.0)
 *       - value                : length(value)
 *       - any optional values
 */
void GroupCommitLogger::value(const TransactionID transaction_id, const std::string table_name, const RowID row_id, const std::vector<AllTypeVariant> values){

  auto entry_length = sizeof(char) + sizeof(TransactionID) + (table_name.size() + 1) + sizeof(ChunkID) + sizeof(ChunkOffset);
  // TODO: use mmap ?

  std::vector<std::pair<char*, size_t>> value_binaries;

  for (auto &value : values) {
    auto value_binary = boost::apply_visitor( value_visitor(), value );
    value_binaries.push_back(value_binary);
    entry_length += value_binary.second;
  }

  // Bitmap of NULL values
  uint null_bitmap_number_of_bytes = ceil(value_binaries.size() / 8.0);
  entry_length += null_bitmap_number_of_bytes;

  // While operating on the underlying char* : Before each write into entry, resize the vector if necessary.
  std::vector<char> entry(entry_length);
  auto current_pos = &entry[0];

  _put_into_entry(current_pos, 'v', transaction_id, table_name, row_id);

  // TODO: write bitmap of NULL values
  // auto bitmap_pos = current_pos;
  // char bitmap[null_bitmap_number_of_bytes];
  for (auto i = 0u; i < null_bitmap_number_of_bytes; ++i) {
    _write_value<char>(current_pos, '\0');
  }
  // memcpy( bitmap_pos, bitmap, null_bitmap_number_of_bytes );
  // current_pos += null_bitmap_number_of_bytes;

  for (auto & binary : value_binaries){
    memcpy( current_pos, binary.first, binary.second );
    current_pos += binary.second;
  }

  _write_to_buffer(entry);
}

void GroupCommitLogger::invalidate(const TransactionID transaction_id, const std::string table_name, const RowID row_id){
  const auto entry_length = sizeof(char) + sizeof(TransactionID) + (table_name.size() + 1) + sizeof(ChunkID) + sizeof(ChunkOffset);
  std::vector<char> entry(entry_length);
  auto cursor = &entry[0];

  _put_into_entry(cursor, 'i', transaction_id, table_name, row_id);

  _write_to_buffer(entry);
}

void GroupCommitLogger::_write_to_buffer(std::vector<char> &entry) {
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

void GroupCommitLogger::_write_buffer_to_logfile(){
  //TODO: perhaps second buffer, then buffer has not to be locked
  _file_mutex.lock();
  _buffer_mutex.lock();

  _log_file.write(_buffer, _buffer_position);
  _log_file.sync();

  _file_mutex.unlock();

  _buffer_position = 0u;
  _has_unflushed_buffer = false;

  for (auto &callback_tuple : _commit_callbacks) {
    callback_tuple.first(callback_tuple.second);
  }
  _commit_callbacks.clear();

  _buffer_mutex.unlock();
}

void GroupCommitLogger::_flush_to_disk_after_timeout(){
  // TODO: while loop should be exited on program termination
  while (true) {
    std::this_thread::sleep_for(LOG_INTERVAL);
    flush();
  }
}

void GroupCommitLogger::flush(){
  if (_has_unflushed_buffer) {
    _write_buffer_to_logfile();
  }
}

void GroupCommitLogger::recover() {
  BinaryRecovery::getInstance().recover();
}


GroupCommitLogger::GroupCommitLogger()
: AbstractLogger()
, _buffer_capacity(LOG_BUFFER_CAPACITY)
, _buffer_position(0u)
, _has_unflushed_buffer(false) {
  _buffer = (char*)malloc(_buffer_capacity);
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

  // TODO: thread should be joined at the end of the program
  std::thread t1(&GroupCommitLogger::_flush_to_disk_after_timeout, this);
  t1.detach();
};

GroupCommitLogger::~GroupCommitLogger() {
  _log_file.close();
  free(_buffer);
}

}  // namespace opossum
