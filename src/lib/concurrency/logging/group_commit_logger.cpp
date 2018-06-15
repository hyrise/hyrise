/*
 *     Commit Entries:
 *       - log entry type ('t') : sizeof(char)
 *       - transaction_id       : sizeof(TransactionID)
 * 
 *     Value Entries:
 *       - log entry type ('v') : sizeof(char)
 *       - transaction_id       : sizeof(transaction_id_t)
 *       - table_name.size()    : sizeof(size_t)             --> what is max table_name size?
 *       - table_name           : table_name.size()
 *       - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset)
 *       - length(value)        : sizeof(size_t)
 *       - value                : length(value)
 *       - { length(value) + value } *
 *       - end indicator        : sizeof(int)
 *         (-> length(value) = 0)
 * 
 *     Invalidation Entries:
 *       - log entry type ('i') : sizeof(char)
 *       - transaction_id       : sizeof(TransactionID)
 *       - table_name.size()    : sizeof(size_t)             --> what is max table_name size?
 *       - table_name           : table_name.size()
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
#include <sstream>
#include <thread>
#include <fstream>
#include <future>
#include <string>

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
  auto entry = (char*) malloc(entry_length);

  // _write_value<char>(entry, 't');
  // _write_value<TransactionID>(entry, transaction_id);
  *entry = 't';
  *(TransactionID*) (entry + sizeof(char)) = transaction_id;

  _commit_callbacks.emplace_back(std::make_pair(callback, transaction_id));

  _write_to_buffer(entry, entry_length);

  free(entry);
}

char* GroupCommitLogger::_put_into_entry(char* entry, const TransactionID &transaction_id, const std::string &table_name, const RowID &row_id) {
  _write_value<TransactionID>(entry, transaction_id);
  _write_value<size_t>(entry, table_name.size());
  _write_value<std::string>(entry, table_name);
  _write_value<ChunkID>(entry, row_id.chunk_id);
  _write_value<ChunkOffset>(entry, row_id.chunk_offset);

  return entry;
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


 /*     Value Entries:    TODO!!!
 *       - log entry type ('v') : sizeof(char)
 *       - transaction_id       : sizeof(transaction_id_t)
 *       - table_name.size()    : sizeof(size_t)             --> what is max table_name size?
 *       - table_name           : table_name.size() + 1, terminated with \0
 *       - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset)
 *       - NULL bitmap          : ceil(values.size() / 8)
 *       - value                : length(value)
 *       - any optional values
 */
void GroupCommitLogger::value(const TransactionID transaction_id, const std::string table_name, const RowID row_id, const std::vector<AllTypeVariant> values){

  auto entry_length = sizeof(char) + sizeof(TransactionID) + sizeof(size_t) + (table_name.size() + 1) + sizeof(ChunkID) + sizeof(ChunkOffset) + sizeof(size_t);
  // TODO: use mmap ?

  std::vector<std::pair<char*, size_t>> value_binaries;

  for (auto &value : values) {
    auto value_binary = boost::apply_visitor( value_visitor(), value );
    value_binaries.push_back(value_binary);
    entry_length += value_binary.second;

    // std::cout << value_binary.second << std::endl;
    // for (auto i = 0u; i < value_binary.second; ++i) {
    //   std::cout << value_binary.first[i];
    // }
    // std::cout << std::endl;
  }

  auto entry = (char*) malloc(entry_length);

  *entry = 'v';
  auto current_pos = entry + sizeof(char);

  current_pos = _put_into_entry(current_pos, transaction_id, table_name, row_id);

  for (auto & binary : value_binaries){
    memcpy( current_pos, binary.first, binary.second );
    current_pos += binary.second;
  }

  // end indicator: length of next value = 0
  *(size_t*) current_pos = 0;

  _write_to_buffer(entry, entry_length);
  
  free(entry);
}

void GroupCommitLogger::invalidate(const TransactionID transaction_id, const std::string table_name, const RowID row_id){
  const auto entry_length = sizeof(char) + sizeof(TransactionID) + sizeof(size_t) + table_name.size() + sizeof(ChunkID) + sizeof(ChunkOffset);
  auto entry = (char*) malloc(entry_length);
  
  *entry = 'i';
  auto current_pos = entry + sizeof(char);

  current_pos = _put_into_entry(current_pos, transaction_id, table_name, row_id);

  _write_to_buffer(entry, entry_length);

  free(entry);
}

void GroupCommitLogger::_write_to_buffer(char* entry, size_t length) {
  _buffer_mutex.lock();

  memcpy(_buffer + _buffer_position, entry, length);
  _buffer_position += length;
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
