/*
 *     Commit Entries:
 *       - log entry type ('C') : sizeof(char)
 *       - transaction_id       : sizeof(TransactionID)
 * 
 *     Value Entries:
 *       - log entry type ('V') : sizeof(char)
 *       - transaction_id       : sizeof(transaction_id_t)
 *       - table_name.size()    : sizeof(size_t)             --> what is max table_name size?
 *       - table_name           : table_name.size()
 *       - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset)
 *       - length(value)        : sizeof(int)
 *       - value                : length(value)
 *       - { length(value) + value } *
 *       - end indicator        : sizeof(int)
 *         (-> length(value) = 0)
 * 
 *     Invalidation Entries:
 *       - log entry type ('I') : sizeof(char)
 *       - transaction_id       : sizeof(TransactionID)
 *       - table_name.size()    : sizeof(size_t)             --> what is max table_name size?
 *       - table_name           : table_name.size()
 *       - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset)     
 *
 */


#include "group_commit_logger.hpp"
#include "logger.hpp"

#include <chrono>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sstream>
#include <thread>
#include <fstream>
#include <future>

#include <iostream>

namespace opossum {

constexpr size_t LOG_BUFFER_CAPACITY = 16384;

void GroupCommitLogger::commit(const TransactionID transaction_id){
  auto entry = (char*) malloc(sizeof(char) + sizeof(TransactionID));
  *entry = 'C';
  *(TransactionID*) (entry + 1) = transaction_id;
  _write_to_buffer(entry, sizeof(char) + sizeof(TransactionID));
}

void GroupCommitLogger::value(const TransactionID transaction_id, const std::string table_name, const RowID row_id, const std::stringstream &values){
  // auto entry = (char*) malloc(sizeof(char) + sizeof(TransactionID));
  // *entry = 'C';
  // *(TransactionID*) (entry + 1) = transaction_id;
  // _write_to_buffer(entry, sizeof(char) + sizeof(TransactionID));

  // std::stringstream ss;
  // ss << "(v," << transaction_id << "," << table_name << "," << row_id << "," << values.str() << ")\n";
  // _write_to_logfile(ss);
}

void GroupCommitLogger::invalidate(const TransactionID transaction_id, const std::string table_name, const RowID row_id){
  // auto entry = (char*) malloc(sizeof(char) + sizeof(TransactionID));
  // *entry = 'C';
  // *(TransactionID*) (entry + 1) = transaction_id;
  // _write_to_buffer(entry, sizeof(char) + sizeof(TransactionID));

  // std::stringstream ss;
  // ss << "(i," << transaction_id << "," << table_name << "," << row_id << ")\n";
  // _write_to_logfile(ss);
}

void GroupCommitLogger::_write_to_buffer(char* entry, size_t length) {
    // TODO: use other mutex
  _mutex.lock();
  
  memcpy(_buffer + _buffer_position, entry, length);
  _buffer_position += length;
  _has_unflushed_buffer = true;

  _mutex.unlock();
}

void GroupCommitLogger::_write_buffer_to_logfile(){
  _mutex.lock();
  auto _log_file = std::fstream(Logger::directory + Logger::filename, std::ios::out | std::ios::binary);
  _log_file.write(_buffer, _buffer_position);
  _log_file.sync();
  _mutex.unlock();
  _has_unflushed_buffer = false;
}

void GroupCommitLogger::_flush_to_disk_after_timeout(){
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    if (_has_unflushed_buffer) {
      _write_buffer_to_logfile();
    }
  }
}

void GroupCommitLogger::flush(){
  if (_has_unflushed_buffer) {
    _write_buffer_to_logfile();
  }
}


GroupCommitLogger::GroupCommitLogger() : AbstractLogger() {
  _buffer_capacity = LOG_BUFFER_CAPACITY;
  _buffer = (char*)malloc(_buffer_capacity);
  _buffer_position = 0u;
  memset(_buffer, 0, _buffer_capacity);

  _has_unflushed_buffer = false;

  // TODO: thread should be joined at the end of the program
  std::thread t1(&GroupCommitLogger::_flush_to_disk_after_timeout, this);
  t1.detach();

  _log_file.open(Logger::directory + Logger::filename, std::ios::out | std::ios::binary);
};

}  // namespace opossum
