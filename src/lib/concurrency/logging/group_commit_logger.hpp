#pragma once

#include "abstract_logger.hpp"

#include <fstream>

#include "types.hpp"
#include "../../utils/loop_thread.hpp"

namespace opossum {

/*
 *  Logger that gathers multiple log entries in a buffer before flushing them to disk.
 */
class GroupCommitLogger : public AbstractLogger {
 public:
  GroupCommitLogger(const GroupCommitLogger&) = delete;
  GroupCommitLogger& operator=(const GroupCommitLogger&) = delete;

  void commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) override;

  void value(const TransactionID transaction_id, const std::string& table_name, const RowID row_id,
             const std::vector<AllTypeVariant> values) override;

  void invalidate(const TransactionID transaction_id, const std::string& table_name, const RowID row_id) override;

  void load_table(const std::string& file_path, const std::string& table_name) override {};

  void flush() override;

  void recover() override;

 private:
  friend class Logger;
  GroupCommitLogger();

 private:
  void _put_into_entry(std::vector<char>& entry, size_t& entry_cursor, const char& type,
                       const TransactionID& transaction_id, const std::string& table_name, const RowID& row_id);
  void _put_into_entry(std::vector<char>& entry, const char& type, const TransactionID& transaction_id);
  void _put_into_entry(std::vector<char>& entry, size_t& entry_cursor, const char& type,
                       const TransactionID& transaction_id);
  void _put_into_entry(std::vector<char>& entry, const char& type,
                       const TransactionID& transaction_id, const std::string& table_name, const RowID& row_id);
  
  void _write_buffer_to_logfile();
  void _write_to_buffer(std::vector<char>& entry);

  template <typename T>
  void _write_value(std::vector<char>& entry, size_t& cursor, const T& value) {
    // Assume entry is already large enough to fit the new value
    DebugAssert(cursor + sizeof(T) <= entry.size(), 
                "logger: value does not fit into vector, call resize() beforehand");
    *reinterpret_cast<T*>(&entry[cursor]) = value;
    cursor += sizeof(T);
  }

  ~GroupCommitLogger();

  char* _buffer;
  const size_t _buffer_capacity;
  size_t _buffer_position;
  bool _has_unflushed_buffer; 
  std::mutex _buffer_mutex;

  std::mutex _file_mutex;

  std::vector<std::pair<std::function<void(TransactionID)>, TransactionID>> _commit_callbacks;

  std::fstream _log_file;

  std::unique_ptr<LoopThread> _flush_thread;
};

}  // namespace opossum
