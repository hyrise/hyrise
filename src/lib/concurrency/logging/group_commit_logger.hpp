#pragma once

#include "abstract_logger.hpp"
#include <fstream>

#include "types.hpp"

namespace opossum {

class GroupCommitLogger : public AbstractLogger{
 public:  
  GroupCommitLogger(const GroupCommitLogger&) = delete;
  GroupCommitLogger& operator=(const GroupCommitLogger&) = delete;

  void commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) override;

  void value(const TransactionID transaction_id, const std::string table_name, const RowID row_id, const std::vector<AllTypeVariant> values) override;

  void invalidate(const TransactionID transaction_id, const std::string table_name, const RowID row_id) override;

  void flush() override;

  void recover() override;

 private:
  friend class Logger;
  GroupCommitLogger();

 private:
  void _put_into_entry(char*& entry_cursor, const char &type, const TransactionID &transaction_id, const std::string &table_name, const RowID &row_id);

  void _flush_to_disk_after_timeout();
  void _write_buffer_to_logfile();
  void _write_to_buffer(std::vector<char> &entry);

  template <typename T>
  void _write_value(char*& cursor, const T value) {
    *(T*)cursor = value;
    cursor += sizeof(T);
  }

  ~GroupCommitLogger();
  
  char* _buffer;
  size_t _buffer_capacity;
  size_t _buffer_position;
  bool _has_unflushed_buffer;
  std::mutex _buffer_mutex;

  std::vector<std::pair<std::function<void(TransactionID)>, TransactionID>> _commit_callbacks;

  std::fstream _log_file;
};

}  // namespace opossum
