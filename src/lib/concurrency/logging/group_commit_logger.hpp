#pragma once

#include "abstract_logger.hpp"
#include <fstream>

#include "types.hpp"

namespace opossum {

class GroupCommitLogger : public AbstractLogger{
 public:  
  GroupCommitLogger(const GroupCommitLogger&) = delete;
  GroupCommitLogger& operator=(const GroupCommitLogger&) = delete;

  void commit(const TransactionID transaction_id) override;

  void value(const TransactionID transaction_id, const std::string table_name, const RowID row_id, const std::stringstream &values) override;

  void invalidate(const TransactionID transaction_id, const std::string table_name, const RowID row_id) override;

  void flush() override;

 private:
  friend class Logger;
  GroupCommitLogger();

 private:
  void _flush_to_disk_after_timeout();
  void _write_buffer_to_logfile();
  void _write_to_buffer(char* entry, size_t length);
  
  char* _buffer;
  size_t _buffer_capacity;
  uint _buffer_position;
  std::fstream _log_file;
  bool _has_unflushed_buffer;
};

}  // namespace opossum
