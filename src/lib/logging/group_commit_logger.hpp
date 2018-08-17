#pragma once

#include <fstream>
#include <mutex>

#include "abstract_logger.hpp"
#include "types.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace opossum {

class LogEntry;

/*
 *  Logger that gathers multiple log entries in a buffer before flushing them to disk.
 */
class GroupCommitLogger : public AbstractLogger {
 public:
  GroupCommitLogger(const GroupCommitLogger&) = delete;
  GroupCommitLogger& operator=(const GroupCommitLogger&) = delete;

  void log_commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) override;

  void log_value(const TransactionID transaction_id, const std::string& table_name, const RowID row_id,
                 const std::vector<AllTypeVariant>& values) override;

  void log_invalidate(const TransactionID transaction_id, const std::string& table_name, const RowID row_id) override;

  void log_load_table(const std::string& file_path, const std::string& table_name) override;

  void log_flush() override;

  // Recovers db from logfiles and returns the number of loaded tables
  uint32_t recover() override;

  GroupCommitLogger();

 private:

  void _write_buffer_to_logfile();
  void _write_to_buffer(std::vector<char>& entry);

  void _open_logfile();

  char* _buffer;
  const uint32_t _buffer_capacity;  // uint32_t: Max buffer capacity ~ 4GB
  uint32_t _buffer_position;
  bool _has_unflushed_buffer;
  std::mutex _buffer_mutex;

  std::vector<std::pair<std::function<void(TransactionID)>, TransactionID>> _commit_callbacks;

  std::mutex _file_mutex;
  std::fstream _log_file;

  std::unique_ptr<PausableLoopThread> _flush_thread;
};

}  // namespace opossum
