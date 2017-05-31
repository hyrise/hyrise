#pragma once

#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "operators/abstract_read_only_operator.hpp"
#include "scheduler/job_task.hpp"

namespace opossum {

// structure to bin orphans and widows to the chunk where they were found
struct ParsingResult;

//  A structure that keeps information concerning structure of the table to create
struct TableInfo {
  TableInfo() : _col_count(0) {}

  explicit TableInfo(std::shared_ptr<Table> table) : _col_count(table->col_count()) {
    for (ColumnID i = 0; i < table->col_count(); i++) {
      _col_types.push_back(table->column_type(i));
    }
  }

  size_t _col_count;
  alloc_vector<std::string> _col_types;
};
/**
 * In contrast to the CsvRfcParse, the CsvNonRfcParser works with a more constraint set of csv files:
 * All linebreaks within quoted strings are further escaped with a \
 * e.g.
 * "abcde","123\njfrrj"\n
 * becomes
 * "abcde","123\\njfrrj"\n
 * This additional constraint is necessary when fields are split up between the quotes and the parser
 * needs to determine if a buffer was quoted or not before split:
 *
 * e.g.
 *
 * ----buffer 0-----
 * "abcdeherh",1233,"esfef
 * ----buffer 1-----
 * kbhdgjknk\\nfkjnkenfknk
 * ----buffer 2-----
 * ", 1234,"ebef"\n
 *
 * If there was no additional escape in buffer 1, the parse wouldn't have jnown whether to treat the
 * linebreak as a csv_delimiter or not.
 * The CsvRfcParse does a whole filescan before splitting up the buffers to avoid this scenario.
 */
class CsvNonRfcParser {
 public:
  /**
   *
   * @param buffer_size specifies the maximum size of the buffers, the file is split up into. In combination with the
   * number of tasks, this determines, how much memory is used to buffer the file before reading. This becomes
   * interesting if the file is too large (encoding might increase the size unnecessarily) to be kept in memory
   * completely while also building the database structure
   * @param num_tasks   number of tasks (parsing one buffer of size buffer_size each) that will run at the same time,
   * once one task has finished, another one will be started immediately
   */
  explicit CsvNonRfcParser(const size_t buffer_size, const unsigned int num_tasks = 4);

  // Returns the table that was created from the csv file.
  std::shared_ptr<Table> parse(const std::string& filename);

 protected:
  const unsigned int _num_tasks;
  const size_t _buffer_size;
  std::ifstream _file_handle;
  std::streamsize _file_size;

  // we need a vector to store the jobs objects so they don't end up leaking memory
  alloc_vector<std::shared_ptr<JobTask>> _jobs;

  // holds information to the table that is going to be created
  TableInfo _file_info;

  // Before a new task is shceduled, a place in this array is reserved, so the order of chunks can be preserved, even
  // though some parsing tasks might finish after tasks that were scheduled after them
  alloc_vector<std::shared_ptr<ParsingResult>> _parsing_results;

  // holds the current number of running tasks, needs to be atomic to be thread-safe
  std::atomic_uint _task_counter;

  // this blocks as long as the maximum number of tasks is running
  std::condition_variable _task_finished_notification;
  std::mutex _notification_mutex;

  // keeps track of the current position in the file, this is necessary because we don't cut the file into buffers
  // in an initial loop, but rather step by step once a parsing task finishes
  uint64_t _file_read_state;

 protected:
  /* Creates the table structure from the meta file.
   *  The following is the content of an example meta file:
   *
   *  PropertyType,Key,Value
   *  Chunk Size,,100
   *  Column Type,a,int
   *  Column Type,b,string
   *  Column Type,c,float
   */
  const std::shared_ptr<Table> _process_meta_file(const std::string& meta_file);

  // Reads in a full CSV row and respects the not-safe-mode conditions
  bool _get_row(std::istream& stream, std::string& out);

  // Reads in a full CSV field or row (safe mode) depending on the given delimiter
  bool _read_csv(std::istream& stream, std::string& out, char delimiter);

  // parses one row of the csv, internally employs _get_fields
  alloc_vector<AllTypeVariant> _parse_row(const std::string line, const TableInfo& info);

  /**
   * This method sanitizes the resultlist that has the following form:
   *
   *        / first row: "abcd","1234","foobar"
   * +result-- chunk with parsed rows without first and last row
   * |       \ last row: "abcd","1234","foobar"
   * |       / first row: "abcd","1234","foobar"
   * +result-- chunk with parsed rows without first and last row
   * |       \ last row: "abcd","123
   * |       / first row:           4","foobar"
   * +result-- chunk with parsed rows without first and last row
   *        \ last row: "abcd","1234","foobar"
   *
   *  Rows, that were cut inbetween are joined, reparsed and added to a chunk
   * @param results the result list
   * @param info the structure of the chunks inside the result list
   */
  void _resolve_orphans_widows(alloc_vector<std::shared_ptr<ParsingResult>>& results, const TableInfo& info);

  /**
   * This method is the entrypoint for all tasks
   * @param new_chunk pointer to the result element in the global result vector, where the result will be saved after
   * the task has finished. This is important to preserve the order of rows.
   * @param buffer a vector containing a section of the file to be parsed
   * @param info information about the structure of the chunks to be created
   */
  void _parse_csv(std::shared_ptr<std::promise<ParsingResult>> new_chunk, std::shared_ptr<alloc_vector<char>> buffer,
                  const TableInfo& info);

  /**
   * This method creates a new buffer by reading from the file, prepares a new task and starts it
   * @return true if buffer has been scheduled, false if not
   */
  bool _parse_next_buffer();

  /**
   * The callback method that gets notified by the scheduler after a task has finished. This method is only concerned
   * in task execution, handling the parsing results is delegated to the process_task_results method
   * @param future
   * @param parsing_result
   */
  void _handle_finished_task(std::shared_ptr<std::future<ParsingResult>> future,
                             std::shared_ptr<ParsingResult> parsing_result);

  /**
   * This method extracts the result from the future object and inserts it into the global result vector on the position
   * that has been determined before the task has been started
   * @param future
   * @param parsing_result
   */
  void _process_task_results(std::shared_ptr<std::future<ParsingResult>> future,
                             std::shared_ptr<ParsingResult> parsing_result);
  /**
   * This method increses a value if it is less than another value. All this happens in a thread-safe fashion and is
   * needed to keep the number of currently running threads below a given number.
   * @param val reference to the variable that shall be increased
   * @param less_than constraint to the increase
   * @return true if val has been increase, false if not
   */
  bool _atomic_check_increment(std::atomic_uint& val, unsigned int less_than);

  /**
   * extracts the next field from the stream
   * @param stream
   * @param out
   * @return true if a field could be extracted, false if not
   */
  bool _get_field(std::istream& stream, std::string& out);

  // Splits and returns all fields from a given CSV row.
  alloc_vector<std::string> _get_fields(const std::string& row);

  bool _start_new_job(std::shared_ptr<alloc_vector<char>> buffer);
};
}  // namespace opossum
