#include "csv_non_rfc_parser.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "import_export/csv.hpp"
#include "import_export/csv_converter.hpp"

namespace opossum {

template <typename CharT, typename TraitsT = std::char_traits<CharT>>
class VectorStream : public std::basic_streambuf<CharT, TraitsT> {
 public:
  explicit VectorStream(std::vector<CharT>& vector) {
    this->setg(vector.data(), vector.data(), vector.data() + vector.size());
  }
};

struct ParsingResult {
  ParsingResult(const ParsingResult&) = delete;
  ParsingResult& operator=(const ParsingResult&) = delete;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  ParsingResult(ParsingResult&&) = default;
  ParsingResult& operator=(ParsingResult&&) = default;

  ParsingResult() = default;
  explicit ParsingResult(std::shared_ptr<Chunk> chunk) : _first_row(""), _last_row(""), _chunk(chunk) {}
  // in case a line is broken up in between the first or the last field, just one, the orphan or the widow is
  // identifiable as such. To preserve the rest of the line, first and last row of every chunk are kept till
  // the resolution of orphans and widows
  std::string _first_row;
  std::string _last_row;

  std::shared_ptr<Chunk> _chunk;
};

CsvNonRfcParser::CsvNonRfcParser(const size_t buffer_size, const unsigned int num_tasks)
    : _num_tasks(num_tasks), _buffer_size(buffer_size), _task_counter(0) {}

std::shared_ptr<Table> CsvNonRfcParser::parse(const std::string& filename) {
  auto table = _process_meta_file(filename + csvMeta_file_extension);
  _file_info = TableInfo(table);

  _file_handle.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  _file_handle.open(filename, std::ifstream::binary | std::ifstream::ate);
  // allow failbit from std::getline
  _file_handle.exceptions(std::ifstream::badbit);
  _file_size = _file_handle.tellg();
  _file_handle.seekg(0);

  bool finished = false;
  do {
    // don't try to parse the next buffer if there is data left
    if (!finished && _atomic_check_increment(_task_counter, _num_tasks)) {
      // returns false if started to process a buffer
      finished = !_parse_next_buffer();

      // if file is finished, ergo no new task scheduled, decrease the task_counter
      if (finished) {
        _task_counter--;
      }
    } else {
      //      wait, till the next finishing thread signals
      std::unique_lock<std::mutex> lock(_notification_mutex);
      _task_finished_notification.wait(lock);
    }
  } while (!finished || _task_counter > 0);

  _resolve_orphans_widows(_parsing_results, _file_info);

  // finally add all processed chunks to the table
  for (auto& result : _parsing_results) {
    table->add_chunk(std::move(*result->_chunk));
  }

  // remove all JobTask shared_ptrs
  _jobs.clear();

  return table;
}

bool CsvNonRfcParser::_start_new_job(std::shared_ptr<std::vector<char>> buffer) {
  //  if buffer is empty, or just a linebreak: skip it!
  if (buffer->empty() || (buffer->front() == csvDelimiter)) {
    return false;
  }

  // create promise object as a sink for the chunk object to be created in a new thread
  auto promise = std::make_shared<std::promise<ParsingResult>>();
  // the future needs to be obtained from promise before the promise is moved to the new thread
  auto future = std::make_shared<std::future<ParsingResult>>(promise->get_future());

  auto job = std::make_shared<JobTask>([promise, buffer, this] { _parse_csv(promise, buffer, _file_info); });

  _jobs.push_back(job);
  // move thread and future object to a vector so it can be processed later on
  auto parsing_result = std::make_shared<ParsingResult>();
  _parsing_results.push_back(parsing_result);

  //  associate job, future and the result object by assigning a finished callback
  job->set_done_callback([future, parsing_result, this] { _handle_finished_task(future, parsing_result); });

  job->schedule();
  return true;
}

bool CsvNonRfcParser::_parse_next_buffer() {
  std::streamsize adapted_buffer_size;
  //      reset buffer size if just processing the rest of a file
  if (_file_size - _file_handle.tellg() <= static_cast<std::streamsize>(_buffer_size)) {
    adapted_buffer_size = _file_size - _file_handle.tellg();
  } else {
    adapted_buffer_size = _buffer_size;
  }

  //      read data to buffer
  auto buffer = std::make_shared<std::vector<char>>(adapted_buffer_size);
  if (_file_handle.tellg() == _file_size || !_file_handle.read(buffer->data(), adapted_buffer_size)) {
    return false;
  }

  return _start_new_job(buffer);
}

void CsvNonRfcParser::_handle_finished_task(std::shared_ptr<std::future<ParsingResult>> future,
                                            std::shared_ptr<ParsingResult> parsing_result) {
  _task_counter--;
  _process_task_results(future, parsing_result);
  _task_finished_notification.notify_one();
}

bool CsvNonRfcParser::_atomic_check_increment(std::atomic_uint& val, unsigned int less_than) {
  unsigned int new_val;
  unsigned int old_val = val.load();
  do {
    if (old_val >= less_than) {
      return false;
    }
    new_val = old_val + 1;
  } while (!val.compare_exchange_weak(old_val, new_val));

  return true;
}

void CsvNonRfcParser::_process_task_results(std::shared_ptr<std::future<ParsingResult>> future,
                                            std::shared_ptr<ParsingResult> parsing_result) {
  ParsingResult result = future->get();

  parsing_result->_first_row = std::move(result._first_row);
  parsing_result->_last_row = std::move(result._last_row);

  // shared pointer, so no move here
  parsing_result->_chunk = result._chunk;
}

void CsvNonRfcParser::_resolve_orphans_widows(std::vector<std::shared_ptr<ParsingResult>>& results,
                                              const TableInfo& info) {
  auto first_row = _parse_row(results[0]->_first_row, info);

  auto first_element_chunk = std::make_shared<Chunk>();
  for (auto& type : info._col_types) {
    first_element_chunk->add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
  }

  first_element_chunk->append(first_row);
  ParsingResult first_element_result(first_element_chunk);

  for (size_t i = 0; i < results.size() - 1; i++) {
    auto last_row = _parse_row(results[i]->_last_row, info);

    auto first_row_of_next_chunk = _parse_row(results[i + 1]->_first_row, info);

    if (last_row.size() < info._col_count || first_row_of_next_chunk.size() < info._col_count) {
      auto joined = _parse_row(results[i]->_last_row + results[i + 1]->_first_row, info);

      results[i]->_chunk->append(joined);
    } else {
      results[i]->_chunk->append(last_row);
      results[i]->_chunk->append(first_row_of_next_chunk);
    }
  }

  auto last_row = _parse_row(results.back()->_last_row, info);

  // empty lines at the end of the document might add a "last row" containing of no elements, ignore that line
  if (last_row.size() == info._col_count) results.back()->_chunk->append(last_row);

  results.insert(results.begin(), std::make_shared<ParsingResult>(std::move(first_element_result)));
}

std::vector<AllTypeVariant> CsvNonRfcParser::_parse_row(const std::string row, const TableInfo& info) {
  const auto fields = _get_fields(row);
  if (info._col_count != fields.size()) {
    // return empty vector so the calling function can handle incomplete lines
    return std::vector<AllTypeVariant>(0);
  }

  std::vector<AllTypeVariant> values(fields.size());
  for (ColumnID i = 0; i < info._col_count; ++i) {
    values[i] = AllTypeVariant(fields[i]);
  }

  return values;
}

void CsvNonRfcParser::_parse_csv(std::shared_ptr<std::promise<ParsingResult>> new_chunk,
                                 std::shared_ptr<std::vector<char>> buffer, const TableInfo& info) {
  // create chunk with the correct columns
  auto chunk = std::make_shared<Chunk>();
  ParsingResult result(chunk);

  for (auto& type : info._col_types) {
    result._chunk->add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
  }

  // wrap the char vector into an istream interface so the naive import can handle it
  VectorStream<char> stream(*buffer);
  std::istream is(&stream);

  std::string row, prefetch_row;

  // save the first line to the result object for the resolution of orphans and widows later on
  _get_row(is, result._first_row);

  bool has_next = _get_row(is, prefetch_row);

  int count = 0;

  while (has_next) {
    row = prefetch_row;
    has_next = _get_row(is, prefetch_row);

    if (!has_next) {
      break;
    }

    const auto fields = _parse_row(row, info);

    result._chunk->append(fields);
    ++count;
  }

  // The last row was skipped but still resides in _last_row
  result._last_row = row;

  new_chunk->set_value(std::move(result));
}

const std::shared_ptr<Table> CsvNonRfcParser::_process_meta_file(const std::string& meta_file) {
  std::ifstream file;
  file.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  file.open(meta_file);
  // allow failbit from std::getline
  file.exceptions(std::ifstream::badbit);

  std::string row;
  // skip header
  _get_row(file, row);

  // read chunk size
  _get_row(file, row);
  auto fields = _get_fields(row);
  const int chunk_size = type_cast<int>(fields[2]);
  const auto table = std::make_shared<Table>(chunk_size);

  // read column info
  while (_get_row(file, row)) {
    fields = _get_fields(row);
    table->add_column(type_cast<std::string>(fields[1]), type_cast<std::string>(fields[2]));
  }
  return table;
}

bool CsvNonRfcParser::_read_csv(std::istream& stream, std::string& out, const char delimiter) {
  out.clear();
  std::string line;

  while (std::getline(stream, line, delimiter)) {
    out += line;
    // If the number of quotes is even, "out" contains a full row.
    // If the number is odd, there is an opening quote but no closing quote. The delimiter is part of the field and must
    // appended in this case.
    // Escaped quotes are two quotes and therefore don't change the result of the modulus operation.
    if (std::count(out.begin(), out.end(), csvQuote) % 2 == 0) {
      return true;
    } else {
      out += delimiter;
    }
  }
  return false;
}

bool CsvNonRfcParser::_get_row(std::istream& stream, std::string& out) {
  out.clear();
  std::string line;

  while (std::getline(stream, line, csvDelimiter)) {
    out += line;

    //    We expcet all delimiters encapsulated in quotes to be escaped with csvEscape, so if the forelast character
    //    is
    //    an escape, the delimiter (last character) is not valid. If the last character is no delimiter, we have
    //    reached the end of the buffer and return an incomplete result
    if (out[out.size() - 1] == csvDelimiter_escape) {
      //      unescape the linebreak
      out.erase(out.begin() + out.size() - 1);
      out += csvDelimiter;
      continue;
    }

    // If the number of quotes is even, "out" contains a full row.
    // If the number is odd, there is an opening quote but no closing quote. The delimiter is part of the
    // field and must appended in this case.
    // Escaped quotes are two quotes and therefore don't change the result of the modulus operation.
    auto number_of_quotes = std::count(out.begin(), out.end(), csvQuote);
    if (number_of_quotes % 2 == 0) {
      return true;
    } else {
      //      Field is incapsulated within quotes
      if (std::count(out.begin(), out.end(), csvQuote) > 0) {
        //         If we are searching for the real end of line (linebreak not encapsulated within csvEscape
        //           example line:
        //           xxx",yyy,"zz\nz"\n
        //           we found the end of the line, if the csvDelimiter is prepended by a csvEscape, getline()
        //           erases csvDelimiter,
        //           so we just check the last character of the fetched line
        if (out.back() == csvQuote) {
          return true;
        }

        //          example line:
        //          xxx",---,"--\n-",---\n
        //          or
        //          "xx\n
        //          This case is more complicated, we have to check wether there are any csvQuote between the last
        //          csv::seperator
        //          and the csvDelimiter respectively, in this case, the end of the fetched line
        auto pos_quote = out.find_last_of(csvQuote);
        auto pos_seperator = out.find_last_of(csvSeparator);
        if (pos_seperator != std::string::npos && pos_quote < pos_seperator) {
          return true;
        }

        //          example line:
        //          xxx",yyy,"zz\nz","www
        //          This should return true, if the end of the stream was hit
        if (stream.eof()) {
          return true;
        }

        //        string does not start with quote, so the row is an orphan (got cut after it's beginning)
        //        returning false will stop the _read_fields loop in the calling method and finally return
        //        an incomplete field vector, so the method calling _get_fields can treat row as an orphan
      }
    }

    out += csvDelimiter;
  }
  return false;
}

bool CsvNonRfcParser::_get_field(std::istream& stream, std::string& out) {
  return _read_csv(stream, out, csvSeparator);
}

std::vector<std::string> CsvNonRfcParser::_get_fields(const std::string& row) {
  std::vector<std::string> fields;
  std::stringstream stream{row};
  std::string field;
  while (_get_field(stream, field)) {
    AbstractCsvConverter::unescape(field);
    fields.emplace_back(field);
  }
  return fields;
}

}  // namespace opossum
