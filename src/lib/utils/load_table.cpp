#include "load_table.hpp"

#include <chrono>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "boost/lexical_cast.hpp"

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/table.hpp"
#include "string_utils.hpp"

using namespace std::chrono_literals;

using namespace std::string_literals;  // NOLINT

namespace opossum {

std::shared_ptr<Table> create_table_from_header(std::ifstream& infile, size_t chunk_size) {
  std::string line;
  std::getline(infile, line);
  Assert(line.find('\r') == std::string::npos, "Windows encoding is not supported, use dos2unix");
  std::vector<std::string> column_names = split_string_by_delimiter(line, '|');
  std::getline(infile, line);
  std::vector<std::string> column_types = split_string_by_delimiter(line, '|');

  auto column_nullable = std::vector<bool>{};
  for (auto& type : column_types) {
    auto type_nullable = split_string_by_delimiter(type, '_');
    type = type_nullable[0];

    auto nullable = type_nullable.size() > 1 && type_nullable[1] == "null";
    column_nullable.push_back(nullable);
  }

  TableColumnDefinitions column_definitions;
  for (size_t i = 0; i < column_names.size(); i++) {
    const auto data_type = data_type_to_string.right.find(column_types[i]);
    Assert(data_type != data_type_to_string.right.end(),
           std::string("Invalid data type ") + column_types[i] + " for column " + column_names[i]);
    column_definitions.emplace_back(column_names[i], data_type->second, column_nullable[i]);
  }

  // Adapted for Chunking-Ping Project. Usually uses Mvcc::Yes.
  return std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::No);
}

std::shared_ptr<Table> create_table_from_header(const std::string& file_name, size_t chunk_size) {
  std::ifstream infile(file_name);
  Assert(infile.is_open(), "load_table: Could not find file " + file_name);
  return create_table_from_header(infile, chunk_size);
}

std::shared_ptr<Table> load_table(const std::string& file_name, size_t chunk_size,
                                  FinalizeLastChunk finalize_last_chunk) {
  std::ifstream infile(file_name);
  Assert(infile.is_open(), "load_table: Could not find file " + file_name);

  auto table = create_table_from_header(infile, chunk_size);
  auto column_count = table->column_count();

  auto chunk_stringstreams = std::vector<std::shared_ptr<std::stringstream>>{};
  chunk_stringstreams.push_back(std::make_shared<std::stringstream>());

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(64);

  auto create_chunk_job = [&](const ChunkID chunk_id) {
    auto quasi_chunk = std::vector<std::vector<std::optional<std::string>>>(column_count);
    for (auto& quasi_segment : quasi_chunk) {
      quasi_segment.reserve(chunk_size);
    }

    auto chunk_line = std::string(200, '.');
    auto row_id = size_t{0};
    auto& sstream = *chunk_stringstreams[chunk_id];
    while (std::getline(sstream, chunk_line)) {
      const auto string_values = split_string_by_delimiter(chunk_line, '|');
      for (auto column_id = ColumnID{0}; column_id < string_values.size(); ++column_id) {
        if (table->column_is_nullable(column_id) && string_values[column_id] == "null") {
          quasi_chunk[column_id].push_back(std::nullopt);
          continue;
        }

        quasi_chunk[column_id].push_back(string_values[column_id]);
      }
      ++row_id;
    }
    //std::cout << "Chunk " << chunk_id << ": finished parsing input file." << std::endl;

    sstream.clear();
    sstream.str("");

    const auto new_chunk_size = row_id;

    Segments segments;
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      auto& quasi_segment = quasi_chunk[column_id];
      resolve_data_type(table->column_data_type(column_id), [&](auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        auto nulls = pmr_vector<bool>(new_chunk_size);
        auto values = pmr_vector<ColumnDataType>(new_chunk_size);

        for (auto row_id = size_t{0}; row_id < quasi_segment.size(); ++row_id) {
          if (!quasi_segment[row_id]) {
            nulls[row_id] = true;
            continue;
          }

          values[row_id] = boost::lexical_cast<ColumnDataType>(*quasi_segment[row_id]);
        }
   
        if (table->column_is_nullable(column_id)) {
          segments.emplace_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(nulls)));
        } else {
          segments.emplace_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(values)));
        }
      });
      quasi_segment.clear();
    }

    while (table->chunk_count() < chunk_id) {
      std::this_thread::sleep_for(100ms);
    }

    table->append_chunk(segments);
    table->get_chunk(chunk_id)->finalize();

    // Disabled for Chunk Project.
    //auto mvcc_data = table->last_chunk()->mvcc_data();
    //mvcc_data->set_begin_cid(table->last_chunk()->size() - 1, 0);
    //std::cout << "Chunk " << chunk_id << ": done." << std::endl;
  };

  auto line_count = size_t{0};
  auto chunk_id = ChunkID{0};
  auto line = std::string(200, '.');
  while (std::getline(infile, line)) {
    *chunk_stringstreams[chunk_id] << line << '\n';
    ++line_count;

    if (line_count == chunk_size) {
      //std::cout << "Sufficient lines for chunk " << chunk_id << " read: initiating creation." << std::endl;
      auto create_chunk_job_param = [&, chunk_id]() { create_chunk_job(chunk_id); };
      jobs.push_back(std::make_shared<JobTask>(create_chunk_job_param));
      jobs.back()->schedule();
      ++chunk_id;
      chunk_stringstreams.push_back(std::make_shared<std::stringstream>());
      line_count = 0;
    }
  }
   
  if (line_count > 0) {
    //std::cout << "Creating chunk " << chunk_id << " for the remaining " << line_count << " rows." << std::endl;
    auto create_chunk_job_param = [&, chunk_id]() { create_chunk_job(chunk_id); };
    jobs.push_back(std::make_shared<JobTask>(create_chunk_job_param));
    jobs.back()->schedule();
  }

  Hyrise::get().scheduler()->wait_for_tasks(jobs);
  
  const auto line_count_of_input_file = chunk_size * (chunk_id) + line_count;
  Assert(line_count_of_input_file == table->row_count(), "Loaded table (" + std::to_string(table->row_count()) + " rows) differs in row count from input TBL file (" + std::to_string(line_count_of_input_file) + " rows).");

  return table;
}

}  // namespace opossum
