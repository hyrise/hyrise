#pragma once

#include <aio.h>
#include <stdint.h>
#include <string>
#include <vector>

namespace hyrise {

void micro_benchmark_clear_cache();
void micro_benchmark_clear_disk_cache();
void aio_error_handling(aiocb* aiocb, uint32_t expected_bytes);
std::vector<uint32_t> generate_random_indexes(uint32_t number);
std::vector<uint32_t> generate_random_positive_numbers(uint32_t size);

// Closes the passed filedescriptor and prints the passed message together with the error message belonging to the
// passed error number. Might be used in an Assert or Fail statement.
std::string close_file_and_return_error_message(int32_t fd, std::string message, int error_num);

}  // namespace hyrise
