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
std::string fail_and_close_file(int32_t fd, std::string message, int error_num);
std::string fail_and_close_files(std::vector<int32_t> filedescriptors, std::string message, int error_num);

}  // namespace hyrise
