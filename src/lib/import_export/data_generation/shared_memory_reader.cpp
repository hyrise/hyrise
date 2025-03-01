#include <csignal>
#include <cstdint>
#include <fcntl.h>
#include <memory>
#include <stdexcept>
#include <sys/mman.h>
#include <vector>

#include "encoding_config.hpp"
#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "shared_memory_reader.hpp"
#include "pdgf_table_builder.hpp"
#include "pdgf_table_schema_builder.hpp"
#include "utils/timer.hpp"

namespace hyrise {
template <uint32_t reader_num_columns>
std::shared_ptr<BaseSharedMemoryReader> create_shared_memory_reader(
  const std::shared_ptr<BenchmarkConfig>& benchmark_config, const char* shared_memory_name, const char* data_ready_sem, const char* buffer_free_sem) {
  switch (benchmark_config->pdgf_work_unit_size) {
    case    8u: return std::make_shared<SharedMemoryReader<   8u, reader_num_columns>>(benchmark_config, shared_memory_name, data_ready_sem, buffer_free_sem);
    case   16u: return std::make_shared<SharedMemoryReader<  16u, reader_num_columns>>(benchmark_config, shared_memory_name, data_ready_sem, buffer_free_sem);
    case   32u: return std::make_shared<SharedMemoryReader<  32u, reader_num_columns>>(benchmark_config, shared_memory_name, data_ready_sem, buffer_free_sem);
    case   64u: return std::make_shared<SharedMemoryReader<  64u, reader_num_columns>>(benchmark_config, shared_memory_name, data_ready_sem, buffer_free_sem);
    case  128u: return std::make_shared<SharedMemoryReader< 128u, reader_num_columns>>(benchmark_config, shared_memory_name, data_ready_sem, buffer_free_sem);
    case  256u: return std::make_shared<SharedMemoryReader< 256u, reader_num_columns>>(benchmark_config, shared_memory_name, data_ready_sem, buffer_free_sem);
    case  512u: return std::make_shared<SharedMemoryReader< 512u, reader_num_columns>>(benchmark_config, shared_memory_name, data_ready_sem, buffer_free_sem);
    case 1024u: return std::make_shared<SharedMemoryReader<1024u, reader_num_columns>>(benchmark_config, shared_memory_name, data_ready_sem, buffer_free_sem);
    case 2048u: return std::make_shared<SharedMemoryReader<2048u, reader_num_columns>>(benchmark_config, shared_memory_name, data_ready_sem, buffer_free_sem);
    case 4096u: return std::make_shared<SharedMemoryReader<4096u, reader_num_columns>>(benchmark_config, shared_memory_name, data_ready_sem, buffer_free_sem);
    case 8192u: return std::make_shared<SharedMemoryReader<8192u, reader_num_columns>>(benchmark_config, shared_memory_name, data_ready_sem, buffer_free_sem);
    default: throw std::runtime_error("Unknown work unit size for shared memory reader!");
  }
}

std::shared_ptr<BaseSharedMemoryReader> create_shared_memory_reader(
const std::shared_ptr<BenchmarkConfig>& benchmark_config, uint32_t reader_num_columns,
const char* shared_memory_name, const char* data_ready_sem, const char* buffer_free_sem) {
  switch (reader_num_columns) {
    case 16u: return create_shared_memory_reader<16u>(benchmark_config, shared_memory_name, data_ready_sem, buffer_free_sem);
    case 32u: return create_shared_memory_reader<32u>(benchmark_config, shared_memory_name, data_ready_sem, buffer_free_sem);
    default: throw std::runtime_error("Invalid number of buffer columns for shared memory reader!");
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
SharedMemoryReader<work_unit_size, num_columns>::SharedMemoryReader(
    const std::shared_ptr<BenchmarkConfig>& benchmark_config, const char* shared_memory_name, const char* data_ready_sem, const char* buffer_free_sem)
    : _benchmark_config(benchmark_config), _shared_memory_file_name(shared_memory_name) {
  // Maximum size: HEADER + 2 ** 30.5
  // BUFFER_SIZE (2 ** x) * WORK_UNIT_SIZE (2 ** 7) * TABLE_FIELDS (2 ** 4) * SHARED_MEMORY_FIELD_SIZE (3 * 64 ~ 2 ** 8.5)

  if (work_unit_size <= 1 || num_columns < 1) {
    throw std::runtime_error("Invalid buffer dimensions provided");
  }

  // Remove existing file just in case so that we have a fresh state
  // We don't care about errors (such as the file not existing) here
  shm_unlink(shared_memory_name);

  _shm_fd = shm_open(shared_memory_name, O_CREAT | O_RDWR, 0666);
  if (_shm_fd == -1) {
    throw std::runtime_error("Shared memory opening failed");
  }

  if (-1 == ftruncate(_shm_fd, SHARED_MEMORY_DATA_BUFFER_OFFSET +
                                   sizeof(DataBuffer<buffer_size, work_unit_size, num_columns>))) {
    throw std::runtime_error("Resizing shared memory file failed");
  }

  // Map the shared memory object in memory
  _data_buffer = static_cast<DataBuffer<buffer_size, work_unit_size, num_columns>*>(
      mmap(nullptr, sizeof(DataBuffer<buffer_size, work_unit_size, num_columns>), PROT_READ | PROT_WRITE, MAP_SHARED,
           _shm_fd, SHARED_MEMORY_DATA_BUFFER_OFFSET));
  if (_data_buffer == MAP_FAILED) {
    throw std::runtime_error("Data memory mapping failed");
  }

  _ring_buffer = std::make_shared<MultiProcessRingBuffer<buffer_size>>(_shm_fd, work_unit_size, num_columns,
                                                                       data_ready_sem, buffer_free_sem, _benchmark_config->pdgf_disable_micro_benchmarks);
}

template <uint32_t work_unit_size, uint32_t num_columns>
SharedMemoryReader<work_unit_size, num_columns>::~SharedMemoryReader() {
  if (_data_buffer != MAP_FAILED) {
    std::cerr << "Unmapping data buffer\n";
    munmap(_data_buffer, sizeof(DataBuffer<buffer_size, work_unit_size, num_columns>));
  }
  if (_shm_fd != -1) {
    std::cerr << "Closing shared memory file\n";
    close(_shm_fd);
    shm_unlink(_shared_memory_file_name);
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
void SharedMemoryReader<work_unit_size, num_columns>::reset() {
  _num_tables_to_read = INT32_MAX;
  _num_read_tables = 0;
  _ring_buffer->reset();
}

template <uint32_t work_unit_size, uint32_t num_columns>
bool SharedMemoryReader<work_unit_size, num_columns>::has_next_table() const {
  return _num_read_tables < _num_tables_to_read;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::unique_ptr<BasePDGFTableSchemaBuilder> SharedMemoryReader<work_unit_size, num_columns>::read_next_schema() {
  // Table schema
  auto ring_cell = _ring_buffer->prepare_retrieval();
  Assert(ring_cell->cell_type == RingBufferCellType::TableSchema, "First information received by PDGF should be table schema, was " + std::to_string(ring_cell->cell_type));
  auto table_schema_builder = std::make_unique<PDGFTableSchemaBuilder<work_unit_size, num_columns>>(ring_cell->table_id, _benchmark_config->chunk_size);

  auto data_slot = ring_cell->data_buffer_offset;
  auto addressed_data = _data_buffer->get_addressed_by(ring_cell);
  _num_tables_to_read = * reinterpret_cast<uint32_t*>(addressed_data->data[0][0]);
  _ring_buffer->retrieval_finished();

  table_schema_builder->read_schema(addressed_data);
  _return_data_slot(data_slot);
  _num_read_tables++;
  return table_schema_builder;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::shared_ptr<BasePDGFTableBuilder> SharedMemoryReader<work_unit_size, num_columns>::read_next_table() {
  auto timer = Timer{};

  // Generation info
  auto ring_cell = _ring_buffer->prepare_retrieval();
  Assert(ring_cell->cell_type == RingBufferCellType::TableGenerationInfo, "Did not receive table generation info, was " + std::to_string(ring_cell->cell_type));
  auto table_builder = std::make_shared<PDGFTableBuilder<work_unit_size, num_columns>>(ring_cell->table_id, _benchmark_config->chunk_size);
  auto data_slot = ring_cell->data_buffer_offset;
  auto addressed_data = _data_buffer->get_addressed_by(ring_cell);
  _ring_buffer->retrieval_finished();
  _num_tables_to_read = * reinterpret_cast<uint32_t*>(addressed_data->data[0][0]);
  table_builder->read_generation_info(addressed_data, _benchmark_config->encoding_config);
  _return_data_slot(data_slot);

  // TODO(JEH): parallelize. adaptively use different number of workers here (only use a large number if table actually has a lot of chunks)
  if (table_builder->reading_should_be_parallelized()) {
    auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
    tasks.reserve(_benchmark_config->data_preparation_cores);
    for (auto worker_index = uint32_t{0}; worker_index < _benchmark_config->data_preparation_cores; ++worker_index) {
      tasks.emplace_back(std::make_shared<JobTask>([this, &table_builder, worker_index] {
        _worker_read_data(worker_index, table_builder);
      }));
    }
    auto scheduler = Hyrise::get().scheduler();
    scheduler->schedule_and_wait_for_tasks(tasks);
  } else {
    std::cerr << "Not parallelizing reading for this table, as it is too small.\n";
    _worker_read_data(0, table_builder);
  }

  ring_cell = _ring_buffer->prepare_retrieval();
  Assert(ring_cell->cell_type == RingBufferCellType::TableCompleted, "Did not receive table completed indicator, was " + std::to_string(ring_cell->cell_type));
  _num_read_tables++;
  _ring_buffer->retrieval_finished();
  _return_data_slot(ring_cell->data_buffer_offset);

  std::cerr << "Finished reading table " << table_builder->table_name() << " (" << timer.lap_formatted() << ").\n";
  return table_builder;
}

template <uint32_t work_unit_size, uint32_t num_columns>
void SharedMemoryReader<work_unit_size, num_columns>::_return_data_slot(uint32_t buffer_offset) {
  auto buffer_cell = _ring_buffer->prepare_writing();
  buffer_cell->cell_type = RingBufferCellType::Noop;
  buffer_cell->data_buffer_offset = buffer_offset;
  _ring_buffer->writing_finished();
}

template <uint32_t work_unit_size, uint32_t num_columns>
void SharedMemoryReader<work_unit_size, num_columns>::_worker_read_data(uint32_t reader_index, std::shared_ptr<PDGFTableBuilder<work_unit_size, num_columns>>& table_builder) {
  auto timer = Timer{};
  long num_processed = 0;
  long waiting_time = 0;
  long appending_time = 0;
  long encoding_time = 0;
  while (table_builder->reader_should_handle_another_work_unit()) {
    num_processed++;
    timer.lap();
    auto ring_cell = _ring_buffer->prepare_retrieval();
    waiting_time += timer.lap().count();
    Assert(ring_cell->cell_type == RingBufferCellType::Data, "Did not receive data, was " + std::to_string(ring_cell->cell_type));
    auto data_slot = ring_cell->data_buffer_offset;
    auto table_id = ring_cell->table_id;
    auto sorting_id = ring_cell->sorting_id;
    auto contained_rows = ring_cell->contained_rows;
    auto addressed_data = _data_buffer->get_addressed_by(ring_cell);
    _ring_buffer->retrieval_finished();
    timer.lap();
    table_builder->read_data(table_id, sorting_id, addressed_data, contained_rows);
    appending_time += timer.lap().count();
    _return_data_slot(data_slot);
    // In order to start encoding only after we have returned the data slot, only declare that we have successfully added some values to the segments now.
    table_builder->declare_values_added(sorting_id, contained_rows);
    encoding_time += timer.lap().count();
  }
  if (num_processed == 0) {
    auto out = table_builder->table_name() + ": Worker " + std::to_string(reader_index) + " had no work units!\n";
    std::cerr << out;
  } else {
    auto out = table_builder->table_name() + ": Worker " + std::to_string(reader_index) + " read " + std::to_string(num_processed) + " work units, waited " + std::to_string(waiting_time / num_processed) + " and read data " + std::to_string(appending_time / num_processed) + " and encoded " + std::to_string(encoding_time / num_processed) + "\n";
    std::cerr << out;
  }
}

template class SharedMemoryReader<   8u, 16u>; // 65536
template class SharedMemoryReader<  16u, 16u>; // 32768
template class SharedMemoryReader<  32u, 16u>; // 16384
template class SharedMemoryReader<  64u, 16u>; //  8192
template class SharedMemoryReader< 128u, 16u>; //  4096 buffer size
template class SharedMemoryReader< 256u, 16u>; //  2048
template class SharedMemoryReader< 512u, 16u>; //  1024
template class SharedMemoryReader<1024u, 16u>; //   512
template class SharedMemoryReader<2048u, 16u>; //   256
template class SharedMemoryReader<4096u, 16u>; //   128
template class SharedMemoryReader<8192u, 16u>; //    64

template class SharedMemoryReader<   8u, 32u>; // 32768
template class SharedMemoryReader<  16u, 32u>; // 16384
template class SharedMemoryReader<  32u, 32u>; //  8192
template class SharedMemoryReader<  64u, 32u>; //  4096
template class SharedMemoryReader< 128u, 32u>; //  2048 buffer size
template class SharedMemoryReader< 256u, 32u>; //  1024
template class SharedMemoryReader< 512u, 32u>; //   512
template class SharedMemoryReader<1024u, 32u>; //   256
template class SharedMemoryReader<2048u, 32u>; //   128
template class SharedMemoryReader<4096u, 32u>; //    64
template class SharedMemoryReader<8192u, 32u>; //    32
} // namespace hyrise
