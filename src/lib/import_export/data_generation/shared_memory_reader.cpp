#include <cstdint>
#include <stdexcept>
#include <sys/mman.h>
#include <csignal>
#include <fcntl.h>
#include "import_export/data_generation/shared_memory_reader.hpp"

namespace hyrise {
template <uint32_t work_unit_size, uint32_t num_columns>
SharedMemoryReader<work_unit_size, num_columns>::SharedMemoryReader(
    const char* shared_memory_name, const char* data_ready_sem, const char* buffer_free_sem)
    : _shared_memory_file_name(shared_memory_name) {
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
                                                                       data_ready_sem, buffer_free_sem);
}

template <uint32_t work_unit_size, uint32_t num_columns>
SharedMemoryReader<work_unit_size, num_columns>::~SharedMemoryReader() {
  if (_data_buffer != MAP_FAILED) {
    std::cerr << "Unmapping data buffer" << std::endl;
    munmap(_data_buffer, sizeof(DataBuffer<buffer_size, work_unit_size, num_columns>));
  }
  if (_shm_fd != -1) {
    std::cerr << "Closing shared memory file" << std::endl;
    close(_shm_fd);
    shm_unlink(_shared_memory_file_name);
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
void SharedMemoryReader<work_unit_size, num_columns>::read_data() {
  // TODO: how to know we are finished

  while (_read_next_cell()) {}
}

template <uint32_t work_unit_size, uint32_t num_columns>
bool SharedMemoryReader<work_unit_size, num_columns>::_read_next_cell() {
  auto buffer_cell = _ring_buffer->prepare_retrieval();
  auto buffer_offset = buffer_cell->data_buffer_offset;
  switch (buffer_cell->cell_type) {
    case RingBufferCellType::TableInfo:
      _read_table_info_cell(buffer_cell);
      break;
    case RingBufferCellType::Data:
      _read_data_cell(buffer_cell);
      break;
    case RingBufferCellType::TableCompleted:
      _read_table_complete_cell(buffer_cell);
      break;
    default:
      throw std::runtime_error("Unknown cell type encountered!");
  }

  _return_data_slot(buffer_offset);

  if (_num_read_tables == _num_tables_to_read) {
    return false;
  }

  return true;
}

template <uint32_t work_unit_size, uint32_t num_columns>
void SharedMemoryReader<work_unit_size, num_columns>::_return_data_slot(uint32_t buffer_offset) {
  auto buffer_cell = _ring_buffer->prepare_writing();
  buffer_cell->cell_type = RingBufferCellType::Noop;
  buffer_cell->data_buffer_offset = buffer_offset;
  _ring_buffer->writing_finished();
}

template <uint32_t work_unit_size, uint32_t num_columns>
void SharedMemoryReader<work_unit_size, num_columns>::_read_table_info_cell(RingBufferCell * cell) {
  std::cerr << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" << std::endl;
  std::cerr << "TABLE ID " << cell->table_id << std::endl;
  std::cerr << "NUM ROWS " << cell->table_num_rows << std::endl;

  auto addressed_data = _addressed_data_by_cell(cell);
  _ring_buffer->retrieval_finished();

  _num_tables_to_read = *((uint32_t*)addressed_data->data[0][0]);

  std::cerr << "TABLE NAME " << std::string(addressed_data->data[1][0]) << std::endl;
  std::cerr << "FIELDS OVERVIEW" << std::endl;
  auto table_num_columns = *((uint32_t*)addressed_data->data[1][1]);
  for (uint32_t i = 0; i < table_num_columns; ++i) {
    std::cerr << i << " " << std::string(addressed_data->data[2][i]) << " "
              << *((uint32_t*)addressed_data->data[3][i]) << std::endl;
  }
}

template <uint32_t work_unit_size, uint32_t num_columns>
void SharedMemoryReader<work_unit_size, num_columns>::_read_data_cell(RingBufferCell * cell) {
  // std::cout << "Sort #" << cell->sorting_id << std::endl;
  _ring_buffer->retrieval_finished();
}

template <uint32_t work_unit_size, uint32_t num_columns>
void SharedMemoryReader<work_unit_size, num_columns>::_read_table_complete_cell(RingBufferCell * cell) {
  std::cerr << "==> TABLE FINISHED" << std::endl;
  _num_read_tables++;
  _ring_buffer->retrieval_finished();
}

template <uint32_t work_unit_size, uint32_t num_columns>
DataCell<work_unit_size, num_columns>* SharedMemoryReader<work_unit_size, num_columns>::_addressed_data_by_cell(
    RingBufferCell * cell) {
  return &_data_buffer->data[cell->data_buffer_offset / work_unit_size / num_columns / SHARED_MEMORY_FIELD_SIZE];
}
} // namespace hyrise
