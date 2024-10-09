#include <cstdint>
#include <stdexcept>
#include <sys/mman.h>
#include <csignal>
#include <fcntl.h>

#include "shared_memory_reader.hpp"

namespace hyrise {
template <uint32_t work_unit_size, uint32_t num_columns>
SharedMemoryReader<work_unit_size, num_columns>::SharedMemoryReader(
    ChunkOffset hyrise_table_chunk_size, const char* shared_memory_name, const char* data_ready_sem, const char* buffer_free_sem)
    : _hyrise_table_chunk_size(hyrise_table_chunk_size), _shared_memory_file_name(shared_memory_name) {
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
bool SharedMemoryReader<work_unit_size, num_columns>::has_next_table() {
  return _num_read_tables < _num_tables_to_read;
}

template <uint32_t work_unit_size, uint32_t num_columns>
std::unique_ptr<PDGFTableBuilder<work_unit_size, num_columns>> SharedMemoryReader<work_unit_size, num_columns>::read_next_table() {
  // Table schema
  auto ring_cell = _ring_buffer->prepare_retrieval();
  Assert(ring_cell->cell_type == RingBufferCellType::TableSchema, "First information received by PDGF should be table schema, was " + std::to_string(ring_cell->cell_type));
  auto table_builder = std::make_unique<PDGFTableBuilder<work_unit_size, num_columns>>(ring_cell->table_id, _hyrise_table_chunk_size);

  auto data_slot = ring_cell->data_buffer_offset;
  auto addressed_data = _data_buffer->get_addressed_by(ring_cell);
  _num_tables_to_read = *((uint32_t*)addressed_data->data[0][0]);
  _ring_buffer->retrieval_finished();

  table_builder->read_schema(addressed_data);
  _return_data_slot(data_slot);

  if (table_builder->expects_more_data()) {
    // Generation info
    ring_cell = _ring_buffer->prepare_retrieval();
    Assert(ring_cell->cell_type == RingBufferCellType::TableGenerationInfo, "Did not receive table generation info, was " + std::to_string(ring_cell->cell_type));
    data_slot = ring_cell->data_buffer_offset;
    addressed_data = _data_buffer->get_addressed_by(ring_cell);
    _ring_buffer->retrieval_finished();
    table_builder->read_generation_info(addressed_data);
    _return_data_slot(data_slot);

    while (table_builder->expects_more_data()) {
      ring_cell = _ring_buffer->prepare_retrieval();
      Assert(ring_cell->cell_type == RingBufferCellType::Data, "Did not receive data, was " + std::to_string(ring_cell->cell_type));
      data_slot = ring_cell->data_buffer_offset;
      auto table_id = ring_cell->table_id;
      auto sorting_id = ring_cell->sorting_id;
      addressed_data = _data_buffer->get_addressed_by(ring_cell);
      _ring_buffer->retrieval_finished();
      table_builder->read_data(table_id, sorting_id, addressed_data);
      _return_data_slot(data_slot);
    }

  }

  ring_cell = _ring_buffer->prepare_retrieval();
  Assert(ring_cell->cell_type == RingBufferCellType::TableCompleted, "Did not receive table completed indicator, was " + std::to_string(ring_cell->cell_type));
  _num_read_tables++;
  _ring_buffer->retrieval_finished();
  _return_data_slot(ring_cell->data_buffer_offset);

  return table_builder;
}

template <uint32_t work_unit_size, uint32_t num_columns>
void SharedMemoryReader<work_unit_size, num_columns>::_return_data_slot(uint32_t buffer_offset) {
  auto buffer_cell = _ring_buffer->prepare_writing();
  buffer_cell->cell_type = RingBufferCellType::Noop;
  buffer_cell->data_buffer_offset = buffer_offset;
  _ring_buffer->writing_finished();
}
} // namespace hyrise
