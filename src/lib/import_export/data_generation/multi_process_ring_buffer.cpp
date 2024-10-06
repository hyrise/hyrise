#include <semaphore.h>
#include <fcntl.h>
#include <stdexcept>
#include <sys/mman.h>
#include <iostream>
#include "import_export/data_generation/multi_process_ring_buffer.hpp"

namespace hyrise {
template <uint32_t buffer_size>
MultiProcessRingBuffer<buffer_size>::MultiProcessRingBuffer(int shm_fd, uint32_t workunit_size, uint32_t num_columns,
                                                            const char* data_available_sem_path,
                                                            const char* data_written_sem_path)
    : _workunit_size(workunit_size),
      _num_columns(num_columns),
      _data_available_sem_path(data_available_sem_path),
      _current_read_index(0),
      _data_written_sem_path(data_written_sem_path),
      _current_write_index(0) {
  _ring_buffer = static_cast<RingBuffer<buffer_size>*>(
      mmap(nullptr, sizeof(RingBuffer<buffer_size>), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0));
  if (_ring_buffer == MAP_FAILED) {
    throw std::runtime_error("Ring buffer memory mapping failed");
  }

  // Remove existing ones just in case so that we have a fresh state
  // We don't care about errors (such as the semaphore not existing) here
  sem_unlink(_data_available_sem_path);
  sem_unlink(_data_written_sem_path);

  _data_available_semaphore = sem_open(data_available_sem_path, O_CREAT, 0666, 0);
  _data_written_semaphore = sem_open(data_written_sem_path, O_CREAT, 0666, buffer_size);

  if (_data_available_semaphore == SEM_FAILED || _data_written_semaphore == SEM_FAILED) {
    throw std::runtime_error("Semaphore opening failed");
  }

  _initialize();
}

template <uint32_t buffer_size>
MultiProcessRingBuffer<buffer_size>::~MultiProcessRingBuffer() {
  if (_ring_buffer != MAP_FAILED) {
    std::cerr << "Unmapping ring buffer" << std::endl;
    munmap(_ring_buffer, sizeof(RingBuffer<buffer_size>));
  }

  if (_data_available_semaphore != SEM_FAILED) {
    std::cerr << "Unlinking semaphore" << std::endl;
    sem_close(_data_available_semaphore);
    sem_unlink(_data_available_sem_path);
  }

  if (_data_written_semaphore != SEM_FAILED) {
    std::cerr << "Unlinking semaphore" << std::endl;
    sem_close(_data_written_semaphore);
    sem_unlink(_data_written_sem_path);
  }
}

template <uint32_t buffer_size>
void MultiProcessRingBuffer<buffer_size>::_initialize() {
  // All dataslots are initially available for writing by PDGF.
  for (uint32_t i = 0; i < buffer_size; ++i) {
    auto& cell = _ring_buffer->cells[i];
    cell.cell_type = RingBufferCellType::Noop;
    cell.data_buffer_offset = i * _workunit_size * _num_columns * SHARED_MEMORY_FIELD_SIZE;
  }
}

template <uint32_t buffer_size>
RingBufferCell* MultiProcessRingBuffer<buffer_size>::prepare_retrieval() {
  sem_wait(_data_available_semaphore);
  _read_access_semaphore.lock();
  return &_ring_buffer->cells[_current_read_index % buffer_size];
}

template <uint32_t buffer_size>
void MultiProcessRingBuffer<buffer_size>::retrieval_finished() {
  _current_read_index++;
  _read_access_semaphore.unlock();
}

template <uint32_t buffer_size>
RingBufferCell* MultiProcessRingBuffer<buffer_size>::prepare_writing() {
  _write_access_semaphore.lock();

  // Invariant: Me must retrieve the data first before we write new one!
  // Also, for this reason, we don't check if there actually is free space in this method, because retrieving the data for this slot
  // must have freed it up.
  if (_current_write_index >= _current_read_index) {
    throw std::runtime_error("Data must be retrieved first before writing new one!");
  }

  return &_ring_buffer->cells[_current_write_index % buffer_size];
}

template <uint32_t buffer_size>
void MultiProcessRingBuffer<buffer_size>::writing_finished() {
  _current_write_index++;
  _write_access_semaphore.unlock();
  sem_post(_data_written_semaphore);
}

template class MultiProcessRingBuffer<4096u>;
} // namespace hyrise
