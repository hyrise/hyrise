#include <cstdint>
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>

#include "hyrise.hpp"
#include "pdgf_column.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/encoding_type.hpp"


namespace hyrise {
BasePDGFColumn::BasePDGFColumn(int64_t num_rows, ChunkOffset chunk_size) : _num_rows(num_rows), _chunk_size(chunk_size) {}

BasePDGFColumn::BasePDGFColumn() : BasePDGFColumn(1, ChunkOffset{1}) {}

template <typename T>
PDGFColumn<T>::PDGFColumn(SegmentEncodingSpec encoding_spec, int64_t num_rows, ChunkOffset chunk_size) : BasePDGFColumn( num_rows, chunk_size), _encoding_spec(encoding_spec) {
  auto req_chunks = std::div(_num_rows, _chunk_size);

  auto remaining_chunks_to_generate = std::atomic_int64_t{req_chunks.quot};
  auto segment_access_mutex = std::mutex{};

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  auto num_tasks = Hyrise::get().scheduler()->num_workers(); // this is a bit of a abstraction breaker, but it should do for now
  for (auto worker_index = uint32_t{0}; worker_index < num_tasks; worker_index++) {
    tasks.emplace_back(std::make_shared<JobTask>([this, &segment_access_mutex, &remaining_chunks_to_generate] {
      while (remaining_chunks_to_generate.fetch_sub(1) > 0) {
        auto chunk_vector = pmr_vector<T>(_chunk_size);
        segment_access_mutex.lock();
        _data_segments.emplace_back(std::move(chunk_vector));
        segment_access_mutex.unlock();
      }
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
  // Add remaining smaller vector
  if (req_chunks.rem > 0) {
    auto chunk_vector = pmr_vector<T>(req_chunks.rem);
    _data_segments.emplace_back(std::move(chunk_vector));
  }

  // Directly assigning to vector is not possible (undefined behavior) -- moving uninitialized vector to normal vector appears to be not possible

  // lazy initialization?
  // multithreaded init?
  // uninitialized vector?
}

template <typename T>
void PDGFColumn<T>::virtual_add(int64_t row, char* data) {
  auto segment_index = row / _chunk_size;
  auto segment_position = row % _chunk_size;
  _data_segments[segment_index][segment_position] = * reinterpret_cast<T*>(data);
}

template<>
void PDGFColumn<pmr_string>::virtual_add(int64_t row, char* data) {
  auto segment_index = row / _chunk_size;
  auto segment_position = row % _chunk_size;

  _data_segments[segment_index][segment_position] = pmr_string(data);
}

template <typename T>
bool PDGFColumn<T>::has_another_segment() {
  return _num_built_segments < _data_segments.size();
}

template <typename T>
std::shared_ptr<AbstractSegment> PDGFColumn<T>::build_next_segment() {
  Assert(_num_built_segments < _data_segments.size(), "There are no segments left to build!");
  auto next_build_index = _num_built_segments;
  auto segment = std::make_shared<ValueSegment<T>>(std::move(_data_segments[next_build_index]));
  _num_built_segments++;
  return segment;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(PDGFColumn);
} // namespace hyrise
