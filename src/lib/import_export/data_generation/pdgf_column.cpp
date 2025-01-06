#include <cstdint>
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>

#include "hyrise.hpp"
#include "pdgf_column.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"


namespace hyrise {
BasePDGFColumn::BasePDGFColumn(DataType data_type, int64_t num_rows, ChunkOffset chunk_size) : _data_type(data_type), _num_rows(num_rows), _chunk_size(chunk_size) {}

BasePDGFColumn::BasePDGFColumn(DataType data_type) : BasePDGFColumn(data_type, 1, ChunkOffset{1}) {}

template <typename T>
PDGFColumn<T>::PDGFColumn(DataType data_type, SegmentEncodingSpec encoding_spec, int64_t num_rows, ChunkOffset chunk_size) : BasePDGFColumn(data_type, num_rows, chunk_size), _encoding_spec(encoding_spec) {
  auto req_chunks = std::div(_num_rows, _chunk_size);

  auto remaining_chunks_to_generate = std::atomic_int64_t{req_chunks.quot};
  auto num_smaller_chunks = req_chunks.rem > 0 ? 1: 0;
  auto segment_access_mutex = std::mutex{};

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  auto num_tasks = Hyrise::get().scheduler()->num_workers(); // this is a bit of a abstraction breaker, but it should do for now
  for (auto worker_index = uint32_t{0}; worker_index < num_tasks; worker_index++) {
    tasks.emplace_back(std::make_shared<JobTask>([this, &segment_access_mutex, &remaining_chunks_to_generate] {
      while (remaining_chunks_to_generate.fetch_sub(1) > 0) {
        auto chunk_vector = pmr_vector<T>(_chunk_size);
        segment_access_mutex.lock();
        _data_segments.emplace_back(std::move(chunk_vector));
        _finished_segments.emplace_back(nullptr);
        _segment_fullness.push_back(std::make_shared<std::atomic_uint32_t>(0));
        segment_access_mutex.unlock();
      }
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
  // Add remaining smaller vector
  if (num_smaller_chunks > 0) {
    auto chunk_vector = pmr_vector<T>(req_chunks.rem);
    _data_segments.emplace_back(std::move(chunk_vector));
    _finished_segments.emplace_back(nullptr);
    _segment_fullness.push_back(std::make_shared<std::atomic_uint32_t>(0));
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

template<typename T>
void PDGFColumn<T>::values_added(int64_t start_row, size_t num_values) {
  const auto segment_index = start_row / _chunk_size;
  const auto this_segment_values = std::min(static_cast<long>(num_values), (segment_index + 1) * _chunk_size - start_row);
  const auto next_segment_values = num_values - this_segment_values;

  auto this_segment_fullness = _segment_fullness[segment_index]->fetch_add(this_segment_values, std::memory_order_relaxed); // TODO(JEH): does relaxed memory order matter here performance-wise?
  this_segment_fullness += this_segment_values;
  if (this_segment_fullness >= _chunk_size) {
    build_segment(static_cast<ChunkID>(segment_index));
  }

  if (next_segment_values > 0) {
    auto next_segment_fullness = _segment_fullness[segment_index + 1]->fetch_add(next_segment_values, std::memory_order_relaxed);
    next_segment_fullness += next_segment_values;

    if (next_segment_fullness >= _chunk_size) {
      build_segment(static_cast<ChunkID>(segment_index + 1));
    }
  }
}


template <typename T>
ChunkID PDGFColumn<T>::num_segments() {
  return static_cast<ChunkID>(_data_segments.size());
}

template <typename T>
void PDGFColumn<T>::build_segment(ChunkID segment_index) {
  Assert(segment_index < _finished_segments.size(), "Accessed segment out of range!");
  Assert(!_finished_segments[segment_index], "Segment already finished!");
  auto segment = std::make_shared<ValueSegment<T>>(std::move(_data_segments[segment_index]));
  _finished_segments[segment_index] = ChunkEncoder::encode_segment(segment, _data_type, _encoding_spec);
}

template <typename T>
std::shared_ptr<AbstractSegment> PDGFColumn<T>::obtain_segment(ChunkID segment_index) {
  Assert(segment_index < _finished_segments.size(), "Accessed segment out of range!");
  if (!_finished_segments[segment_index]) {
    build_segment(segment_index);
  }
  return _finished_segments[segment_index];
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(PDGFColumn);
} // namespace hyrise
