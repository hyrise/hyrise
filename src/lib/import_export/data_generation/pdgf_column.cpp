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
  auto req_chunks = (_num_rows + _chunk_size - 1) / _chunk_size; // compute ceiling of division.

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  _data_segments.resize(req_chunks);
  _encoding_tasks.resize(req_chunks);
  _finished_segments.resize(req_chunks);
  _segment_initialization_locks.reserve(req_chunks);
  _segment_fullness.reserve(req_chunks);
  for (auto chunk_index = uint32_t{0}; chunk_index < req_chunks; chunk_index++) {
      _segment_initialization_locks.push_back(std::make_shared<std::mutex>());
      _segment_fullness.push_back(std::make_shared<std::atomic_uint32_t>(0));
  }

  // Directly assigning to vector is not possible (undefined behavior) -- moving uninitialized vector to normal vector appears to be not possible

  // lazy initialization?
  // multithreaded init?
  // uninitialized vector?
}

template<typename T>
void PDGFColumn<T>::initialize_segment(ChunkID chunk_id, bool should_try_continue_initialization) {
  // If the lock is gone, we know that the segment is already initialized.
  if (const auto lock = _segment_initialization_locks[chunk_id]) {
    auto locked = lock->try_lock();
    if (!locked) {
      if (chunk_id + 1 < _data_segments.size()) {
        // when multiple workers use this point at once and would have to stall, might as well already do useful work
        // and initialize next segment.
        initialize_segment(ChunkID{chunk_id + 1}, true);
      }
      // Now that we have spent our stalling time initializing a segment of ourselves (or not finding one to do so),
      // let's now wait for access to the segment we were intending to access.
      lock->lock();
    }
    if (_data_segments[chunk_id].size() == 0) {
      _data_segments[chunk_id].resize(std::min(static_cast<int64_t>(_chunk_size), _num_rows - chunk_id * _chunk_size));
    }
    _segment_initialization_locks[chunk_id] = nullptr; // no need for locking anymore once we have initialize the vector.
    lock->unlock();
  } else if (should_try_continue_initialization && chunk_id + 1 < _data_segments.size()) {
    // We are in a stall situation; other threads were as well, apparently, and already initialized the current segment.
    // However, since this does not change the fact that we are stalling, might as well continue searching for a segment
    // for us to initialize.
    initialize_segment(ChunkID{chunk_id + 1}, true);
  }
}


template <typename T>
void PDGFColumn<T>::virtual_add(int64_t row, char* data) {
  auto segment_index = row / _chunk_size;
  auto segment_position = row % _chunk_size;
  initialize_segment(static_cast<ChunkID>(segment_index), false);
  _data_segments[segment_index][segment_position] = * reinterpret_cast<T*>(data);
}

template<>
void PDGFColumn<pmr_string>::virtual_add(int64_t row, char* data) {
  auto segment_index = row / _chunk_size;
  auto segment_position = row % _chunk_size;
  initialize_segment(static_cast<ChunkID>(segment_index), false);
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
  // _encoding_tasks[segment_index] = std::make_shared<JobTask>([this, segment_index] {
  auto segment = std::make_shared<ValueSegment<T>>(std::move(_data_segments[segment_index]));
  _finished_segments[segment_index] = ChunkEncoder::encode_segment(segment, _data_type, _encoding_spec);
  // });
  // _encoding_tasks[segment_index]->schedule();
}

template <typename T>
std::shared_ptr<AbstractSegment> PDGFColumn<T>::obtain_segment(ChunkID segment_index) {
  Assert(segment_index < _finished_segments.size(), "Accessed segment out of range!");
  if (!_finished_segments[segment_index]) {
    build_segment(segment_index);
  }
  // Hyrise::get().scheduler()->wait_for_tasks({_encoding_tasks[segment_index]});
  return _finished_segments[segment_index];
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(PDGFColumn);
} // namespace hyrise
