#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include <utility>

#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "types.hpp"

namespace hyrise {

/** Runs a callback over all chunks using scheduler jobs and a shared chunk counter. */
template <typename Function>
void run_jobs_over_chunks(const size_t chunk_count, const size_t requested_job_count, const Function& function) {
  const auto job_count = std::min(requested_job_count, chunk_count);
  auto next_chunk_id = std::atomic<uint32_t>{0};
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(job_count);

  for (auto job_id = size_t{0}; job_id < job_count; ++job_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, job_id] {
      const auto next_chunk = [&]() -> std::optional<ChunkID> {
        const auto chunk_id = next_chunk_id.fetch_add(1, std::memory_order_relaxed);
        if (chunk_id >= chunk_count) {
          return std::nullopt;
        }
        return ChunkID{chunk_id};
      };

      function(job_id, next_chunk);
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
}

template <typename Function>
void run_jobs_over_chunks(const size_t chunk_count, Function&& function) {
  run_jobs_over_chunks(chunk_count, Hyrise::get().topology.num_cpus(), std::forward<Function>(function));
}

}  // namespace hyrise
