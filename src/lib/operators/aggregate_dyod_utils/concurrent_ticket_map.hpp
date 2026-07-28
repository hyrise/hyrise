#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "utils/assert.hpp"

inline std::uint64_t rotl(std::uint64_t x, int r) {
  return (x << r) | (x >> (64 - r));
}

inline std::uint64_t read64(const unsigned char* p) {
  std::uint64_t v;
  std::memcpy(&v, p, sizeof v);  // single mov, no UB
  return v;
}

inline std::uint64_t fmix64(std::uint64_t k) {
  k ^= k >> 33;
  k *= 0xff51afd7ed558ccdULL;
  k ^= k >> 33;
  k *= 0xc4ceb9fe1a85ec53ULL;
  k ^= k >> 33;
  return k;
}

inline std::uint64_t compute_hash(const void* key, std::size_t len, std::uint64_t seed = 0) {
  const unsigned char* p = static_cast<const unsigned char*>(key);
  const std::size_t nblocks = len / 8;

  constexpr std::uint64_t c1 = 0x87c37b91114253d5ULL;
  constexpr std::uint64_t c2 = 0x4cf5ad432745937fULL;

  std::uint64_t h = seed;

  // body: full 8-byte words
  for (std::size_t i = 0; i < nblocks; ++i) {
    std::uint64_t k = read64(p + i * 8);
    k *= c1;
    k = rotl(k, 31);
    k *= c2;
    h ^= k;
    h = rotl(h, 27);
    h = h * 5 + 0x52dce729ULL;
  }

  // tail: either nothing, or exactly 4 bytes
  if (len & 4) {
    std::uint32_t t;
    std::memcpy(&t, p + nblocks * 8, 4);  // single 32-bit load, no UB
    std::uint64_t k = t;
    k *= c1;
    k = rotl(k, 31);
    k *= c2;
    h ^= k;
    // no h = rotl/h*5+const here, matching MurmurHash3's tail
  }

  return fmix64(h);  // avalanche
}

namespace hyrise {

// Slot meanings:
//   state == EMPTY (0)    : free.
//   state == CLAIMED (1)  : a thread is writing its key
//   state >= TICKET_BIAS  : published, the ticket is `state - TICKET_BIAS`
template <typename Key, typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class ConcurrentTicketMap {
 public:
  ConcurrentTicketMap() = default;

  // Sizes the table to hold at least `max_groups` entries below the load factor, rounded up to a power of two.
  // The table never grows, `max_groups` MUST!!!!! be a true upper bound on the number of distinct groups.
  explicit ConcurrentTicketMap(const size_t max_groups, const Hash& hash = Hash{},
                               const KeyEqual& key_equal = KeyEqual{}) {
    initialize(max_groups, hash, key_equal);
  }

  void initialize(const size_t max_groups, const Hash& hash = Hash{}, const KeyEqual& key_equal = KeyEqual{}) {
    _hash = hash;
    _key_equal = key_equal;

    auto capacity = MIN_CAPACITY;
    while (static_cast<double>(capacity) * MAX_LOAD_FACTOR < static_cast<double>(max_groups + 1)) {
      capacity <<= 1;
    }
    _capacity = capacity;
    _mask = capacity - 1;

    auto* slots_ptr = static_cast<Slot*>(std::calloc(capacity, sizeof(Slot)));
    Assert(slots_ptr, "Failed to allocate memory for ConcurrentTicketMap.");
    _slots = SlotArray{slots_ptr};
  }

  uint64_t try_emplace(const Key& key, const uint64_t ticket) {
    auto probe_counter = size_t{0};
    {
      const auto lock =
          std::shared_lock<std::shared_mutex>(_is_resizing_mutex);  // shared lock to prevent resizing while probing
      const auto hash = _hash(key);
      auto index = static_cast<size_t>(hash) & _mask;
      while (true) {
        auto& slot = _slots[index];
        auto state = slot.state.load();

        if (state == EMPTY) {
          auto expected = EMPTY;
          if (slot.state.compare_exchange_strong(expected, CLAIMED)) {
            slot.key = key;
            slot.state = ticket + TICKET_BIAS;
            slot.hash = hash;
            return ticket;
          }
          state = expected;
        }

        while (state == CLAIMED) {
          state = slot.state.load();
        }

        if (hash == slot.hash && _key_equal(slot.key, key)) {
          return state - TICKET_BIAS;
        }

        // Linear probing
        index = (index + 1) & _mask;
        ++probe_counter;

        if (probe_counter >= MAX_PROBE_COUNT) {
          break;
        }
      }
    }

    if (probe_counter >= MAX_PROBE_COUNT) {
      resize(_capacity * 2);
    }

    // Retry the insertion after resizing.
    return try_emplace(key, ticket);
  }

  // NOTE: This is just a fallback and VERY slow. We should never hit it though...
  void resize(const size_t new_max_groups) {
    // Prevent probing during the resize.
    const auto lock = std::unique_lock<std::shared_mutex>(_is_resizing_mutex);

    // If another thread already resized the table we can skip this resize.
    if (new_max_groups <= static_cast<size_t>(static_cast<double>(_capacity) * MAX_LOAD_FACTOR)) {
      return;
    }

    auto new_capacity = MIN_CAPACITY;
    while (static_cast<double>(new_capacity) * MAX_LOAD_FACTOR < static_cast<double>(new_max_groups + 1)) {
      new_capacity <<= 1;
    }

    auto* new_slots_ptr = static_cast<Slot*>(std::calloc(new_capacity, sizeof(Slot)));
    Assert(new_slots_ptr, "Failed to allocate memory for ConcurrentTicketMap.");
    SlotArray new_slots{new_slots_ptr};
    const auto new_mask = new_capacity - 1;

    for (size_t i = 0; i < _capacity; ++i) {
      auto& old_slot = _slots[i];
      auto state = old_slot.state.load();
      if (state >= TICKET_BIAS) {
        const auto ticket = state - TICKET_BIAS;
        const auto& key = old_slot.key;
        const auto hash = old_slot.hash;
        auto index = hash & new_mask;

        while (true) {
          auto& new_slot = new_slots[index];
          auto expected = EMPTY;
          if (new_slot.state.compare_exchange_strong(expected, CLAIMED)) {
            new_slot.key = key;
            new_slot.state = ticket + TICKET_BIAS;
            new_slot.hash = hash;
            break;
          }
          index = (index + 1) & new_mask;  // Linear probing
        }
      }
    }

    _slots.swap(new_slots);
    _capacity = new_capacity;
    _mask = new_mask;
  }

  template <typename Fn>
  void remap_tickets(Fn&& fn) {
    const auto remap_range = [this, &fn](const size_t begin, const size_t end) {
      for (auto index = begin; index < end; ++index) {
        const auto state = _slots[index].state.load(std::memory_order_relaxed);
        if (state >= TICKET_BIAS) {
          _slots[index].state.store(fn(state - TICKET_BIAS) + TICKET_BIAS, std::memory_order_relaxed);
        }
      }
    };

    if (_capacity <= PARALLEL_INIT_SLOTS) {
      remap_range(0, _capacity);
      return;
    }

    const auto max_jobs = (_capacity + PARALLEL_INIT_SLOTS - 1) / PARALLEL_INIT_SLOTS;
    const auto job_count = std::min<size_t>(Hyrise::get().topology.num_cpus(), max_jobs);
    const auto slots_per_job = _capacity / job_count;
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(job_count);

    for (auto job_id = size_t{0}; job_id < job_count; ++job_id) {
      const auto begin = job_id * slots_per_job;
      const auto end = job_id + 1 == job_count ? _capacity : begin + slots_per_job;
      jobs.emplace_back(std::make_shared<JobTask>([&remap_range, begin, end] {
        remap_range(begin, end);
      }));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  size_t capacity() const {
    return _capacity;
  }

  template <typename Fn>
  void for_each_slot_range(const size_t first_slot, const size_t end_slot, Fn&& fn) const {
    for (auto index = first_slot; index < end_slot; ++index) {
      const auto state = _slots[index].state.load();
      if (state >= TICKET_BIAS) {
        fn(_slots[index].key, state - TICKET_BIAS);
      }
    }
  }

  template <typename Fn>
  void for_each(Fn&& fn) const {
    for_each_slot_range(0, _capacity, std::forward<Fn>(fn));
  }

 private:
  struct Slot {
    std::atomic<uint64_t> state;
    uint64_t hash;
    Key key{};
  };

  struct SlotArrayDeleter {
    void operator()(Slot* slots) const noexcept {
      std::free(slots);
    }
  };

  using SlotArray = std::unique_ptr<Slot[], SlotArrayDeleter>;

  static constexpr auto EMPTY = uint64_t{0};
  static constexpr auto CLAIMED = uint64_t{1};
  static constexpr auto TICKET_BIAS = uint64_t{2};
  static constexpr auto PLACE_HOLDER = std::numeric_limits<uint64_t>::max() - TICKET_BIAS;
  static constexpr auto MIN_CAPACITY = size_t{16};
  static constexpr auto MAX_LOAD_FACTOR = 0.7;
  static constexpr auto PARALLEL_INIT_SLOTS = size_t{1} << 18;
  static constexpr auto MAX_PROBE_COUNT = size_t{32};

  SlotArray _slots;
  size_t _capacity = 0;
  size_t _mask = 0;
  Hash _hash{};
  KeyEqual _key_equal{};
  std::shared_mutex _is_resizing_mutex{};
};

}  // namespace hyrise
