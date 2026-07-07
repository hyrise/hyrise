#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <vector>

#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"

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
                               const KeyEqual& key_equal = KeyEqual{})
      : _hash(hash), _key_equal(key_equal) {
    auto capacity = MIN_CAPACITY;
    while (capacity * MAX_LOAD_FACTOR < (max_groups + 1)) {
      capacity <<= 1;
    }
    _capacity = capacity;
    _mask = capacity - 1;
    _slots = SlotArray{new Slot[capacity]};

    const auto initialize_slots = [slots = _slots.get()](const size_t begin, const size_t end) {
      for (auto index = begin; index < end; ++index) {
        slots[index].state.store(EMPTY);
      }
    };

    // Only parallelize once the array is large enough that partitioning beats the scheduling overhead.
    if (capacity <= PARALLEL_INIT_SLOTS) {
      initialize_slots(0, capacity);
      return;
    }

    // When `capacity` is larger than `PARALLEL_INIT_SLOTS`. We split the range into `PARALLEL_INIT_SLOTS`-sized chunks.
    const auto max_jobs = ceil(static_cast<double>(capacity) / PARALLEL_INIT_SLOTS);
    const auto job_count = std::min<size_t>(Hyrise::get().topology.num_cpus(), max_jobs);
    const auto slots_per_job = capacity / job_count;
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(job_count);

    for (auto job_id = size_t{0}; job_id < job_count; ++job_id) {
      const auto begin = job_id * slots_per_job;
      const auto end = job_id + 1 == job_count ? capacity : begin + slots_per_job;
      jobs.emplace_back(std::make_shared<JobTask>([initialize_slots, begin, end] {
        initialize_slots(begin, end);
      }));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  uint64_t try_emplace(const Key& key, const uint64_t ticket) {
    auto index = _slot_index(key);
    while (true) {
      auto& slot = _slots[index];
      auto state = slot.state.load();

      if (state == EMPTY) {
        auto expected = EMPTY;
        if (slot.state.compare_exchange_strong(expected, CLAIMED)) {
          slot.key = key;
          slot.state = ticket + TICKET_BIAS;
          return ticket;
        }
        state = expected;
      }

      while (state == CLAIMED) {
        state = slot.state.load();
      }

      if (_key_equal(slot.key, key)) {
        return state - TICKET_BIAS;
      }

      // Linear probing
      index = (index + 1) & _mask;
    }
  }

  template <typename Fn>
  void for_each(Fn&& fn) const {
    for (auto index = size_t{0}; index < _capacity; ++index) {
      const auto state = _slots[index].state.load();
      if (state >= TICKET_BIAS) {
        fn(_slots[index].key, state - TICKET_BIAS);
      }
    }
  }

  template <typename Fn>
  void remap_tickets(Fn&& fn) {
    for (auto index = size_t{0}; index < _capacity; ++index) {
      const auto state = _slots[index].state.load();
      if (state >= TICKET_BIAS) {
        _slots[index].state = fn(state - TICKET_BIAS) + TICKET_BIAS;
      }
    }
  }

  size_t capacity() const {
    return _capacity;
  }

 private:
  struct Slot {
    std::atomic<uint64_t> state;
    Key key{};
  };

  struct SlotArrayDeleter {
    void operator()(Slot* slots) const noexcept {
      delete[] slots;
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

  size_t _slot_index(const Key& key) const {
    return static_cast<size_t>(fmix64(static_cast<uint64_t>(_hash(key)))) & _mask;
  }

  SlotArray _slots;
  size_t _capacity = 0;
  size_t _mask = 0;
  Hash _hash{};
  KeyEqual _key_equal{};
};

}  // namespace hyrise
