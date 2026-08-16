#pragma once

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <vector>

#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"

inline std::uint64_t rotl(const std::uint64_t value, const std::uint64_t rotation) {
  return (value << rotation) | (value >> (64U - rotation));
}

inline std::uint64_t read64(const unsigned char* const bytes) {
  auto value = std::uint64_t{0};
  std::memcpy(&value, bytes, sizeof(value));
  return value;
}

inline std::uint64_t fmix64(std::uint64_t value) {
  value ^= value >> 33U;
  value *= 0xff51afd7ed558ccdULL;
  value ^= value >> 33U;
  value *= 0xc4ceb9fe1a85ec53ULL;
  value ^= value >> 33U;
  return value;
}

inline std::uint64_t compute_hash(const void* key, std::size_t len, std::uint64_t seed = 0) {
  const auto* bytes = static_cast<const unsigned char*>(key);
  const auto block_count = len / 8;

  constexpr auto MURMUR_C1 = std::uint64_t{0x87c37b91114253d5ULL};
  constexpr auto MURMUR_C2 = std::uint64_t{0x4cf5ad432745937fULL};

  auto hash = seed;

  for (auto block_index = std::size_t{0}; block_index < block_count; ++block_index) {
    auto block = read64(bytes + (block_index * 8));
    block *= MURMUR_C1;
    block = rotl(block, 31);
    block *= MURMUR_C2;
    hash ^= block;
    hash = rotl(hash, 27);
    hash = (hash * 5) + 0x52dce729ULL;
  }

  if ((len & 4U) != 0U) {
    auto tail = std::uint32_t{0};
    std::memcpy(&tail, bytes + (block_count * 8), 4);  // single 32-bit load, no UB
    auto block = std::uint64_t{tail};
    block *= MURMUR_C1;
    block = rotl(block, 31);
    block *= MURMUR_C2;
    hash ^= block;
    // no hash = rotl/hash*5+const here, matching MurmurHash3's tail
  }

  // We do not avalanche (using `fmix64`) here, because this is done inside `ConcurrentTicketMap::try_emplace`.
  return hash;
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
      capacity <<= 1U;
    }
    _capacity = capacity;
    _mask = capacity - 1;
    _probe_limit = _max_probe_count(capacity);

    // `calloc` is deliberate here, because it hands out lazily zeroed pages.
    // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory,hicpp-no-malloc)
    auto* new_slots_ptr = static_cast<Slot*>(std::calloc(_capacity, sizeof(Slot)));
    Assert(new_slots_ptr, "Failed to allocate memory for ConcurrentTicketMap.");
    _slots = SlotArray{new_slots_ptr};
    _prefault();
  }

  uint64_t try_emplace(const Key& key, const uint64_t ticket) {
    return try_emplace(key, ticket, [](const Key& probe_key) {
      return probe_key;
    });
  }

  template <typename Fn>
  uint64_t try_emplace(const Key& key, const uint64_t ticket, const Fn& promote_key) {
    const auto hash = fmix64(static_cast<uint64_t>(_hash(key)));

    while (true) {
      auto probe_counter = size_t{0};
      auto index = static_cast<size_t>(hash) & _mask;
      while (probe_counter < _probe_limit) {
        auto& slot = _slots[index];
        auto state = slot.state.load();

        if (state == EMPTY) {
          auto expected = EMPTY;
          if (slot.state.compare_exchange_strong(expected, CLAIMED)) {
            slot.key = promote_key(key);
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
        ++probe_counter;
      }

      // The probe chain ran past `_probe_limit`. Grow and then retry against the new table.
      _probers.fetch_sub(1, std::memory_order_release);
      if (!_is_resizing.test_and_set()) {
        while (_probers.load() > 0) {
          // Spin until every other prober has left.
        }
        resize(_capacity * 2);
        _is_resizing.clear();
      }
      _probers.fetch_add(1);
    }
  }

  // NOTE: This is just a fallback and VERY slow. We should never hit it though...
  void resize(const size_t new_max_groups) {
    // If another thread already resized the table we can skip this resize.
    if (new_max_groups <= static_cast<size_t>(static_cast<double>(_capacity) * MAX_LOAD_FACTOR)) {
      return;
    }

    auto new_capacity = MIN_CAPACITY;
    while (static_cast<double>(new_capacity) * MAX_LOAD_FACTOR < static_cast<double>(new_max_groups + 1)) {
      new_capacity <<= 1U;
    }

    // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory,hicpp-no-malloc)
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
        const auto hash = fmix64(static_cast<uint64_t>(_hash(key)));
        auto index = hash & new_mask;

        while (true) {
          auto& new_slot = new_slots[index];
          auto expected = EMPTY;
          if (new_slot.state.compare_exchange_strong(expected, CLAIMED)) {
            new_slot.key = key;
            new_slot.state = ticket + TICKET_BIAS;
            break;
          }
          index = (index + 1) & new_mask;  // Linear probing
        }
      }
    }

    _slots.swap(new_slots);
    _capacity = new_capacity;
    _mask = new_mask;
    _probe_limit = _max_probe_count(new_capacity);
  }

  template <typename Fn>
  void remap_tickets(const Fn& callback) {
    _parallel_for_slot_ranges([this, &callback](const size_t begin, const size_t end) {
      for (auto index = begin; index < end; ++index) {
        const auto state = _slots[index].state.load(std::memory_order_relaxed);
        if (state >= TICKET_BIAS) {
          _slots[index].state.store(callback(state - TICKET_BIAS) + TICKET_BIAS, std::memory_order_relaxed);
        }
      }
    });
  }

  size_t capacity() const {
    return _capacity;
  }

  template <typename Fn>
  void for_each_slot_range(const size_t first_slot, const size_t end_slot, const Fn& callback) const {
    for (auto index = first_slot; index < end_slot; ++index) {
      const auto state = _slots[index].state.load();
      if (state >= TICKET_BIAS) {
        callback(_slots[index].key, state - TICKET_BIAS);
      }
    }
  }

  template <typename Fn>
  void for_each(const Fn& callback) const {
    for_each_slot_range(0, _capacity, callback);
  }

  void register_prober() {
    while (true) {
      _probers.fetch_add(1);
      if (!_is_resizing.test()) {
        return;
      }
      _probers.fetch_sub(1);
      while (_is_resizing.test()) {
        // Spin until the resize is done.
      }
    }
  }

  void unregister_prober() {
    _probers.fetch_sub(1);
  }

 private:
  // The slot deliberately caches no hash.
  struct Slot {
    std::atomic<uint64_t> state;
    Key key{};
  };

  struct SlotArrayDeleter {
    void operator()(Slot* slots) const noexcept {
      // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory,hicpp-no-malloc)
      std::free(static_cast<void*>(slots));
    }
  };

  // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,hicpp-avoid-c-arrays,modernize-avoid-c-arrays)
  using SlotArray = std::unique_ptr<Slot[], SlotArrayDeleter>;

  static constexpr auto EMPTY = uint64_t{0};
  static constexpr auto CLAIMED = uint64_t{1};
  static constexpr auto TICKET_BIAS = uint64_t{2};
  static constexpr auto PLACE_HOLDER = std::numeric_limits<uint64_t>::max() - TICKET_BIAS;
  static constexpr auto MIN_CAPACITY = size_t{16};
  static constexpr auto MAX_LOAD_FACTOR = 0.7;
  static constexpr auto PARALLEL_INIT_SLOTS = size_t{1} << 18U;
  static constexpr auto MIN_PROBE_COUNT = size_t{8};

  // From Knuth's hashing result for the expected number of probes in an unsuccessful search. (The maximum probe count
  // then grows logarithmically with table size).
  static constexpr size_t _max_probe_count(const size_t capacity, const double load_factor = MAX_LOAD_FACTOR) {
    const auto average_probes = 0.5 * (1.0 + (1.0 / ((1.0 - load_factor) * (1.0 - load_factor))));
    // `capacity` is a power of two, so `bit_width - 1` is an exact log2.
    const auto log2_capacity = static_cast<double>(std::bit_width(capacity) - 1);
    const auto probe_count = static_cast<size_t>(average_probes * log2_capacity);
    return std::min(capacity, std::max(MIN_PROBE_COUNT, probe_count));
  }

  // Splits capacity into ranges and for each range runs `fn(begin, end)`.
  template <typename Fn>
  void _parallel_for_slot_ranges(const Fn& callback) {
    if (_capacity <= PARALLEL_INIT_SLOTS) {
      callback(size_t{0}, _capacity);
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
      jobs.emplace_back(std::make_shared<JobTask>([&callback, begin, end] {
        callback(begin, end);
      }));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  void _prefault() {
    static constexpr auto MIN_PAGE_SIZE = size_t{4096};
    const auto stride = std::max(size_t{1}, MIN_PAGE_SIZE / sizeof(Slot));

    _parallel_for_slot_ranges([this](const size_t begin, const size_t end) {
      for (auto index = begin; index < end; index += stride) {
        _slots[index].state.store(EMPTY, std::memory_order_relaxed);
      }
    });
  }

  SlotArray _slots;
  size_t _capacity = 0;
  size_t _mask = 0;
  size_t _probe_limit = 0;
  Hash _hash{};
  KeyEqual _key_equal{};
  std::atomic_flag _is_resizing;
  std::atomic<uint32_t> _probers;
};

}  // namespace hyrise
