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
#include <mutex>
#include <type_traits>
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
//   state == MIGRATED (2) : the entry has been copied into a larger table, this one is retired
//   state >= TICKET_BIAS  : published, the ticket is `state - TICKET_BIAS`
//
// The table is sized up front from a cardinality estimate (see `cardinality_estimation.hpp`) and normally never resizes:
// inserts are lock-free, keys never move and readers never synchronize beyond the slot they touch. Should the estimate
// undershoot, the map falls back to migrating every entry into a table of twice the size (see `_grow`). That fallback is
// deliberately simple rather than fast - one thread copies while the others block - because a good estimate makes it
// rare. What it buys is that an undershooting estimate merely costs time instead of overfilling the table, which used to
// spin in `try_emplace` forever.
template <typename Key, typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class ConcurrentTicketMap {
  // Slots are `calloc`ed and never constructed, and migration copies keys byte-wise between tables.
  static_assert(std::is_trivially_copyable_v<Key>, "ConcurrentTicketMap keys must be trivially copyable.");

 public:
  ConcurrentTicketMap() = default;

  // Sizes the table to hold at least `max_groups` entries below the load factor, rounded up to a power of two. Exceeding
  // `max_groups` is handled by growing the table, so the value only has to be a good guess, not a hard bound.
  explicit ConcurrentTicketMap(const size_t max_groups, const Hash& hash = Hash{},
                               const KeyEqual& key_equal = KeyEqual{})
      : _hash(hash), _key_equal(key_equal) {
    _publish(_allocate_table(max_groups));
  }

  ConcurrentTicketMap(const ConcurrentTicketMap&) = delete;
  ConcurrentTicketMap& operator=(const ConcurrentTicketMap&) = delete;

  // Moving is only safe while no other thread touches either map, i.e. before the grouping pass starts.
  ConcurrentTicketMap(ConcurrentTicketMap&& other) noexcept {
    _adopt(std::move(other));
  }

  ConcurrentTicketMap& operator=(ConcurrentTicketMap&& other) noexcept {
    if (this != &other) {
      _adopt(std::move(other));
    }
    return *this;
  }

  // Returns the ticket of `key`: `ticket` if this call inserted it, the stored one otherwise. Callers hand out tickets
  // from disjoint per-thread ranges, so a return value equal to `ticket` identifies the inserting call.
  uint64_t try_emplace(const Key& key, const uint64_t ticket) {
    const auto key_hash = _mixed_hash(key);
    auto probe_budget = MAX_PROBE_LENGTH;

    while (true) {
      auto* const table = _table.load(std::memory_order_acquire);
      const auto result = _try_emplace_in(*table, key, key_hash, ticket, probe_budget);

      if (result == RETIRED) [[unlikely]] {
        // The table was migrated away under us. Probe the new one.
        probe_budget = MAX_PROBE_LENGTH;
        continue;
      }

      if (result == BUDGET_EXHAUSTED) [[unlikely]] {
        // A probe this long means either the table is genuinely out of room, or many keys collide into one region and
        // growing would not help. Only the first case is worth a migration, so check the load before paying for one.
        if (probe_budget >= table->capacity || _is_overfull(*table)) {
          _grow(table);
          probe_budget = MAX_PROBE_LENGTH;
        } else {
          probe_budget = table->capacity;  // There is room, our region is just crowded: keep probing.
        }
        continue;
      }

      return result;
    }
  }

  template <typename Fn>
  void for_each(Fn&& fn) const {
    const auto& table = *_table.load(std::memory_order_acquire);
    for (auto index = size_t{0}; index < table.capacity; ++index) {
      const auto state = table.slots[index].state.load();
      if (state >= TICKET_BIAS) {
        fn(table.slots[index].key, state - TICKET_BIAS);
      }
    }
  }

  template <typename Fn>
  void remap_tickets(Fn&& fn) {
    auto& table = *_table.load(std::memory_order_acquire);

    const auto remap_range = [&table, &fn](const size_t begin, const size_t end) {
      for (auto index = begin; index < end; ++index) {
        const auto state = table.slots[index].state.load(std::memory_order_relaxed);
        if (state >= TICKET_BIAS) {
          table.slots[index].state.store(fn(state - TICKET_BIAS) + TICKET_BIAS, std::memory_order_relaxed);
        }
      }
    };

    if (table.capacity <= PARALLEL_INIT_SLOTS) {
      remap_range(0, table.capacity);
      return;
    }

    const auto max_jobs = (table.capacity + PARALLEL_INIT_SLOTS - 1) / PARALLEL_INIT_SLOTS;
    const auto job_count = std::min<size_t>(Hyrise::get().topology.num_cpus(), max_jobs);
    const auto slots_per_job = table.capacity / job_count;
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(job_count);

    for (auto job_id = size_t{0}; job_id < job_count; ++job_id) {
      const auto begin = job_id * slots_per_job;
      const auto end = job_id + 1 == job_count ? table.capacity : begin + slots_per_job;
      jobs.emplace_back(std::make_shared<JobTask>([&remap_range, begin, end] {
        remap_range(begin, end);
      }));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  size_t capacity() const {
    return _table.load(std::memory_order_acquire)->capacity;
  }

  // How often the table had to grow because the cardinality estimate was too low. Zero on a well-estimated query. Only
  // meaningful once all inserting threads have finished, as growing appends to `_tables`.
  size_t growth_count() const {
    return _tables.empty() ? 0 : _tables.size() - 1;
  }

 private:
  struct Slot {
    std::atomic<uint64_t> state;
    Key key{};
  };

  struct SlotArrayDeleter {
    void operator()(Slot* slots) const noexcept {
      std::free(slots);
    }
  };

  using SlotArray = std::unique_ptr<Slot[], SlotArrayDeleter>;

  // One generation of the table.
  struct Table {
    SlotArray slots;
    size_t capacity = 0;
    size_t mask = 0;
  };

  static constexpr auto EMPTY = uint64_t{0};
  static constexpr auto CLAIMED = uint64_t{1};
  static constexpr auto MIGRATED = uint64_t{2};
  static constexpr auto TICKET_BIAS = uint64_t{3};
  static constexpr auto MIN_CAPACITY = size_t{16};
  static constexpr auto MAX_LOAD_FACTOR = 0.7;
  static constexpr auto PARALLEL_INIT_SLOTS = size_t{1} << 18;

  // How far a single probe may walk before the table is suspected of being out of room. Below the load factor, linear
  // probing produces runs of a handful of slots and a run this long is astronomically unlikely, while an overfull table
  // produces them constantly. Checking the probe length costs a loop counter, whereas tracking the occupancy exactly
  // would put a contended atomic on the insert path.
  static constexpr auto MAX_PROBE_LENGTH = size_t{256};

  // Slots sampled to decide whether a long probe was caused by an overfull table or by colliding keys.
  static constexpr auto LOAD_SAMPLE_SLOTS = size_t{1024};

  // Out-of-band results of `_try_emplace_in`. Neither is a valid ticket: tickets are bounded by the group count.
  static constexpr auto RETIRED = std::numeric_limits<uint64_t>::max();
  static constexpr auto BUDGET_EXHAUSTED = std::numeric_limits<uint64_t>::max() - 1;

  size_t _mixed_hash(const Key& key) const {
    return static_cast<size_t>(fmix64(static_cast<uint64_t>(_hash(key))));
  }

  // Sizes a table to hold `max_groups` entries below the load factor.
  std::unique_ptr<Table> _allocate_table(const size_t max_groups) const {
    auto capacity = MIN_CAPACITY;
    while (static_cast<double>(capacity) * MAX_LOAD_FACTOR < static_cast<double>(max_groups + 1)) {
      capacity <<= 1;
    }
    return _allocate_table_with_capacity(capacity);
  }

  std::unique_ptr<Table> _allocate_table_with_capacity(const size_t capacity) const {
    auto* const slots_ptr = static_cast<Slot*>(std::calloc(capacity, sizeof(Slot)));
    Assert(slots_ptr, "Failed to allocate memory for ConcurrentTicketMap.");

    auto table = std::make_unique<Table>();
    table->slots = SlotArray{slots_ptr};
    table->capacity = capacity;
    table->mask = capacity - 1;
    return table;
  }

  // Whether a strided sample of the slots puts the table above its load factor. Only called after a probe ran long: a
  // crowded region in an otherwise empty table means colliding keys, which a larger table would not spread out.
  bool _is_overfull(const Table& table) const {
    const auto sample_slots = std::min(LOAD_SAMPLE_SLOTS, table.capacity);
    // An odd stride is invertible modulo the power-of-two capacity, so the sample hits `sample_slots` distinct slots.
    const auto stride = (table.capacity / sample_slots) | size_t{1};

    auto occupied_slots = size_t{0};
    for (auto sample_index = size_t{0}; sample_index < sample_slots; ++sample_index) {
      const auto index = (sample_index * stride) & table.mask;
      if (table.slots[index].state.load(std::memory_order_relaxed) != EMPTY) {
        ++occupied_slots;
      }
    }

    return static_cast<double>(occupied_slots) > static_cast<double>(sample_slots) * MAX_LOAD_FACTOR;
  }

  // Makes `table` the current generation. Retired generations stay alive in `_tables` until the map is destroyed: a
  // thread may still be probing one, and keeping a handful of them around is far cheaper than the epoch reclamation
  // that freeing them safely would need. Only ever called from the constructor or with `_resize_mutex` held.
  void _publish(std::unique_ptr<Table> table) {
    auto* const table_ptr = table.get();
    _tables.emplace_back(std::move(table));
    _table.store(table_ptr, std::memory_order_release);
  }

  // Probes at most `probe_budget` slots of `table`. Returns the key's ticket, `RETIRED` if the table was migrated away
  // mid-probe, or `BUDGET_EXHAUSTED` if the budget ran out before an empty slot or the key showed up.
  uint64_t _try_emplace_in(Table& table, const Key& key, const size_t key_hash, const uint64_t ticket,
                           const size_t probe_budget) const {
    auto index = key_hash & table.mask;

    for (auto probe = size_t{0}; probe < probe_budget; ++probe) {
      auto& slot = table.slots[index];
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

      // Wait for a concurrent inserter to publish its key before reading it.
      while (state == CLAIMED) {
        state = slot.state.load();
      }

      // Checked before touching the key: a slot retired straight from `EMPTY` carries no key at all.
      if (state == MIGRATED) {
        return RETIRED;
      }

      if (_key_equal(slot.key, key)) {
        return state - TICKET_BIAS;
      }

      // Linear probing
      index = (index + 1) & table.mask;
    }

    return BUDGET_EXHAUSTED;
  }

  // Migrates every entry of `stale_table` into a table twice its size, or returns once another thread has done so. The
  // caller retries its probe on the new table afterwards.
  void _grow(Table* const stale_table) {
    const auto lock = std::lock_guard<std::mutex>{*_resize_mutex};
    if (_table.load(std::memory_order_acquire) != stale_table) {
      return;  // Another thread already migrated away from `stale_table`.
    }

    auto new_table = _allocate_table_with_capacity(stale_table->capacity * 2);

    // Retire the slots in index order, waiting for in-flight inserts to publish, and copy the published entries over.
    // An inserter cannot slip past us: it either publishes into a slot we have not retired yet - which we then migrate -
    // or its claim fails against `MIGRATED` and it ends up blocked on the mutex above. Because `_table` is published
    // only after the last slot has been retired, nobody inserts into `new_table` while we fill it, so no group can end
    // up with two tickets.
    for (auto index = size_t{0}; index < stale_table->capacity; ++index) {
      auto& slot = stale_table->slots[index];
      auto state = slot.state.load(std::memory_order_acquire);

      while (true) {
        while (state == CLAIMED) {
          state = slot.state.load(std::memory_order_acquire);
        }
        auto expected = state;
        if (slot.state.compare_exchange_strong(expected, MIGRATED)) {
          break;
        }
        state = expected;
      }

      if (state >= TICKET_BIAS) {
        _insert_exclusive(*new_table, slot.key, state - TICKET_BIAS);
      }
    }

    _publish(std::move(new_table));
  }

  // Inserts into a table that is not published yet, so no other thread can be probing it. The keys migrated out of a
  // single table are distinct by construction, hence no equality check.
  void _insert_exclusive(Table& table, const Key& key, const uint64_t ticket) const {
    auto index = _mixed_hash(key) & table.mask;
    while (table.slots[index].state.load(std::memory_order_relaxed) != EMPTY) {
      index = (index + 1) & table.mask;
    }
    table.slots[index].key = key;
    table.slots[index].state.store(ticket + TICKET_BIAS, std::memory_order_relaxed);
  }

  void _adopt(ConcurrentTicketMap&& other) noexcept {
    _tables = std::move(other._tables);
    _table.store(other._table.load(std::memory_order_relaxed), std::memory_order_relaxed);
    other._table.store(nullptr, std::memory_order_relaxed);
    _resize_mutex = std::move(other._resize_mutex);
    _hash = std::move(other._hash);
    _key_equal = std::move(other._key_equal);
  }

  // The current generation, and every generation ever published. `_table` always points into `_tables`.
  std::atomic<Table*> _table{nullptr};
  std::vector<std::unique_ptr<Table>> _tables;

  // Held while migrating. Threads that ran into a retired or overfull table block here, so the migrating thread has the
  // old table to itself once they do. Behind a pointer to keep the map movable.
  std::unique_ptr<std::mutex> _resize_mutex = std::make_unique<std::mutex>();

  Hash _hash{};
  KeyEqual _key_equal{};
};

}  // namespace hyrise
