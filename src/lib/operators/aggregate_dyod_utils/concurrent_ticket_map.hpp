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
#include <utility>
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

  // We do not avalanche (using `fmix64`) here, because this is done inside `ConcurrentTicketMap::try_emplace`.
  return h;
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
    _probe_limit = _max_probe_count(capacity);

    auto* new_slots_ptr = static_cast<Slot*>(std::calloc(_capacity, sizeof(Slot)));
    Assert(new_slots_ptr, "Failed to allocate memory for ConcurrentTicketMap.");
    _slots = SlotArray{new_slots_ptr};
    _prefault();
  }

  // Registers the calling thread as being inside the slot array for as long as this object lives. Probing itself takes
  // no lock: `_probers` only counts who is currently inside the table, so the hot path never blocks. A resizer
  // announces itself with `_is_resizing` and then waits for that count to reach zero, which makes it the sole owner of
  // the array it is about to replace and free.
  //
  // Hold one scope across a whole *batch* of `try_emplace` calls - one per chunk in the aggregate, not one per row.
  // See `try_emplace` for why the registration is not taken per call.
  class ProbeScope {
   public:
    explicit ProbeScope(ConcurrentTicketMap& map) : _map(map) {
      _acquire();
    }

    ~ProbeScope() {
      _release();
    }

    ProbeScope(const ProbeScope&) = delete;
    ProbeScope& operator=(const ProbeScope&) = delete;
    ProbeScope(ProbeScope&&) = delete;
    ProbeScope& operator=(ProbeScope&&) = delete;

   private:
    friend class ConcurrentTicketMap;

    // Register *before* testing the flag, and back out if it is set. Only in this order does `_probers == 0` tell a
    // resizer both that nobody is still inside the table and that every later arrival will back out here.
    //
    // Neither side may miss the other - a prober registering just as a resize starts, or a resizer reading a count
    // that has not become visible yet - so the four handshake operations (`_probers.fetch_add` and `_is_resizing.test`
    // here, `test_and_set` and `_probers.load` in `_grow`) are left at the default seq_cst. Anything weaker lets each
    // side observe the other's flag/count as stale and both proceed.
    void _acquire() {
      while (true) {
        _map._probers.fetch_add(1);
        if (!_map._is_resizing.test()) {
          return;
        }
        _map._probers.fetch_sub(1, std::memory_order_release);
        while (_map._is_resizing.test()) {
          // Spin until the resize is done. We hold no registration here, so the resizer can still drain.
        }
      }
    }

    void _release() {
      _map._probers.fetch_sub(1, std::memory_order_release);
    }

    ConcurrentTicketMap& _map;
  };

  ProbeScope probe_scope() {
    return ProbeScope{*this};
  }

  // The caller must hold a `ProbeScope` for the whole batch of probes; passing it in is what makes that a compile-time
  // requirement rather than a convention. The registration is deliberately *not* taken per call: at high cardinality
  // this runs once per row on every worker, and a shared-cache-line atomic RMW here serializes the entire ticketing
  // phase - the `_probers` line ping-pongs between cores and each probe waits its turn on it. Hoisted out to once per
  // chunk, the same handshake costs nothing measurable.
  uint64_t try_emplace(const Key& key, const uint64_t ticket, ProbeScope& scope) {
    DebugAssert(&scope._map == this, "ProbeScope belongs to a different ConcurrentTicketMap.");

    // Mix the hash before it selects a slot. `Hash` is not required to avalanche - `std::hash` of an integer is the
    // identity - and under linear probing a strided key distribution then collapses into long clusters, which costs
    // probes and eventually drives the `_grow` fallback below. `slot.hash` stores the mixed value, so `resize` reuses
    // it as-is and must not mix again.
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
            // `key` and `hash` must both be written *before* the slot is published: the store to `state` below is the
            // release that makes them visible, and a probing thread that observes the published state goes on to read
            // `slot.hash`. Publishing first would let that thread read a still-zero hash, miss the key it is looking
            // for and insert the same group a second time under a different ticket.
            slot.key = key;
            slot.hash = hash;
            slot.state = ticket + TICKET_BIAS;
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
      }

      // The probe chain ran past `_probe_limit`. Grow, then retry against the new table - ours if we won the flag,
      // otherwise the winner's.
      _grow(scope);
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
            new_slot.hash = hash;
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

  // Rewrites every published ticket in place via `fn`. Called only after all `try_emplace`s have joined (so there is no
  // concurrent access), which lets us use relaxed atomics and partition the slot range across scheduler jobs. Each slot
  // is independent, so `fn` is invoked concurrently and must be thread-safe (read-only over its captures).
  template <typename Fn>
  void remap_tickets(Fn&& fn) {
    _parallel_for_slot_ranges([this, &fn](const size_t begin, const size_t end) {
      for (auto index = begin; index < end; ++index) {
        const auto state = _slots[index].state.load(std::memory_order_relaxed);
        if (state >= TICKET_BIAS) {
          _slots[index].state.store(fn(state - TICKET_BIAS) + TICKET_BIAS, std::memory_order_relaxed);
        }
      }
    });
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
    // The slots come from `std::calloc`, so they must go back to `std::free` - `::operator delete[]` is not a valid
    // pairing for it. `Slot` is trivially destructible, so there is no per-element teardown to do either.
    void operator()(Slot* slots) const noexcept {
      std::free(static_cast<void*>(slots));
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
  static constexpr auto MIN_PROBE_COUNT = size_t{8};

  // Number of slots we probe before we give up and resize. Under linear probing, an unsuccessful lookup at load factor
  // `load_factor` needs 0.5 * (1 + 1 / (1 - load_factor)^2) probes on average (Knuth, TAOCP 3, 6.4), i.e., ~6 probes
  // at 0.7. The worst-case cluster grows with the table size, so we scale the average by log2(capacity) instead of
  // using a fixed bound: that way a small table does not resize on a single unlucky cluster and a large one still
  // bails out long before it degrades into a linear scan.
  static constexpr size_t _max_probe_count(const size_t capacity, const double load_factor = MAX_LOAD_FACTOR) {
    const auto average_probes = 0.5 * (1.0 + 1.0 / ((1.0 - load_factor) * (1.0 - load_factor)));
    // `capacity` is a power of two, so `bit_width - 1` is an exact log2.
    const auto log2_capacity = static_cast<double>(std::bit_width(capacity) - 1);
    const auto probe_count = static_cast<size_t>(average_probes * log2_capacity);
    return std::min(capacity, std::max(MIN_PROBE_COUNT, probe_count));
  }

  // Splits [0, _capacity) into one range per core and runs `fn(begin, end)` on each, falling back to a direct call
  // when the table is too small for the scheduling overhead to pay off.
  template <typename Fn>
  void _parallel_for_slot_ranges(Fn&& fn) {
    if (_capacity <= PARALLEL_INIT_SLOTS) {
      fn(size_t{0}, _capacity);
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
      jobs.emplace_back(std::make_shared<JobTask>([&fn, begin, end] {
        fn(begin, end);
      }));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  // `std::calloc` hands back lazily mapped zero pages, so without this the first-touch faults land inside the probe
  // loop instead - in random slot order, on whichever chunk gets there first. At high cardinality that is ruinous: a
  // chunk of N rows probes N random slots, so it alone faults in `1 - e^-1` ≈ 63% of a table with N pages, one fault
  // at a time, with every worker piling into the same VM object. Touching the pages up front makes those same faults
  // happen sequentially and in parallel, which is far friendlier to the kernel and the TLB.
  //
  // One store per page is enough to materialize it - there is no need to write all 33M slots to fault in 65k pages.
  // The value written is `EMPTY`, which is what `calloc` already guarantees, so this only maps pages.
  void _prefault() {
    // A 4 KiB stride touches every page on any platform we run on (macOS/arm64 uses 16 KiB, x86-64 uses 4 KiB).
    static constexpr auto MIN_PAGE_SIZE = size_t{4096};
    const auto stride = std::max(size_t{1}, MIN_PAGE_SIZE / sizeof(Slot));

    _parallel_for_slot_ranges([this](const size_t begin, const size_t end) {
      for (auto index = begin; index < end; index += stride) {
        _slots[index].state.store(EMPTY, std::memory_order_relaxed);
      }
    });
  }

  // Cold path out of `try_emplace`: double the table, then return with our registration re-taken so the caller can
  // retry. We must drop our own registration first - the drain below waits for the prober count to reach zero and
  // would otherwise wait on ourselves. Re-acquiring at the end also parks us until a resize we lost the race for has
  // finished, so the caller always retries against a table that is done growing.
  void _grow(ProbeScope& scope) {
    scope._release();
    if (!_is_resizing.test_and_set()) {
      while (_probers.load() > 0) {
        // Spin until every other prober has left, so the array we replace and free below is ours alone.
      }
      resize(_capacity * 2);
      _is_resizing.clear();
    }
    scope._acquire();
  }

  // Read-mostly after `initialize`, and read on every probe.
  SlotArray _slots;
  size_t _capacity = 0;
  size_t _mask = 0;
  size_t _probe_limit = 0;
  Hash _hash{};
  KeyEqual _key_equal{};
  // Set while a resize is in flight; `_probers` counts the threads currently inside the slot array. Together they let a
  // resizer take exclusive ownership of the array without probers ever taking a lock (see `ProbeScope`). They get their
  // own cache line: a `ProbeScope` writes `_probers` on every acquire, and sharing a line with the hot metadata above
  // would invalidate that line on other cores along with it.
  alignas(64) std::atomic_flag _is_resizing;
  std::atomic<uint32_t> _probers;
};

}  // namespace hyrise
