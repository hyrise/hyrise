#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <new>
#include <type_traits>
#include <vector>

#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// A fixed-capacity, lock-free, insert-only concurrent hash table mapping a group key to a caller-supplied "ticket",
// returned directly from the insert call.
//
// It replaces `boost::concurrent_flat_map` on the single-column group-by build. That map cost an atomic,
// cross-core-contended probe on every row, and - having only a visitor API, no accessors - forced an
// insert-placeholder-then-read-back dance to recover the ticket. Here the common paths are lock free: a lookup of an
// existing group is a single acquire load plus a key compare, and a new group is one CAS. The ticket comes straight
// back from `try_emplace`, so there is no placeholder and no second lookup.
//
// Ticket assignment stays with the caller so that the fuzzy-ticketing scheme keeps working: each thread hands out
// tickets from its own claimed range and only needs the global counter (contended) once per range, not once per group.
// `try_emplace(key, ticket)` stores `ticket` when `key` is new and returns it; when `key` already exists it returns the
// existing ticket and ignores the argument. Because a range hands each ticket to exactly one thread, and a ticket only
// becomes "stored" once its group is inserted, no stored ticket equals a caller's not-yet-consumed candidate.
// Therefore `try_emplace(key, candidate) == candidate` exactly when this call inserted `key`, which is how the caller
// decides whether to advance its range cursor.
//
// The table never resizes: capacity is fixed at construction from an up-front group-count estimate the caller
// guarantees is an upper bound (a single-column group-by has at most `row_count` groups). The load factor stays below
// 1, so an empty slot always exists and probing always terminates. Only insertion is thread safe; `for_each` and
// `size()` must be called after all inserting threads have joined.
//
// Slot protocol. Each slot holds a plain `key` and an atomic `state`:
//   state == EMPTY (0)    : free.
//   state == CLAIMED (1)  : a thread won this slot and is writing its key; the key is not yet readable.
//   state >= TICKET_BIAS  : published; the group's ticket is `state - TICKET_BIAS` and `key` is stable.
// A claimer CASes EMPTY->CLAIMED, writes `key`, then release-stores the biased ticket. A reader that observes a
// published state through an acquire load is therefore guaranteed to see the fully written key. `key` is written once
// and never mutated (there is no erase), so after publication it is read as a plain, race-free load.
template <typename Key, typename Hash = std::hash<Key>>
class ConcurrentTicketMap {
  static_assert(std::is_trivially_copyable_v<Key>,
                "ConcurrentTicketMap publishes keys with a plain store/load and so needs a trivially copyable key.");

 public:
  // Sizes the table to hold at least `max_groups` entries below the load factor, rounded up to a power of two. Since
  // the table never grows, `max_groups` must be a true upper bound on the number of distinct groups.
  explicit ConcurrentTicketMap(const size_t max_groups) {
    auto capacity = MIN_CAPACITY;
    while (capacity * MAX_LOAD_PERCENT < (max_groups + 1) * 100) {
      capacity <<= 1;
    }
    _capacity = capacity;
    _mask = capacity - 1;

    // Allocate raw, uninitialized storage rather than `make_unique<Slot[]>`. Value-initializing (or zeroing) the whole
    // array is single-threaded work proportional to `capacity`, and for a large high-cardinality table it is dominated
    // by first-touch page faults - a serial cost that does not scale and drags the build down at high thread counts. We
    // instead set every slot to EMPTY in parallel below (partitioning the array across scheduler jobs) so the page
    // faults spread across cores and land core-locally. `Slot` is trivially destructible, so the matching
    // `::operator delete[]` in `SlotArrayDeleter` needs no per-element teardown.
    _slots = SlotArray{static_cast<Slot*>(::operator new[](capacity * sizeof(Slot)))};

    const auto initialize_slots = [slots = _slots.get()](const size_t begin, const size_t end) {
      // Construct each slot in the EMPTY state (value-initialized `state == 0`). Completes before any inserter runs.
      for (auto index = begin; index < end; ++index) {
        ::new (static_cast<void*>(&slots[index])) Slot{};
      }
    };

    // Only parallelize once the array is large enough that partitioning beats the scheduling overhead; otherwise the
    // serial init below is cheaper. `max_jobs` also bounds the split so each job gets a worthwhile slice.
    const auto max_jobs = capacity / PARALLEL_INIT_SLOTS;
    if (max_jobs <= 1) {
      initialize_slots(0, capacity);
      return;
    }

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

  // Returns the group's ticket. If `key` is new it is inserted with `ticket` and `ticket` is returned; otherwise the
  // existing ticket is returned and `ticket` is left unused. Thread safe against concurrent `try_emplace` calls. The
  // caller detects insertion via `result == ticket` (see the class comment).
  uint64_t try_emplace(const Key& key, const uint64_t ticket) {
    DebugAssert(ticket < PLACE_HOLDER, "Ticket collides with a reserved slot-state sentinel.");
    auto index = _slot_index(key);
    while (true) {
      auto& slot = _slots[index];
      auto state = slot.state.load(std::memory_order_acquire);

      if (state == EMPTY) {
        auto expected = EMPTY;
        if (slot.state.compare_exchange_strong(expected, CLAIMED, std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
          // We won this slot: `key` is a new group. Publish the key, then the ticket. No other thread can read `key`
          // until the release store below makes the biased ticket visible.
          slot.key = key;
          slot.state.store(ticket + TICKET_BIAS, std::memory_order_release);
          return ticket;
        }
        // We lost the race; `expected` now holds whatever the winner stored (CLAIMED or a published ticket). Interpret
        // it below instead of re-reading.
        state = expected;
      }

      if (state == CLAIMED) {
        // Another thread owns this slot but has not published its key. It might be inserting our key, in which case
        // advancing would create a duplicate, so we must wait for publication before comparing.
        while (state == CLAIMED) {
          state = slot.state.load(std::memory_order_acquire);
        }
      }

      // Published slot: compare the (now stable) key. A hit returns the existing ticket; a miss probes the next slot.
      if (slot.key == key) {
        return state - TICKET_BIAS;
      }
      index = (index + 1) & _mask;
    }
  }

  // Invokes `fn(key, ticket)` once per entry, in unspecified order. Call only after all inserting threads have joined.
  template <typename Fn>
  void for_each(Fn&& fn) const {
    for (auto index = size_t{0}; index < _capacity; ++index) {
      const auto state = _slots[index].state.load(std::memory_order_relaxed);
      if (state >= TICKET_BIAS) {
        fn(_slots[index].key, state - TICKET_BIAS);
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

  static_assert(std::is_trivially_destructible_v<Slot>,
                "Slots are freed via ::operator delete[] without per-element destruction.");
  static_assert(alignof(Slot) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__,
                "Slot is over-aligned; its raw storage would need an aligned operator new[].");

  // Frees the raw storage allocated with `::operator new[]`. Paired with raw allocation (not a `new Slot[]` array
  // expression), so there is no array cookie and no per-element destructor to run.
  struct SlotArrayDeleter {
    void operator()(Slot* slots) const noexcept {
      ::operator delete[](static_cast<void*>(slots));
    }
  };

  using SlotArray = std::unique_ptr<Slot[], SlotArrayDeleter>;

  // Reserved `state` values; real tickets are stored biased by `TICKET_BIAS` so that 0 can mean EMPTY. `PLACE_HOLDER`
  // is the largest ticket that still stays clear of `uint64_t` overflow once biased - larger values are rejected.
  static constexpr auto EMPTY = uint64_t{0};
  static constexpr auto CLAIMED = uint64_t{1};
  static constexpr auto TICKET_BIAS = uint64_t{2};
  static constexpr auto PLACE_HOLDER = std::numeric_limits<uint64_t>::max() - TICKET_BIAS;

  static constexpr auto MIN_CAPACITY = size_t{16};
  // Keep the table below this fill so linear probing stays short and an empty slot always exists (probing terminates).
  static constexpr auto MAX_LOAD_PERCENT = size_t{70};
  // Initialize the slot array in parallel once it reaches at least twice this many slots; smaller tables init inline.
  // Also the minimum slice per init job, so we never spawn a job for a trivial amount of zeroing.
  static constexpr auto PARALLEL_INIT_SLOTS = size_t{1} << 18;

  // Avalanches the user hash so the low bits used by `& _mask` are well distributed; `std::hash` for integers is
  // typically the identity, whose low bits alias badly under power-of-two masking. This is the fmix64 finalizer also
  // used by the byte-row path's MurmurHash tail.
  static uint64_t _mix(uint64_t hash) {
    hash ^= hash >> 33;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33;
    hash *= 0xc4ceb9fe1a85ec53ULL;
    hash ^= hash >> 33;
    return hash;
  }

  size_t _slot_index(const Key& key) const {
    return static_cast<size_t>(_mix(static_cast<uint64_t>(_hash(key)))) & _mask;
  }

  SlotArray _slots;
  size_t _capacity = 0;
  size_t _mask = 0;
  Hash _hash{};
};

}  // namespace hyrise
