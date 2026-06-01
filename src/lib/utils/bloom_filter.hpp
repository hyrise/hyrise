// Simple BloomFilter implementation with a fixed size of 1 MiB. A single filter does not support concurrent accesses
// (i.e., writing and reading should only be done from a single thread). However, multiple filters can concurrently
// merged into another. and Merging can be done in parallel as the filter uses atomic integers for storing its data.
class BloomFilter {
 public:
  static constexpr auto VECTOR_SIZE = size_t{2 << (20 - 6)};

  BloomFilter() : _filter(VECTOR_SIZE), _non_atomic_view{reinterpret_cast<uint64_t*>(_filter.data())} {
    static_assert(decltype(_filter)::value_type::is_always_lock_free);
  }

  BloomFilter(bool jo) : _filter(VECTOR_SIZE), _non_atomic_view{reinterpret_cast<uint64_t*>(_filter.data())} {
    set_all_true();
  }

  void add(size_t hash) {
    constexpr auto MASK = size_t{(2 << 14) - 1};
    const auto bit_in_integer = hash & size_t{63};
    const auto integer_offset = (hash >> size_t{6}) & MASK;
    _non_atomic_view[integer_offset] |= (size_t{1} << bit_in_integer);
  }

  bool does_not_contain(size_t hash) const {
    constexpr auto MASK = size_t{(2 << 14) - 1};
    const auto bit_in_integer = hash & size_t{63};
    const auto integer_offset = (hash >> size_t{6}) & MASK;
    return !(_non_atomic_view[integer_offset] & (size_t{1} << bit_in_integer));
  }

  void merge(const BloomFilter& other_filter) {
    const auto forward_iteration = (std::hash<std::thread::id>{}(std::this_thread::get_id()) % 2) == 0;
    const auto* other_filter_view = other_filter.non_atomic_view();

    if (forward_iteration) {
      for (auto index = size_t{0}; index < VECTOR_SIZE; ++index) {
        _filter[index] |= other_filter_view[index];
      }
    } else {
      for (auto index = int64_t{VECTOR_SIZE - 1}; index >= 0; --index) {
        _filter[index] |= other_filter_view[index];
      }
    }
  }

  uint64_t* non_atomic_view() const {
    return _non_atomic_view;
  }

  void set_all_true() {
    for (auto index = uint32_t{0}; index < VECTOR_SIZE; ++index) {
      _non_atomic_view[index] = std::numeric_limits<uint64_t>::max();
      Assert(_filter[index] == std::numeric_limits<uint64_t>::max(), "NARF");
    }
  }

  // Keep interface for now.
  bool empty() const {
    return true;
  }

  void resize(size_t /*size*/) {}

  size_t size() const {
    return VECTOR_SIZE;
  }

 private:
  std::vector<std::atomic<uint64_t>> _filter;
  uint64_t* _non_atomic_view;
};
