#include "operators/aggregate_dyod/scatter_store.hpp"

#include <cstddef>

#if defined(__SSE2__) || defined(_M_X64) || (defined(_M_IX86_FP) && _M_IX86_FP >= 2)
  #include <immintrin.h>
  #define REGION_STREAM_SSE2 1
#else
  #define REGION_STREAM_SSE2 0
#endif

namespace {
  void copy_line(std::byte* destination, const std::byte* source) {
#if REGION_STREAM_SSE2
    static_assert(hyrise::SWWC_LINE_BYTES % 16 == 0, "line must be a whole number of 128-bit stores");

    for (auto offset = size_t{0}; offset < hyrise::SWWC_LINE_BYTES; offset += 16) {
      __m128i vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(source + offset));
      _mm_stream_si128(reinterpret_cast<__m128i*>(destination + offset), vec);
    }
#else
    std::memcpy(destination, source, hyrise::SWWC_LINE_BYTES);
#endif
  }

  constexpr size_t REGION_INITIAL_LINES = 16;
  constexpr size_t REGION_INITIAL_CAPACITY = REGION_INITIAL_LINES * hyrise::SWWC_LINE_BYTES;

  constexpr size_t round_up_to_lines(const size_t n) noexcept {
    return (n + hyrise::SWWC_LINE_BYTES - 1) / hyrise::SWWC_LINE_BYTES * hyrise::SWWC_LINE_BYTES;
  }
}

namespace hyrise {

void Region::grow() {
  const auto required = size_t{_size + SWWC_LINE_BYTES};
  const auto doubled = size_t{_capacity * 2};
  const auto new_capacity = size_t{round_up_to_lines(std::max({required, doubled, REGION_INITIAL_CAPACITY}))};

  auto* block =  std::aligned_alloc(64, new_capacity);
  if (!block) {
    Fail("Allocation failed");
  }

  auto* new_data = static_cast<std::byte*>(block);

  if (_size > 0) {
    std::memcpy(new_data, _data.get(), _size);
  }

  _data.reset(new_data);
  _capacity = new_capacity;
}

void Region::push_line(const std::byte* line) {
  if (_size + SWWC_LINE_BYTES > _capacity) {
    grow();
  }

  std::byte* destination = _data.get() + _size;
  copy_line(destination, line);
  _size += SWWC_LINE_BYTES;
}

void Region::drain_partial(const std::byte* bytes, size_t length) {
  Assert(length < SWWC_LINE_BYTES, "A full line must use push_line()");
  Assert(_size % SWWC_LINE_BYTES == 0,
         "_size has to be line aligned before drain_partial is called");

  if (length == 0) {
    return;
  }
  if (_size + length > _capacity) {
    grow();
  }

  std::memcpy(_data.get() + _size, bytes, length);
  _size += length;
}

size_t Region::size() const {
  return _size;
}

const std::byte* Region::data() const {
  return _data.get();
}

void Region::clear() {
  _size = 0;
}

}  // namespace hyrise
