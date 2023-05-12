#include <memory>
#include <vector>

#include <boost/container/vector.hpp>
#include "benchmark/benchmark.h"
#include "storage/buffer/utils.hpp"
#include "utils.hpp"

namespace hyrise {

struct PointerObserver {
  virtual void observer(void* ptr) = 0;
  virtual void unregister(void* ptr) = 0;
};

template <int GetPointer>
struct TestPointer {
  using PointedType = int;
  using pointer = PointedType*;
  using reference = typename add_reference<PointedType>::type;
  using element_type = PointedType;
  using value_type = std::remove_cv_t<PointedType>;
  using difference_type = std::ptrdiff_t;
  using iterator_category = std::random_access_iterator_tag;

  explicit TestPointer(FramePtr frame, std::uintptr_t offset, PointerObserver* observer = nullptr)
      : frame(frame), ptr_or_offset(offset), observer(observer) {
    if constexpr (GetPointer == 4) {
      observer->observer(*this);
    }
  }

  ~TestPointer() {
    if constexpr (GetPointer == 4) {
      observer->unregister(*this);
    }
  }

  TestPointer& operator++(void) noexcept {
    ptr_or_offset += sizeof(value_type);
    return *this;
  }

  bool operator!=(const TestPointer& other) const noexcept {
    return get() != other.get();
  }

  bool operator==(const TestPointer& other) const noexcept {
    return get() == other.get();
  }

  reference operator*() const {
    pointer p = get();
    if (!p) {
      Fail("Dereferencing null pointer");
    }
    reference r = *p;
    return r;
  }

  TestPointer& operator+=(difference_type offset) noexcept {
    ptr_or_offset += offset * difference_type(sizeof(PointedType));
    return *this;
  }

  TestPointer& operator--(void) noexcept {
    ptr_or_offset -= difference_type(sizeof(PointedType));
    return *this;
  }

  pointer get() const {
    if constexpr (GetPointer == 0) {
      return static_cast<pointer>(get_pointer_simple());
    } else if constexpr (GetPointer == 1) {
      return static_cast<pointer>(get_pointer_likely());
    } else if constexpr (GetPointer == 2) {
      return static_cast<pointer>(get_pointer_dummy__branchless());
    } else if constexpr (GetPointer == 3 || GetPointer == 4) {
      return static_cast<pointer>(get_raw_pointer());
    } else {
      Fail("Invalid GetPointer");
    }
  }

  void* get_pointer_simple() const {
    if (frame) {
      if (frame->data) {
        return frame->data + ptr_or_offset;
      } else {
        return nullptr;
      }
    } else {
      return reinterpret_cast<void*>(ptr_or_offset);
    }
  }

  void* get_pointer_dummy__branchless() const {
    uintptr_t pointer = ptr_or_offset;
    uintptr_t offset = (uintptr_t)frame->data ? pointer : 0;
    return reinterpret_cast<void*>((uintptr_t)frame->data + offset);
  }

  void* get_pointer_likely() const {
    if (frame) {
      if (frame->data) {
        [[likely]] return frame->data + ptr_or_offset;
      } else {
        return nullptr;
      }
    } else {
      return reinterpret_cast<void*>(ptr_or_offset);
    }
  }

  void* get_raw_pointer() const {
    return reinterpret_cast<void*>(ptr_or_offset);
  }

  friend bool operator<(const TestPointer& ptr1, const TestPointer& ptr2) noexcept {
    return ptr1.get() < ptr2.get();
  }

  friend bool operator>(const TestPointer& ptr1, const TestPointer& ptr2) noexcept {
    return ptr1.get() > ptr2.get();
  }

  friend TestPointer operator+(TestPointer left, difference_type diff) noexcept {
    left += diff;
    return left;
  }

  FramePtr frame;
  std::uintptr_t ptr_or_offset;
  PointerObserver* observer;
};

template <int T>
inline std::ptrdiff_t operator-(const TestPointer<T>& ptr1, const TestPointer<T>& ptr2) {
  return ptr1.get() - ptr2.get();
}

template <int T>
inline bool operator>=(const TestPointer<T>& ptr1, const TestPointer<T>& ptr2) {
  return ptr1.get() >= ptr2.get();
}

struct PointerObserverImpl : PointerObserver {
  void observer(TestPointer<4>* p) {
    p->ptr_or_offset = (std::uintptr_t)p->frame->data + p->ptr_or_offset;
  }

  void unregister(TestPointer<4>* p) {}
};

static void BM_pointer_sort_simple_frame(benchmark::State& state) {
  alignas(512) std::array<typename TestPointer<0>::value_type, 5000> array;
  auto dummy = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram, reinterpret_cast<std::byte*>(array.data()));

  std::iota(array.begin(), array.end(), 1);

  for (auto _ : state) {
    state.PauseTiming();
    std::reverse(array.begin(), array.end());
    auto begin = TestPointer<0>(dummy, 0);
    auto end = TestPointer<0>(dummy, array.size());
    state.ResumeTiming();
    std::sort(begin, end);
  }
}

static void BM_pointer_sort_simple_raw(benchmark::State& state) {
  alignas(512) std::array<typename TestPointer<0>::value_type, 5000> array;
  auto dummy = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram, nullptr);

  std::iota(array.begin(), array.end(), 1);

  for (auto _ : state) {
    state.PauseTiming();
    std::reverse(array.begin(), array.end());
    auto begin = TestPointer<0>(dummy, reinterpret_cast<std::uintptr_t>(array.data()));
    auto end = TestPointer<0>(dummy, reinterpret_cast<std::uintptr_t>(array.data() + array.size()));
    state.ResumeTiming();
    std::sort(begin, end);
  }
}

static void BM_pointer_sort_simple_likley_frame(benchmark::State& state) {
  alignas(512) std::array<typename TestPointer<1>::value_type, 5000> array;
  auto dummy = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram, reinterpret_cast<std::byte*>(array.data()));

  std::iota(array.begin(), array.end(), 1);

  for (auto _ : state) {
    state.PauseTiming();
    std::reverse(array.begin(), array.end());
    auto begin = TestPointer<1>(dummy, 0);
    auto end = TestPointer<1>(dummy, array.size());
    state.ResumeTiming();
    std::sort(begin, end);
  }
}

static void BM_pointer_sort_simple_likley_raw(benchmark::State& state) {
  alignas(512) std::array<typename TestPointer<1>::value_type, 5000> array;
  auto dummy = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram, nullptr);

  std::iota(array.begin(), array.end(), 1);

  for (auto _ : state) {
    state.PauseTiming();
    std::reverse(array.begin(), array.end());
    auto begin = TestPointer<1>(dummy, reinterpret_cast<std::uintptr_t>(array.data()));
    auto end = TestPointer<1>(dummy, reinterpret_cast<std::uintptr_t>(array.data() + array.size()));
    state.ResumeTiming();
    std::sort(begin, end);
  }
}

static void BM_pointer_sort_branchless_frame(benchmark::State& state) {
  alignas(512) std::array<typename TestPointer<2>::value_type, 5000> array;

  auto dummy = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram, reinterpret_cast<std::byte*>(array.data()));

  std::iota(array.begin(), array.end(), 1);

  for (auto _ : state) {
    state.PauseTiming();
    std::reverse(array.begin(), array.end());
    auto begin = TestPointer<2>(dummy, 0);
    auto end = TestPointer<2>(dummy, array.size());
    state.ResumeTiming();
    std::sort(begin, end);
  }
}

static void BM_pointer_sort_branchless_raw(benchmark::State& state) {
  alignas(512) std::array<typename TestPointer<2>::value_type, 5000> array;

  auto dummy = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram, nullptr);

  std::iota(array.begin(), array.end(), 1);

  for (auto _ : state) {
    state.PauseTiming();
    std::reverse(array.begin(), array.end());
    auto begin = TestPointer<2>(dummy, reinterpret_cast<std::uintptr_t>(array.data()));
    auto end = TestPointer<2>(dummy, reinterpret_cast<std::uintptr_t>(array.data() + array.size()));
    state.ResumeTiming();
    std::sort(begin, end);
  }
}

static void BM_pointer_sort_raw(benchmark::State& state) {
  alignas(512) std::array<typename TestPointer<3>::value_type, 5000> array;

  std::iota(array.begin(), array.end(), 1);

  for (auto _ : state) {
    state.PauseTiming();
    std::reverse(array.begin(), array.end());
    auto begin = TestPointer<3>(nullptr, reinterpret_cast<std::uintptr_t>(array.data()));
    auto end = TestPointer<3>(nullptr, reinterpret_cast<std::uintptr_t>(array.data() + array.size()));
    state.ResumeTiming();

    std::sort(begin, end);
  }
}

BENCHMARK(BM_pointer_sort_simple_frame);
BENCHMARK(BM_pointer_sort_simple_raw);
BENCHMARK(BM_pointer_sort_branchless_frame);
BENCHMARK(BM_pointer_sort_branchless_raw);
BENCHMARK(BM_pointer_sort_raw);
BENCHMARK(BM_pointer_sort_simple_likley_frame);
BENCHMARK(BM_pointer_sort_simple_likley_raw);

// Benchmark random vs sequential access e.g vector vs tree with set or linked list
// Benchmark different sizes of the vector
// Benchmark selectivity of the vector

}  // namespace hyrise