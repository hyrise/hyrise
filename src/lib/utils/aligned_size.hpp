namespace opossum {

// returns the actually used size by an object of type T
template <typename T>
constexpr size_t aligned_size() {
  return sizeof(T) + alignof(T) - 1 - (sizeof(T) - 1) % alignof(T);
}

}  // namespace opossum
