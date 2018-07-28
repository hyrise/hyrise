namespace opossum {

// returns the actually used size by an object of type T
template <typename T>
constexpr size_t aligned_size() {
  // next multiple of alignof(T), based on https://stackoverflow.com/a/4073700/2204581
  return sizeof(T) + alignof(T) - 1 - (sizeof(T) - 1) % alignof(T);
}

}  // namespace opossum
