namespace opossum {

template <typename T>
std::shared_ptr<SingleBinHistogram<T>> AbstractStatisticsObject::reduce_to_single_bin_histogram() const {
  const auto single_bin_histogram = _reduce_to_single_bin_histogram_impl();
  if (!single_bin_histogram) return nullptr;
  return std::static_pointer_cast<SingleBinHistogram<T>>(single_bin_histogram);
}

}  // namespace opossum
