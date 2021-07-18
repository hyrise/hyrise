#include <memory>
#include <optional>
#include <utility>

#include "abstract_statistics_object.hpp"
#include "all_type_variant.hpp"
#include "dips_min_max_filter.hpp"
#include "lossless_cast.hpp"
#include "min_max_filter.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
DipsMinMaxFilter<T>::DipsMinMaxFilter(T init_min, T init_max, CommitID init_commit_id)
    : MinMaxFilter<T>(init_min, init_max), commit_id(init_commit_id) {}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DipsMinMaxFilter);

}  // namespace opossum
