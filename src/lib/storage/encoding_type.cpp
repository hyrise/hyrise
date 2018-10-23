#include "encoding_type.hpp"

#include <boost/hana/for_each.hpp>

#include "constant_mappings.hpp"

namespace opossum {

namespace hana = boost::hana;

bool encoding_supports_data_type(EncodingType encoding_type, DataType data_type) {
  bool result = false;

  hana::for_each(supported_data_types_for_encoding_type, [&](auto encoding_pair) {
    if (hana::first(encoding_pair).value == encoding_type) {
      hana::for_each(data_type_pairs, [&](auto data_type_pair) {
        if (hana::first(data_type_pair) == data_type) {
          result = hana::contains(hana::at_key(supported_data_types_for_encoding_type, hana::first(encoding_pair)),
                                  hana::second(data_type_pair));
          return;
        }
      });
      return;
    }
  });

  return result;
}

}  // namespace opossum
