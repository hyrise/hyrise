//
// Created by Lukas Böhme on 03.01.20.
//

#include <operators/abstract_operator.hpp>
#include <string>

namespace opossum {
class MeasurementSave {
 public:
  MeasurementSave(std::string path_to_dir): path_to_dir(path_to_dir);

  void export_to_csv(std::shared_ptr<const AbstractOperator> op) const;

 private:

    void export_specific_operator(std::shared_ptr<const AbstractOperator> op) const;
    const std::string path_to_dir;
};
}  // namespace opossum
