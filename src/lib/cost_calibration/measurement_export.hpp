//
// Created by Lukas Böhme on 03.01.20.
//

#include <operators/abstract_operator.hpp>
#include <string>

namespace opossum {
class MeasurementExport {
 public:
  MeasurementExport(std::string path_to_dir);

  void export_to_csv(std::shared_ptr<const AbstractOperator> op) const;

 private:
    const std::string path_to_dir;
    void export_typed_operator(std::shared_ptr<const AbstractOperator> op) const;

    std::string get_path_by_type(OperatorType operator_type);
};
}  // namespace opossum
