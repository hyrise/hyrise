#pragma once

#include "utils/settings/abstract_setting.hpp"

namespace hyrise {

class StatisticsExporter {
 public:
  StatisticsExporter();

  ~StatisticsExporter();

  void export_statistics(const std::string& output_directory);

 protected:
  class SubDirectory : public AbstractSetting {
   public:
    static inline const std::string DEFAULT_SUB_DIRECTORY{"/statistics"};
    explicit SubDirectory(const std::string& init_name);

    const std::string& description() const final;

    const std::string& get() final;

    void set(const std::string& value) final;

   private:
    std::string _value = DEFAULT_SUB_DIRECTORY;
  };

  std::shared_ptr<SubDirectory> _sub_directory;
};

}  // namespace hyrise
