#pragma once

#include "hyrise.hpp"
#include "utils/abstract_plugin.hpp"

namespace hyrise {

class BufferManagerPlugin : public AbstractPlugin {
 public:
  BufferManagerPlugin() : buffer_manager(Hyrise::get().buffer_manager) {}

  std::string description() const final;

  void start() final;

  void stop() final;

  class BufferManagerSetting : public AbstractSetting {
   protected:
    BufferManagerSetting(BufferManager& buffer_manager, const std::string& name);

    BufferManager& buffer_manager;
  };

  class BufferManagerPoolSizeSetting : public BufferManagerSetting {
   public:
    BufferManagerPoolSizeSetting();

    const std::string& description() const;

    const std::string& get();

    void set(const std::string& value);

   private:
    std::string pool_size;
  };

  class BufferManagerSSDPathSetting : public BufferManagerSetting {
   public:
    BufferManagerSSDPathSetting();

    const std::string& description() const;

    const std::string& get();

    void set(const std::string& value);

   private:
    std::string ssd_path;
  };

 private:
  BufferManager& buffer_manager;

  std::shared_ptr<BufferManagerPoolSizeSetting> buffer_manager_pool_size_setting;
  std::shared_ptr<BufferManagerSSDPathSetting> buffer_manager_ssd_path_setting;
};

}  // namespace hyrise
