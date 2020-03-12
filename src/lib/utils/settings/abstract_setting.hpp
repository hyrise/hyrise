#pragma once

#include "types.hpp"

namespace opossum {

/**
 * This is an abstract class for all settings objects.
 * A setting is a configuration knob of any component with a variable value.
 * The component configured by the setting instantiates and destroys a setting.
 *
 * Settings need to be registered and deregistered in the SettingsManager.
 * If a component ends its lifecycle (e.g., a plugin in unloaded) and does not deregister its settings,
 * this might lead to undefined behavior.
 *
 * Settings provide a getter/setter that returns/requires a std::string.
 * When the set(...) method is called, a setting can invoke a method of its owning component
 * that triggers the immediate appliance of the changed value.
 *
 * Settings have a unique name that consists of the name of its parent and the setting
 * name (e.g. "TaskScheduler.workers")
 */
class AbstractSetting : public Noncopyable, public std::enable_shared_from_this<AbstractSetting> {
 public:
  explicit AbstractSetting(const std::string& init_name);

  virtual ~AbstractSetting() = default;

  virtual const std::string& description() const = 0;

  virtual const std::string& get() = 0;

  virtual void set(const std::string& value) = 0;

  virtual void register_at_settings_manager();

  virtual void unregister_at_settings_manager();

  const std::string name;
};

}  // namespace opossum
