#pragma once

#include <sys/sdt.h>

// Systemtap is not available on OS X. The alternative would be dtrace. Actually, systemtap probes are compatible
// with dtrace probes and the other way round. However, Apple's dtrace implementation differs from the original solaris
// implementation. Thus, Apple's dtrace does not understand the USDT probe definition. In order to be able to compile
// the project on Mac we define the probes in this file.

// In order to be compatible with Mac dtrace probes, both provider and probe name have to be uppercase
constexpr bool is_valid_name(const char* name) {
  do {
    if (*name >= 'a' && *name <= 'z') return false;
    if (*name == '-') return false;
  } while (*++name != '\0');
  return true;
}

#if defined(__APPLE__) || defined(__MACOS__)

#include "provider.hpp"

// Construct the probe definition by provider and probe name. Because TSan has issues and throw false positives, we
// don't use probes in TSan builds
#if defined(__has_feature)
#if !__has_feature(thread_sanitizer)
#define BUILD_PROBE_NAME(provider, probe, ...)                                                                     \
  static_assert(is_valid_name(#provider) && is_valid_name(#probe), "Provider and probe name must be upper case!"); \
  provider##_##probe(__VA_ARGS__);
#else
#define BUILD_PROBE_NAME(provider, probe, ...)
#endif
#endif

#define DTRACE_PROBE(provider, probe) BUILD_PROBE_NAME(provider, probe);
#define DTRACE_PROBE1(provider, probe, param1) BUILD_PROBE_NAME(provider, probe, param1);
#define DTRACE_PROBE2(provider, probe, param1, param2) BUILD_PROBE_NAME(provider, probe, param1, param2);
#define DTRACE_PROBE3(provider, probe, param1, param2, param3) BUILD_PROBE_NAME(provider, probe, param1, param2, param3)
#define DTRACE_PROBE4(provider, probe, param1, param2, param3, param4) \
  BUILD_PROBE_NAME(provider, probe, param1, param2, param3, param4);
#define DTRACE_PROBE5(provider, probe, param1, param2, param3, param4, param5) \
  BUILD_PROBE_NAME(provider, probe, param1, param2, param3, param4, param5);
#define DTRACE_PROBE6(provider, probe, param1, param2, param3, param4, param5, param6) \
  BUILD_PROBE_NAME(provider, probe, param1, param2, param3, param4, param5, param6);
#define DTRACE_PROBE7(provider, probe, param1, param2, param3, param4, param5, param6, param7) \
  BUILD_PROBE_NAME(provider, probe, param1, param2, param3, param4, param5, param6, param7);
#define DTRACE_PROBE8(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8) \
  BUILD_PROBE_NAME(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8);
#define DTRACE_PROBE9(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8, param9) \
  BUILD_PROBE_NAME(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8, param9);
#define DTRACE_PROBE10(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8, param9, \
                       param10)                                                                                 \
  BUILD_PROBE_NAME(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10);
#define DTRACE_PROBE11(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8, param9,      \
                       param10, param11)                                                                             \
  BUILD_PROBE_NAME(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10, \
                   param11);
#define DTRACE_PROBE12(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8, param9,      \
                       param10, param11, param12)                                                                    \
  BUILD_PROBE_NAME(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10, \
                   param11, param12);

// If not MACOS
#else

// Because TSan has issues and throw false positives, we don't use probes in TSan builds
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)

// We are redefining a macro here, we have to turn of the warning. Clang picks up that pragma as well
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmacro-redefined"

#define DTRACE_PROBE(provider, probe)
#define DTRACE_PROBE1(provider, probe, param1)
#define DTRACE_PROBE2(provider, probe, param1, param2)
#define DTRACE_PROBE3(provider, probe, param1, param2, param3)
#define DTRACE_PROBE4(provider, probe, param1, param2, param3, param4)
#define DTRACE_PROBE5(provider, probe, param1, param2, param3, param4, param5)
#define DTRACE_PROBE6(provider, probe, param1, param2, param3, param4, param5, param6)
#define DTRACE_PROBE7(provider, probe, param1, param2, param3, param4, param5, param6, param7)
#define DTRACE_PROBE8(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8)
#define DTRACE_PROBE9(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8, param9)
#define DTRACE_PROBE10(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10)
#define DTRACE_PROBE11(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8, param9, \
                       param10, param11)
#define DTRACE_PROBE12(provider, probe, param1, param2, param3, param4, param5, param6, param7, param8, param9, \
                       param10, param11, param12)

#pragma GCC diagnostic pop

#endif  // defined(__has_feature)
#endif  // __has_feature(thread_sanitizer)
#endif  // else-branch of defined(__APPLE__) || defined(__MACOS__)
