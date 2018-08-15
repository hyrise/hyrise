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

#include "utils/tracing/probe_definitions.h"
#include "utils/tracing/provider.h"

// Construct the probe definition by provider and probe name
#define _build_name(provider, probe, ...)                                                                          \
  static_assert(is_valid_name(#provider) && is_valid_name(#probe), "Provider and probe name must be upper case!"); \
  provider##_##probe(__VA_ARGS__);

#define DTRACE_PROBE(provider, probe) _build_name(provider, probe);
#define DTRACE_PROBE1(provider, probe, parm1) _build_name(provider, probe, parm1);
#define DTRACE_PROBE2(provider, probe, parm1, parm2) _build_name(provider, probe, parm1, parm2);
#define DTRACE_PROBE3(provider, probe, parm1, parm2, parm3) _build_name(provider, probe, parm1, parm2, parm3)
#define DTRACE_PROBE4(provider, probe, parm1, parm2, parm3, parm4) \
  _build_name(provider, probe, parm1, parm2, parm3, parm4);
#define DTRACE_PROBE5(provider, probe, parm1, parm2, parm3, parm4, parm5) \
  _build_name(provider, probe, parm1, parm2, parm3, parm4, parm5);
#define DTRACE_PROBE6(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6) \
  _build_name(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6);
#define DTRACE_PROBE7(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6, parm7) \
  _build_name(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6, parm7);
#define DTRACE_PROBE8(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6, parm7, parm8) \
  _build_name(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6, parm7, parm8);
#define DTRACE_PROBE9(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6, parm7, parm8, parm9) \
  _build_name(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6, parm7, parm8, parm9);
#define DTRACE_PROBE10(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6, parm7, parm8, parm9, parm10) \
  _build_name(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6, parm7, parm8, parm9, parm10);
#define DTRACE_PROBE11(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6, parm7, parm8, parm9, parm10, parm11) \
  _build_name(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6, parm7, parm8, parm9, parm10, parm11);
#define DTRACE_PROBE12(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6, parm7, parm8, parm9, parm10, parm11, \
                       parm12)                                                                                         \
  _build_name(provider, probe, parm1, parm2, parm3, parm4, parm5, parm6, parm7, parm8, parm9, parm10, parm11, parm12);
#endif
