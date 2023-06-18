STATE_MASK = 0xFF00000000000000
NODE_MASK = 0x000F000000000000
DIRTY_MASK = 0x00F0000000000000
VERSION_MASK = 0x0000FFFFFFFFFFFF

NODE_SHIFT = 48
DIRTY_SHIFT = 52
STATE_SHIFT = 56


def state_string(state):
    if state == 0:
        return "UNLOCKED"
    if state <= 252:
        return "LOCKED SHARED ({})".format(state)
    if state == 253:
        return "LOCKED EXCLUSIVE"
    if state == 254:
        return "MARKED"
    if state == 255:
        return "EVICTED"


def FrameSummary(valobj, internal_dict, *args, **options):
    value = valobj.GetValueAsUnsigned()
    version = value & VERSION_MASK
    memory_node = (value & NODE_MASK) >> NODE_SHIFT
    dirty = bool((value & DIRTY_MASK) >> DIRTY_SHIFT)
    state_value = (value & STATE_MASK) >> STATE_SHIFT
    state = state_string(state_value)
    return "State: {}, Dirty: {}, MemoryNode: {}, Version: {}".format(state, dirty, memory_node, version)


def PageIDSummary(valobj, internal_dict, *args, **options):
    valid = bool(valobj.GetChildMemberWithName("_valid").GetValueAsUnsigned())
    size_type_idx = valobj.GetChildMemberWithName(
        "_size_type").GetValueAsUnsigned()
    size_type = "{}KiB".format(1 << (3 + size_type_idx))
    idx = valobj.GetChildMemberWithName("index").GetValueAsUnsigned()
    return "Valid: {}, SizeType: {}, Index: {}".format(valid, size_type, idx)


def __lldb_init_module(debugger, dictionary):
    debugger.HandleCommand("type summary add -F " +
                           __name__ + ".FrameSummary hyrise::Frame")
    debugger.HandleCommand("type summary add -F " +
                           __name__ + ".PageIDSummary hyrise::PageID")
