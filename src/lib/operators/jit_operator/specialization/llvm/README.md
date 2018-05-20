# Overview
This directory contains two files (CloneFunction.cpp and InlineFunction.cpp) that have been copied from the LLVM repository located at `https://github.com/llvm-mirror/llvm/tree/release_60`.
The code specialization requires modifications to these files, which are detailed below.

The changes made to these files are kept to a minimum. Most of the code extending LLVM can be found in and `../llvm_extensions.hpp` and `../llvm_extensions.cpp`.
The files are also excluded from automatic formatting and linting to keep them as close to the original source code as possible.

## Necessary Modifications to LLVM
The code specialization uses cloning and inlining functionality from LLVM, but requires two small modifications to the implementation provided by LLVM:

1. When cloning functions, LLVM maintains a map of corresponding values in the old and new function. This map is used to substitute instruction operands during cloning. When cloning functions across module boundaries this map needs to be initialized with all global objects to ensure references to such objects in the cloned function refer to the correct module. While the `CloneFunction` API accepts an initialized method as a parameter, the `InlineFunction` (which uses `CloneFunction` internally) does not expose this parameter. Our modified implementation adds this parameter and passes it through to the `CloneFunction` function.
2. LLVM supports pruning of basic blocks during function cloning. The conditions of switch and conditional branch instructions are evaluated. In case of a constant conditions, the branch target can be pre-computed and the branch instruction be eliminated. However, LLVM only performs a static analysis of the LLVM bitcode. We introduce a callback that allows extending the pruning mechanism to also consider runtime information.

## Porting to a new LLVM version

When porting to a new LLVM version, these files need to pe replaced by the corresponding files in the new LLVM release and the same (or equivalent) changes need to be applied manually. To make this easier this directory contains a diff for each file that document the necessary changes.
