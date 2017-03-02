#!lua

-- Determining exact tool names
md5Command = ""
if os.is("macosx") == true then
  md5Command = "md5 -q"
else
  md5Command = "md5sum"
end

-- Install pre-commit hook for linting if not installed yet or outdated
if os.execute("test -x .git/hooks/pre-commit") ~= 0 or os.execute(md5Command .. " .git/hooks/pre-commit | grep 8e8df7ddf91f5256604ecf2510958c91 >/dev/null 2>/dev/null") ~= 0 then
  os.execute("touch .git/hooks/pre-commit")
  os.execute("echo '#!/bin/bash\nif [ -f ./git/MERGE_HEAD ];\nthen\n    exit 0\nfi\n\nfunction finish {\n    git stash pop>/dev/null\n}\ngit stash --keep-index >/dev/null && trap finish EXIT\necho \"Linting all code, this may take a while...\"\n\nfind src -iname *.cpp -o -iname *.hpp | while read line;\ndo\n    if ! python2.7 cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright,-whitespace/newline,-runtime/references,-build/c++11 --linelength=120 $line >/dev/null 2>/dev/null\n    then\n        echo \"ERROR: Linting error occured. Execute \\\"premake4 lint\\\" for details!\"\n        exit 1\n    fi\ndone\n\nif [ $? != 0 ]\nthen\n    exit 1\nfi\n\necho \"Success, no linting errors found!\"\n\necho \"Testing the Opossum, grrrrr...\"\nmake -j test >/dev/null 2>/dev/null\nif ! ./build/test >/dev/null 2>/dev/null\nthen\n    echo \"ERROR: Testing error occured. Execute \\\"make test\\\" for details!\"\n    exit 1\nfi\n\necho \"Success, no testing errors found!\"' > .git/hooks/pre-commit")
  os.execute("chmod +x .git/hooks/pre-commit")
  os.execute("echo Successfully installed pre-commit hook.")
end

if os.execute("test -x .git/hooks/pre-push") ~= 0 or os.execute(md5Command .. " .git/hooks/pre-push | grep 1ab787b835edad24a8cac25e9a6d4925 >/dev/null 2>/dev/null") ~= 0 then
  os.execute("touch .git/hooks/pre-push")
  os.execute("echo \"#!/bin/bash\n\nprotected_branch='master'\ncurrent_branch=\\$(git symbolic-ref HEAD | sed -e 's,.*/\\(.*\\),\\1,')\n\nif [ \\$protected_branch = \\$current_branch ] && [ \\$2 = 'git@gitlab.hpi.de:OpossumDB/OpossumDB.git' ]\nthen\n    echo\n    echo 'You are about to push to master. Opossum style dictates that you create a merge request from a different branch.'\n    read -p 'Is pushing to master really what you intended? [y|n] ' -n 1 -r < /dev/tty\n    echo\n    if echo \\$REPLY | grep -E '^[Yy]\\$' > /dev/null\n    then\n        exit 0 # push will execute\n    fi\n    exit 1 # push will not execute\nelse\n    exit 0 # push will execute\nfi\" > .git/hooks/pre-push")
  os.execute("chmod +x .git/hooks/pre-push")
  os.execute("echo Successfully installed pre-push hook.")
end

-- Check for numa availability
numa_supported = os.findlib("numa") ~= nil

-- TODO try LTO/whole program

function default(osName, actionName)
  if osName ~= nil and os.is(osName) == false then
    return
  end

  if _ACTION == nil then
    _ACTION = actionName
  end
end

default("linux", "gmake")
default("macosx", "gmake")

if not _OPTIONS["compiler"] then
  print "No compiler specified. Automatically selected gcc."
  _OPTIONS["compiler"] = "gcc"
end

if _OPTIONS["compiler"] == "clang" then
  premake.gcc.cc  = 'clang++'
  premake.gcc.cxx = 'clang++'
else
  if os.execute("gcc-6 -v 2>/dev/null") == 0 then
    premake.gcc.cc  = 'gcc-6'
    premake.gcc.cxx = 'g++-6'
  else
    if os.execute("gcc --version 2>/dev/null | grep \" 6\.\" >/dev/null") == 0 then
      premake.gcc.cc  = 'gcc'
      premake.gcc.cxx = 'g++'
    else
      if _ACTION ~= "clean" then
        error("gcc version 6 required. Aborting.")
      end
    end
  end
end

solution "opossum"
  configurations { "Debug", "Release" }
  flags { "FatalWarnings", "ExtraWarnings" }
  language "C++"
  targetdir "build"
  buildoptions { "-std=c++1z -pthread -Wno-error=unused-parameter" }
  if os.is("linux") then
    linkoptions {"-pthread"}
  end
  links { "tbb" }
  includedirs { "src/lib/", "/usr/local/include" }

  if numa_supported then
    links { "numa" }
    defines { "OPOSSUM_NUMA_SUPPORT=1" }
  else
    defines { "OPOSSUM_NUMA_SUPPORT=0" }
  end

  configuration "Debug"
    defines { "IS_DEBUG=1" }
    flags { "Symbols" }
    prebuildcommands { "find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} sh -c \"clang-format -i -style=file '{}'\"" }
      -- TODO Shouldn't this be part of the pre-commit hook? "make" should never touch the code

  configuration "Release"
    defines { "IS_DEBUG=0" }
    flags { "OptimizeSpeed" }
    prebuildcommands { "find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} sh -c \"clang-format -i -style=file '{}'\"" }

project "googletest"
  kind "StaticLib"
  files { "third_party/googletest/googletest/src/gtest-all.cc" }
  includedirs { "third_party/googletest/googletest", "third_party/googletest/googletest/include" }

project "opossum"
  kind "StaticLib"
  files { "src/lib/**.hpp", "src/lib/**.cpp", "src/bin/server.cpp" }

project "opossum-asan"
  kind "StaticLib"
  buildoptions {"-fsanitize=address -fno-omit-frame-pointer"}
  linkoptions {"-fsanitize=address"}
  files { "src/lib/**.hpp", "src/lib/**.cpp", "src/bin/server.cpp" }

project "opossumCoverage"
  kind "StaticLib"
  buildoptions { "-fprofile-arcs -ftest-coverage" }
  linkoptions { "-lgcov --coverage" }
  files { "src/lib/**.hpp", "src/lib/**.cpp" }

project "server"
  kind "ConsoleApp"
  links { "opossum" }
  files { "src/bin/server.cpp" }

project "playground"
  kind "ConsoleApp"
  links { "opossum" }
  files { "src/bin/playground.cpp" }

project "test"
  kind "ConsoleApp"

  links { "opossum", "googletest" }
  files { "src/test/**.hpp", "src/test/**.cpp" }
  includedirs { "third_party/googletest/googletest/include" }
  postbuildcommands { "./build/test" }

project "asan"
  kind "ConsoleApp"

  links { "opossum-asan", "googletest" }
  files { "src/test/**.hpp", "src/test/**.cpp" }
  includedirs { "third_party/googletest/googletest/include" }
  buildoptions {"-fsanitize=address -fno-omit-frame-pointer"}
  linkoptions { "-fsanitize=address" }
  postbuildcommands { "./build/asan" }

project "coverage"
  kind "ConsoleApp"

  links { "opossumCoverage", "googletest" }
  linkoptions {"--coverage"}
  files { "src/test/**.hpp", "src/test/**.cpp" }
  buildoptions { "-fprofile-arcs -ftest-coverage" }
  includedirs { "third_party/googletest/googletest/include" }
  postbuildcommands { "./build/coverage && rm -fr coverage; mkdir coverage && gcovr -s -r . --exclude=\"(.*types*.|.*test*.)\" --html --html-details -o coverage/index.html" }

newoption {
  trigger     = "compiler",
  value       = "clang||gcc",
  description = "Choose a compiler",
  allowed = {
    { "gcc",    "gcc of version 6 or higher" },
    { "clang",  "clang llvm frontend" }
  }
}

-- Registering linting and formatting as actions for premake is not optimal, make targets would be the preferable option, but impossible to generate or really hacky

newaction {
  trigger     = "lint",
  description = "Lint the code",
  execute = function ()
    os.execute("find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} python2.7 cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright,-whitespace/newline,-runtime/references,-build/c++11 --linelength=120 {}")
      -- whitespace/newline is broken with lambda expressions and the way clang-format works
  end
}

newaction {
  trigger     = "format",
  description = "Format the code",
  execute = function ()
    os.execute("find src -iname \"*.cpp\" -o -iname \"*.hpp\" | xargs -I{} sh -c \"clang-format -i -style=file '{}'\"")
  end
}

premake.old_generate = premake.generate

function premake.generate(obj, filename, callback)
  premake.old_generate(obj, filename, callback)

  if filename == "Makefile" then
    -- make some changes to Makefile that premake4 does not support

    -- "make all" should only build opossum
    os.execute("sed -i''.bak 's/^all: .*\$/all: opossum server/' Makefile")
    os.execute("rm Makefile.bak")

    -- "make clean" should also call "premake4 clean"
    os.execute("awk '\\\
    /help:/ {\\\
    print \"\tpremake4 clean\"\\\
    }\\\
    { print }' Makefile > Makefile.awk && mv Makefile.awk Makefile")
  end
end
