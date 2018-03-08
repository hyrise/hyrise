#!/bin/bash
set -e

while [ $# -gt 0 ]; do
  case "$1" in
    --build_directory=*)
      build_directory="${1#*=}"
      ;;
    --generate_badge=*)
      generate_badge="${1#*=}"
      ;;
    --test_data_folder=*)
      test_data_folder="${1#*=}"
      ;;
    *)
      printf "Error: Invalid argument."
      exit 1
  esac
  shift
done

if [ -z "$build_directory" ]
  then
    echo "Error: No build directory supplied - use --build_directory=..."
    exit 1
fi

cd $build_directory
cores=$(grep -c ^processor /proc/cpuinfo 2>/dev/null || sysctl -n hw.ncpu)
make hyriseTest -j $((cores / 2))
cd -

./$build_directory/hyriseTest $test_data_folder
rm -fr coverage; mkdir coverage
# call gcovr twice b/c of https://github.com/gcovr/gcovr/issues/112
gcovr -r `pwd` --gcov-executable="gcov -s `pwd` -x" -s -p --exclude='.*/(?:third_party|src/test|src/benchmark).*' --exclude-unreachable-branches -k

# generate HTML
gcovr -r `pwd` --gcov-executable="gcov -s `pwd` -x" -s -p --exclude='.*/(?:third_party|src/test|src/benchmark).*' --exclude-unreachable-branches -k -g --html --html-details -o coverage/index.html > coverage_output.txt
cat coverage_output.txt

if [ "true" == "$generate_badge" ]
then
    # generate XML for pycobertura
    gcovr -r `pwd` --gcov-executable="gcov -s `pwd` -x" -p --exclude='.*/(?:third_party|src/test|src/benchmark).*' --exclude-unreachable-branches -g --xml > coverage.xml
    curl -g -o coverage_master.xml https://ares.epic.hpi.uni-potsdam.de/jenkins/job/Hyrise/job/hyrise/job/master/lastStableBuild/artifact/coverage.xml
    pycobertura diff coverage_master.xml coverage.xml --format html --output coverage_diff.html || true

    # coverage badge generation
    coverage_percent=$(cat coverage_output.txt | grep lines: | sed -e 's/lines: //; s/% .*$//')
    echo $coverage_percent > coverage_percent.txt
    if (( $(bc <<< "$coverage_percent >= 90") ))
    then
        color="brightgreen"
    elif (( $(bc <<< "$coverage_percent >= 75") ))
    then
        color="yellow"
    elif (( $(bc <<< "$coverage_percent < 75") ))
    then
        color="red"
    fi

    url="https://img.shields.io/badge/Coverage-$coverage_percent%25-$color.svg"
    curl -g -o coverage_badge.svg $url
fi

rm coverage_output.txt
