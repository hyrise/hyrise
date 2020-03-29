#!/bin/bash -x

BASE_PATH='./cmake-build-debug'
MEASUREMENT_FOLDER='measurements'
EXECUTABLE='hyrisePlayground'
PYTHON=python3

touch $BASE_PATH/$EXECUTABLE

rm -rf $BASE_PATH/$MEASUREMENT_FOLDER
mkdir $BASE_PATH/$MEASUREMENT_FOLDER

# to train and predict on the same dataset
$PYTHON ./python/cost_models.py -train $BASE_PATH/$MEASUREMENT_FOLDER

# provide a specific dataset to test (e.g. benchmarks)
# $PYTHON ./python/cost_models.py -train $BASE_PATH/$MEASUREMENT_FOLDER --test $PATH_TO_MEASUREMENT
