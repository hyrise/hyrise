#!/bin/bash -x

BASE_PATH='./cmake-build-debug'
MEASUREMENT_FOLDER='measurements'
EXECUTABLE='hyrisePlayground'
PYTHON=python3

touch $BASE_PATH/$EXECUTABLE

rm -rf $BASE_PATH/$MEASUREMENT_FOLDER
mkdir $BASE_PATH/$MEASUREMENT_FOLDER

$PYTHON ./python/cost_models.py $BASE_PATH/$MEASUREMENT_FOLDER
