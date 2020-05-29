#!/bin/bash -x

if [ $# -gt 0 ];
then
BASE_PATH=$1
else
BASE_PATH='./cmake-build-release'
fi

MEASUREMENT_FOLDER='data/test'
EXECUTABLE='hyriseCalibration'
PYTHON=python3

touch $BASE_PATH/$EXECUTABLE

#rm -rf $BASE_PATH/$MEASUREMENT_FOLDER
#mkdir $BASE_PATH/$MEASUREMENT_FOLDER

# to train and predict on the same dataset
$PYTHON ./python/cost_models.py -train $BASE_PATH/$MEASUREMENT_FOLDER

# provide a specific dataset to test (e.g. benchmarks)
# $PYTHON ./python/cost_models.py -train $BASE_PATH/$MEASUREMENT_FOLDER --test $PATH_TO_MEASUREMENT --out $PATH_TO_OUTPUT_FOLDER
