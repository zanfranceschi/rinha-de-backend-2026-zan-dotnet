#!/bin/bash

cd ./tools/Preprocessor
sh run.sh

cd ../../containerization
sh build-and-publish.sh
