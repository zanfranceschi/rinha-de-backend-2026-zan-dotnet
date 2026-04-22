#!/bin/bash

RESOURCES_PATH="../../resources"

# No clustering — convert all 100k references to binary
# dotnet run -- $RESOURCES_PATH

# 5000 medoids
dotnet run -- $RESOURCES_PATH 2000

# 10000 medoids
# dotnet run -- $RESOURCES_PATH 10000

# 3000 medoids, 30 max iterations
# dotnet run -- $RESOURCES_PATH 3000 30
