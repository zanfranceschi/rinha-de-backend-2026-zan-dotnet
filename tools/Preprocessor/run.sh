#!/bin/bash


# 0 — resourcesPath (default: ../../resources relativo)
# 1 — nClusters do IVF (default 2048)
# 2 — trainSample, tamanho do sample pro KMeans (default 131072)
# 3 — trainIters, iterações do KMeans (default 10)
# dotnet run -- "../../resources" 2048 131072 10
dotnet run -- "../../resources" 2048 0 20
