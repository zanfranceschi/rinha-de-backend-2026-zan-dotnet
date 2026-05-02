#!/bin/bash
BIN="${1:-resources/references.bin}"

read_int32() {
  od -An -td4 -N4 --endian=little | tr -d ' '
}

count=$(dd if="$BIN" bs=4 count=1 status=none | read_int32)
echo "count = $count"

vec_skip=$((4 + count * 16 * 2 + count))
nClusters=$(dd if="$BIN" bs=1 skip=$vec_skip count=4 status=none | read_int32)
echo "nClusters = $nClusters"

members_offset=$((vec_skip + 4 + nClusters * 16 * 2 + nClusters * 4 + nClusters * 4))
totalMembers=$(dd if="$BIN" bs=1 skip=$members_offset count=4 status=none | read_int32)
echo "totalMembers = $totalMembers"

expected=$((members_offset + 4 + totalMembers * 4))
actual=$(stat -c%s "$BIN")
echo "expected size = $expected"
echo "actual size = $actual"
[ "$expected" -eq "$actual" ] && echo "OK" || echo "ERRO"
