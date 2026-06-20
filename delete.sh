#!/usr/bin/env bash
set -euo pipefail

printf "helloworld" > /tmp/test.txt

for i in $(seq 1 4000); do
  rc rm "rusts3/test1/folder${i}/test.txt"
done
