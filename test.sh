#!/usr/bin/env bash
set -euo pipefail

printf "helloworld" > /tmp/test.txt

for i in $(seq 1 4000); do
  rc cp /tmp/test.txt "rusts3/test1/folder${i}/test.txt"
done
