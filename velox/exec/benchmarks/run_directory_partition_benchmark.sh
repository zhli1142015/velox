#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
BM=./velox/exec/benchmarks/velox_directory_partition_benchmark
ITERS=8
CSV_FILE=/tmp/bench_csv_results.txt

CASES=(
  AGG-S1 AGG-M2 AGG-L1 AGG-L2
  TPCDS-Q97 TPCDS-Q65 TPCDS-Q67 TPCDS-Q23a
  TPCDS-Q4 TPCDS-Q38 TPCDS-Q47 TPCDS-Q1
  JOIN-SS JOIN-LL JOIN-LS
)

true >"$CSV_FILE"
total=${#CASES[@]}
idx=0

for case_name in "${CASES[@]}"; do
  idx=$((idx + 1))
  echo "[$idx/$total] $case_name baseline..."
  $BM --num_iterations=$ITERS --case_filter="$case_name" --run_mode=baseline 2>/dev/null |
    grep "^CSV," >>"$CSV_FILE"
  echo "[$idx/$total] $case_name partitioned..."
  $BM --num_iterations=$ITERS --case_filter="$case_name" --run_mode=partitioned 2>/dev/null |
    grep "^CSV," >>"$CSV_FILE"
done

echo ""
echo "========================================================"
echo "FULLY ISOLATED RESULTS (separate process per case×mode)"
echo "Iterations: $ITERS"
echo "========================================================"
printf "%-20s %12s %12s %8s %10s\n" "Case" "Baseline" "Partitioned" "Speedup" "Rows"
printf '%0.s-' {1..66}
echo ""

for case_name in "${CASES[@]}"; do
  b_line=$(grep "^CSV,${case_name},baseline," "$CSV_FILE")
  p_line=$(grep "^CSV,${case_name},partitioned," "$CSV_FILE")
  if [ -n "$b_line" ] && [ -n "$p_line" ]; then
    b_ns=$(echo "$b_line" | cut -d, -f4)
    b_rows=$(echo "$b_line" | cut -d, -f5)
    p_ns=$(echo "$p_line" | cut -d, -f4)
    b_ms=$(echo "scale=1; $b_ns / 1000000" | bc)
    p_ms=$(echo "scale=1; $p_ns / 1000000" | bc)
    if [ "$p_ns" -gt 0 ]; then
      speedup=$(echo "scale=2; $b_ns * 100 / $p_ns" | bc)
      sp_int=${speedup%.*}
      sp_show=$(echo "scale=2; $speedup / 100" | bc)
      if [ "$sp_int" -ge 150 ]; then
        ind=" ★★★"
      elif [ "$sp_int" -ge 120 ]; then
        ind=" ★★"
      elif [ "$sp_int" -ge 105 ]; then
        ind=" ★"
      elif [ "$sp_int" -lt 97 ]; then
        ind=" ⚠"
      else
        ind=""
      fi
      printf "%-20s %9s ms %9s ms  %5sx%-4s %10s\n" "$case_name" "$b_ms" "$p_ms" "$sp_show" "$ind" "$b_rows"
    fi
  fi
done
echo ""
echo "CSV data saved to: $CSV_FILE"
