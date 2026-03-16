#!/bin/bash
# Run N concurrent benchmark instances, each on its own core
N=${1:-8}
BENCH=./velox/exec/benchmarks/velox_directory_partition_benchmark

echo "=== Running $N concurrent instances ==="

# Run all instances in background, capture output
for i in $(seq 1 $N); do
    taskset -c $((i-1)) $BENCH --num_iterations=1 2>/dev/null | grep "CSV," > /tmp/bench_task_$i.txt &
done

# Wait for all
wait

# Aggregate results
echo "=== Results per instance ==="
for i in $(seq 1 $N); do
    AGG_M2=$(grep "AGG-M2" /tmp/bench_task_$i.txt | cut -d, -f3)
    AGG_L1=$(grep "AGG-L1" /tmp/bench_task_$i.txt | cut -d, -f3)
    AGG_L2=$(grep "AGG-L2" /tmp/bench_task_$i.txt | cut -d, -f3)
    echo "Task $i: AGG-M2=${AGG_M2:-FAIL} AGG-L1=${AGG_L1:-FAIL} AGG-L2=${AGG_L2:-FAIL}"
done

# Compute averages
echo ""
echo "=== Averages ==="
python3 << 'PYEOF'
import os
results = {}
for i in range(1, int(os.environ.get('N', '8'))+1):
    try:
        with open(f'/tmp/bench_task_{i}.txt') as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) >= 3:
                    case = parts[1]
                    ns = int(parts[2])
                    if case not in results:
                        results[case] = []
                    results[case].append(ns)
    except:
        pass

for case in sorted(results.keys()):
    vals = results[case]
    avg = sum(vals) / len(vals)
    mn = min(vals)
    mx = max(vals)
    print(f"  {case}: avg={avg/1e6:.1f}ms min={mn/1e6:.1f} max={mx/1e6:.1f} ({len(vals)} tasks)")
PYEOF
