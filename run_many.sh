
#!/usr/bin/env bash

# Usage: ./run_many.sh 10 [extra harness flags...]

set -u

N="${1:-5}"
shift || true

pass=0
fail=0

mkdir -p runs

for i in $(seq 1 "$N"); do
    echo "=== run $i/$N ==="

    if docker compose run --rm chaos \
        -compose-project soup \
        -out /runs \
        -reset "$@"; then
        pass=$((pass + 1))
    else
        fail=$((fail + 1))
    fi
done

echo "runs=$N pass=$pass fail=$fail" | tee -a runs/SUMMARY.txt
