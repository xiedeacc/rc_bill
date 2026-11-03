#!/usr/bin/env bash
# A/B/B/A on one runner, fresh processes, schemas, topics and group for each round.
set -euo pipefail
baseline="${1:?Supply an audited baseline commit SHA}"
[[ "$baseline" =~ ^[a-f0-9]{40}$ ]]
candidate=$(git rev-parse HEAD)
base_dir="$PWD/tmp/performance-baseline"
git fetch --depth=1 origin "$baseline"
git worktree add --detach "$base_dir" "$baseline"
(cd "$base_dir" && ./mvnw -q -DskipTests package) > ci-demo-comparison-build.log 2>&1
base_jar="$base_dir/notification-api/target/notification-api-0.1.0-SNAPSHOT.jar"
candidate_jar="$PWD/notification-api/target/notification-api-0.1.0-SNAPSHOT.jar"
mkdir -p tmp/performance-initial
for file in ci-demo-*; do mv "$file" tmp/performance-initial/; done
index=0
for version in baseline candidate candidate baseline; do
  index=$((index + 1))
  export PERF_FIXTURE_ID="perf_${GITHUB_RUN_ID:-local}_${index}"
  docker compose -f compose.test.yaml exec -T postgres psql -U notification -d notification \
    -v ON_ERROR_STOP=1 -c "CREATE SCHEMA $PERF_FIXTURE_ID"
  if [[ "$version" == baseline ]]; then
    export DEMO_API_JAR="$base_jar"
  else
    export DEMO_API_JAR="$candidate_jar"
  fi
  bash scripts/demo-smoke.sh
  for file in ci-demo-*; do
    mv "$file" "ci-paired-${index}-${version}-${file#ci-demo-}"
  done
done
printf '{"baseline":"%s","candidate":"%s","order":["baseline","candidate","candidate","baseline"],"worker":"candidate throughout"}\n' \
  "$baseline" "$candidate" > ci-demo-comparison.json
for file in ci-paired-*; do mv "$file" "ci-demo-${file#ci-}"; done
for file in tmp/performance-initial/*; do cp "$file" .; done
