#!/usr/bin/env bash
# Demo: prints every record in the dead-letter topic as its key, reject reason and detail. Run from the HQ side.
# For all headers and the full value, use kafka-ui instead (infra: docker compose --profile tools up -d kafka-ui).
#   demo/read-dlt.sh
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: keep container paths as they are
tab=$'\t'
# Console output per record: headers<TAB>key<TAB>value. The partition/offset headers are binary, so only the
# text headers are picked out.
docker run --rm --network branch-sales-hq apache/kafka:4.3.1 \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 \
  --topic branch-sales.daily-summary.dlt --from-beginning --timeout-ms 10000 \
  --formatter-property print.key=true --formatter-property print.headers=true 2>/dev/null \
  | LC_ALL=C grep -a -o -E "kafka_dlt-exception-message:.*,kafka_dlt-original-topic:|reject-reason:[A-Z_]+${tab}[^${tab}]*" \
  | sed -E -e 's/^kafka_dlt-exception-message:(.*),kafka_dlt-original-topic:$/  detail: \1/' \
           -e "s/^reject-reason:([A-Z_]+)${tab}(.*)$/key=\2 reason=\1/" \
  || true
