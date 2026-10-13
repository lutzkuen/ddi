Task: build an inventory of our production kafka-delta-ingest (kdi) deployments, as evidence for porting them to ddi. READ-ONLY: do not apply, restart, delete or modify anything in any cluster, topic, registry or table. Do not print, log or write any secret (SCRAM/PLAIN passwords, SR credentials, storage keys, tokens): record only the NAME of the env var / secret that holds one. Write the result to ./kdi-inventory.md (and ./kdi-inventory/ for supporting files). If something cannot be determined from the repo or from read-only access, write "UNKNOWN: <what I'd need>" instead of guessing.

1. Per kdi deployment (one row per topic/table; find them in the manifests / Helm / compose / CI in this repo and, if kubectl is available and I confirm, `kubectl get ... -o yaml` read-only):
   - topic, table URI, replica count, namespace/deployment name
   - ALL kdi flags verbatim: -a, -g, -l/-m/-b, -c, -o, -K and every -t transform EXACTLY as the pod runs it (an entrypoint may rewrite the list: read the startup log), statsd endpoint/prefix, other env (names only for secrets)
   - container image: repo:tag AND @sha256 digest as actually running, plus the kdi git commit/branch/patches if the Dockerfile builds from source
   - versions of apache-avro, schema_registry_converter, rdkafka, serde_json, deltalake from Cargo.lock or `strings`/`grep -a` on the binary

2. Per bronze table: DDL (SHOW CREATE TABLE or the Delta schema), properties, partitioning, other writers (commit history users), for each STRING column the count of `= 'null'` text and of true NULLs over 7 days, 5 sample rows with free-text/PII columns redacted (say which), latest Delta version, checkpoints, latest `txn` per appId, min/max of timestamp columns over 7 days, whether the JSON text column is in canonical key order (`json_format(json_parse(c)) = c`).

3. Per Schema Registry subject: versions, compatibility, references, naming strategy, decimal encodings (bytes vs fixed, precision/scale), saved as .avsc; SR host only, auth type, private CA or not.

4. Kafka per topic: partition count, retention, cleanup.policy, transactions, ACLs for kdi's principal and group prefix, can it read a group named `ddi-<name>`, lag.

5. Downstream: what consumes kdi's statsd; what reads the bronze tables (Trino views, dbt models, ddi pipelines) and which kdi quirks they depend on ('null' text, raw-number timestamps, byte-array decimals, sorted-key JSON, partition values).

6. Fixture material (last, only with my explicit OK): propose (do not run) a script sampling ~2,000 real messages across 3 partitions of one low-traffic topic with their schema ids.

End kdi-inventory.md with "Answers to open questions" (deployments and where; downstream reliance on kdi quirks; metadata columns present; statsd; SR kind/strategy/CA; group ACLs; any timestamp 1000x too small) and a table of every UNKNOWN with the access needed.
