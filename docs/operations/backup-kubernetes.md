# Backups on Kubernetes (runbook)

Daily backups via the CronJob in
`deploy/kubernetes/otel-sqlite-backup-cronjob.yaml`. Contract in
[backup-restore](backup-restore.md): CronJob mounts the **same** data
volume and runs `otel-sqlite backup` read-only — server keeps ingesting.

## 1. Install

Prereqs: cluster with `ReadWriteOnce` StorageClass; server deployed
(`deploy/kubernetes/otel-sqlite.yaml`, StatefulSet + data PVC +
`otel-sqlite-security` Secret).

```sh
kubectl -n otel-sqlite apply -f deploy/kubernetes/otel-sqlite.yaml
kubectl -n otel-sqlite apply -f deploy/kubernetes/otel-sqlite-backup-cronjob.yaml
```

RWO volumes mount on one node only: the CronJob pod schedules onto the
server's node automatically via volume affinity. If `Pending`, check
`kubectl describe pod`. Artifacts: `otel-logs.db.<UTC>.bak` / `.otsb`,
verified before exit 0.

Verify: `kubectl get cronjob otel-sqlite-backup`,
`kubectl create job --from=cronjob/otel-sqlite-backup manual-backup`,
`kubectl logs job/manual-backup -c backup`.

## 2. Encryption (recommended)

```sh
openssl rand -out /tmp/backup.key 32
kubectl -n otel-sqlite create secret generic otel-sqlite-security \
  --from-file=backup.key=/tmp/backup.key --dry-run=client -o yaml | kubectl apply -f -
rm /tmp/backup.key
```

Uncomment `--key-file /keys/backup.key` args + `key` volume/mount in the
CronJob (`secretName: otel-sqlite-security`, `defaultMode: 0400`),
re-apply, trigger `enc-check`, confirm `.otsb`.

`--keep 30` prunes beyond newest 30. Size backups PVC as
`(2 × DB) + (keep × avg)` (manifest: `40Gi`); disk-full exits non-zero.

## 3. Alerting

```yaml
- alert: OtelSqliteBackupFailed
  expr: kube_job_status_failed{job_name=~"otel-sqlite-backup.*", condition="true"} > 0
- alert: OtelSqliteBackupStale
  expr: time() - kube_cronjob_status_last_successful_time{job_name="otel-sqlite-backup"} > 36*3600
```

Plus volume space:
`kubelet_volume_stats_available_bytes{persistentvolumeclaim="otel-sqlite-backups"} < 2e9`.

## 4. Restore (offline)

Scale to 0, restore into a clean dir, verify, swap, scale back:

```sh
kubectl -n otel-sqlite scale statefulset otel-sqlite --replicas=0
# one-shot restore Job (mounts backups + data PVCs, + key Secret if .otsb):
# args: restore --backup /backups/otel-logs.db.<UTC>.otsb --dir /data/restored [--key-file /keys/backup.key]
kubectl -n otel-sqlite wait --for=condition=complete job/otel-sqlite-restore
# verify Job: verify --db /data/restored/otel-logs.db (integrity=ok, FK=0, schema=005)
# swap Job (server still scaled to 0):
#   mv otel-logs.db otel-logs.db.pre-restore; mv restored/otel-logs.db otel-logs.db
kubectl -n otel-sqlite delete job otel-sqlite-restore otel-sqlite-verify otel-sqlite-swap
kubectl -n otel-sqlite scale statefulset otel-sqlite --replicas=1
```

Full Job specs reuse the CronJob's pod template (`deploy/kubernetes/otel-sqlite-backup-cronjob.yaml`)
with `restore` / `verify` args and the same security context (`runAsUser: 10001`,
`readOnlyRootFilesystem`, drop `ALL`). Keep failed DB + artifact until next success.

## 5. Troubleshooting

| Symptom | Fix |
|---|---|
| `CreateContainerConfigError` | Create `otel-sqlite-security` with `backup.key` |
| Job `Pending` | RWO node affinity / PVC `Bound` — `describe pod` |
| `cannot open …` | StatefulSet must be Serving (creates `-shm`) |
| Restore refuses dir | Fresh empty `/data/restored` (never overwrites) |
| `wrong key or corrupted` | Re-check 32-byte Secret; new backup |

Refs: [backup-restore](backup-restore.md),
`deploy/kubernetes/otel-sqlite.yaml`,
`otel-sqlite-backup-cronjob.yaml`, [upgrades](upgrades.md).
