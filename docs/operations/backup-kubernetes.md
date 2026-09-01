# Backups on Kubernetes (runbook)

This guide configures **automated daily backups** of the `otel-sqlite`
StatefulSet using the CronJob shipped in
`deploy/kubernetes/otel-sqlite-backup-cronjob.yaml`. It covers install,
verification, encryption, retention, alerting and restore.

The backup uses the standard contract from
[docs/operations/backup-restore.md](backup-restore.md): the CronJob pod mounts
the **same** data volume as the server pod and runs `otel-sqlite backup`, which
opens the live database **read-only** — the server keeps ingesting while the
snapshot is taken.

## 1. Prerequisites

* A running cluster with a `StorageClass` that provides
  `ReadWriteOnce` volumes.
* The server deployed first (see `deploy/kubernetes/otel-sqlite.yaml`):
  StatefulSet `otel-sqlite`, its data PVC
  `otel-sqlite-data-otel-sqlite-0`, and the `otel-sqlite-security` Secret.

## 2. Install

```sh
kubectl create namespace otel-sqlite   # if not already used

# 1. Deploy (or update) the server.
kubectl -n otel-sqlite apply -f deploy/kubernetes/otel-sqlite.yaml

# 2. Deploy the backup PVC + CronJob.
kubectl -n otel-sqlite apply -f deploy/kubernetes/otel-sqlite-backup-cronjob.yaml
```

Because the data volume is `ReadWriteOnce`, the CronJob pod is scheduled onto
the node that already hosts the volume; both pods mount it concurrently, which
SQLite WAL supports for one reader plus one writer.

## 3. Verify it works

```sh
# CronJob is registered with its schedule.
kubectl -n otel-sqlite get cronjob otel-sqlite-backup

# Trigger a backup immediately and watch it.
kubectl -n otel-sqlite create job --from=cronjob/otel-sqlite-backup manual-backup
kubectl -n otel-sqlite get job manual-backup
kubectl -n otel-sqlite logs job/manual-backup -c backup

# List the artifacts on the backups volume (backup pod, once finished).
kubectl -n otel-sqlite get pods -l job-name=manual-backup
kubectl -n otel-sqlite exec otel-sqlite-0 -- /usr/local/bin/otel-sqlite verify --db /data/otel-logs.db
```

Artifacts are named `otel-logs.db.<UTC>.bak` (plaintext) or `.otsb`
(encrypted) and are verified (integrity, foreign keys, row counts) before the
backup exits 0.

## 4. Encryption (optional but recommended)

1. Create a 32-byte key Secret **outside** the data PVC:

   ```sh
   openssl rand -out /tmp/backup.key 32
   kubectl -n otel-sqlite create secret generic otel-sqlite-security \
       --from-file=backup.key=/tmp/backup.key \
       --dry-run=client -o yaml | kubectl -n otel-sqlite apply -f -
   rm /tmp/backup.key
   ```

   (Or patch the existing `otel-sqlite-security` Secret — see
   `deploy/kubernetes/otel-sqlite.yaml` for its other keys.)

2. Edit `deploy/kubernetes/otel-sqlite-backup-cronjob.yaml`: uncomment the
   `--key-file` args and the `key` volume mount/volume:

   ```yaml
   args:
     - backup
     - --db
     - /data/otel-logs.db
     - --out
     - /backups
     - --key-file
     - /keys/backup.key
     - --keep
     - "30"
   ```

   ```yaml
   volumeMounts:
     - name: key
       mountPath: /keys
       readOnly: true
   volumes:
     - name: key
       secret:
         secretName: otel-sqlite-security
         items:
           - key: backup.key
             path: backup.key
         defaultMode: 0400
   ```

3. Re-apply and re-verify:

   ```sh
   kubectl -n otel-sqlite apply -f deploy/kubernetes/otel-sqlite-backup-cronjob.yaml
   kubectl -n otel-sqlite create job --from=cronjob/otel-sqlite-backup enc-check
   kubectl -n otel-sqlite logs job/enc-check -c backup   # .otsb artifact
   ```

Artifacts then carry the `"OTSQBAK1"` magic; a wrong key or a tampered
artifact fails AEAD authentication on restore.

## 5. Retention and disk capacity

`--keep 30` retains the newest 30 artifacts per run. The `otel-sqlite-backups`
PVC (`40Gi` in the manifest) must hold at least:

```text
(2 × database size) + (keep × average artifact size)
```

Resize the PVC request if needed, and alert on the backups volume filling.
A disk-full backup exits non-zero and never reports success for an unverified
artifact.

## 6. Alerting on failure

A failed backup is a failed CronJob Job. Alert with Prometheus + kube-state-metrics:

```yaml
# kube_job_status_failed: any failed job; kube_cronjob_status_last_successful_time:
# backup "went stale".
groups:
  - name: otel-sqlite-backup.rules
    rules:
      - alert: OtelSqliteBackupFailed
        expr: kube_job_status_failed{job_name=~"otel-sqlite-backup.*", condition="true"} > 0
        for: 5m
        labels: { severity: page }
        annotations:
          summary: "otel-sqlite backup job {{ $labels.job_name }} failed"
      - alert: OtelSqliteBackupStale
        expr: time() - kube_cronjob_status_last_successful_time{job_name="otel-sqlite-backup"} > 36 * 3600
        labels: { severity: page }
        annotations:
          summary: "no successful otel-sqlite backup for 36h"
```

Also alert on the backups volume:
`kubelet_volume_stats_available_bytes{persistentvolumeclaim="otel-sqlite-backups"} < 2e9`.

## 7. Restore

Restore is **offline**: the server pod must be scaled to 0 first, then the
restored database is copied into the data PVC.

```sh
# 1. Stop the server (graceful drain + final WAL truncate).
kubectl -n otel-sqlite scale statefulset otel-sqlite --replicas=0

# 2. Run a one-shot restore pod mounting the backups volume, pick an artifact
#    (.bak plaintext or .otsb with the key Secret) and restore into a clean
#    directory.
kubectl -n otel-sqlite apply -f - <<'YAML'
apiVersion: batch/v1
kind: Job
metadata:
  name: otel-sqlite-restore
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: restore
          image: otel-sqlite:0.1.0
          args:
            - restore
            - --backup
            - /backups/otel-logs.db.20260830T121426Z.otsb   # <- your artifact
            - --dir
            - /data/restored
            - --key-file
            - /keys/backup.key                              # only if encrypted
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: { drop: ["ALL"] }
          volumeMounts:
            - { name: backups, mountPath: /backups }
            - { name: data, mountPath: /data }
            - { name: key, mountPath: /keys, readOnly: true }
            - { name: tmp, mountPath: /tmp }
      volumes:
        - name: backups
          persistentVolumeClaim: { claimName: otel-sqlite-backups }
        - name: data
          persistentVolumeClaim: { claimName: otel-sqlite-data-otel-sqlite-0 }
        - name: key
          secret:
            secretName: otel-sqlite-security
            items: [{ key: backup.key, path: backup.key }]
            defaultMode: 0400
        - name: tmp
          emptyDir: {}
YAML
kubectl -n otel-sqlite wait --for=condition=complete job/otel-sqlite-restore
kubectl -n otel-sqlite logs job/otel-sqlite-restore   # verified before/after

# 3. Verify the restored database before trusting it.
kubectl -n otel-sqlite apply -f - <<'YAML'
apiVersion: batch/v1
kind: Job
metadata: { name: otel-sqlite-verify }
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: verify
          image: otel-sqlite:0.1.0
          args: ["verify", "--db", "/data/restored/otel-logs.db"]
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: { drop: ["ALL"] }
          volumeMounts:
            - { name: data, mountPath: /data }
            - { name: tmp, mountPath: /tmp }
      volumes:
        - name: data
          persistentVolumeClaim: { claimName: otel-sqlite-data-otel-sqlite-0 }
        - name: tmp
          emptyDir: {}
YAML
kubectl -n otel-sqlite wait --for=condition=complete job/otel-sqlite-verify
kubectl -n otel-sqlite logs job/otel-sqlite-verify
#    Confirm: integrity=ok, foreign keys=0, schema=003, rows match the backup.

# 4. Put the restored database in place and start the server.
kubectl -n otel-sqlite delete job otel-sqlite-restore otel-sqlite-verify
kubectl -n otel-sqlite exec otel-sqlite-0 -- sh -c \
  'cd /data && mv otel-logs.db otel-logs.db.pre-restore && mv restored/otel-logs.db otel-logs.db'
kubectl -n otel-sqlite scale statefulset otel-sqlite --replicas=1
```

> The `exec otel-sqlite-0` step above runs *after* the server is back up — if
> you prefer, do the `mv` inside the restore job before deleting it (add a
> `command: ["/bin/sh","-c", "...restore && mv ..."]`). Keep the failed
> database and the artifact until the next successful backup.

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Backup job `CreateContainerConfigError` | Key Secret missing / wrong name | Create `otel-sqlite-security` with `backup.key` (section 4). |
| Backup job stays `Pending` | RWO volume already used on another node, or PVC not bound | The CronJob pod is scheduled to the server's node automatically; check `kubectl describe pod`. Ensure the `otel-sqlite-backups` PVC is Bound. |
| Backup job fails "cannot open …" | Server pod not running (no `-shm` sidecar yet), or mount perms | Confirm the StatefulSet is Serving; check `kubectl logs` for the exact error. |
| Artifact is `-1.bak` suffixed | Two runs in the same second | Harmless; the tool never overwrites. |
| Restore refuses the directory | Directory not empty | Use a fresh empty dir (`/data/restored`); the tool never overwrites. |
| Restore fails "wrong key or corrupted" | Key mismatch or damaged artifact | Re-check the `backup.key` Secret bytes (length 32); take a new backup. |

## 9. Reference

* Runbook & contract: `docs/operations/backup-restore.md`
* Manifests: `deploy/kubernetes/otel-sqlite.yaml`,
  `deploy/kubernetes/otel-sqlite-backup-cronjob.yaml`
* Schema migration/rollback: `docs/operations/upgrades.md`