# Deploying TokenRouter to Coolify

This guide is for platform teams operating the hardened TokenRouter
distribution on a Coolify instance. It covers a first deployment, the secrets
that must be generated once, upgrades, backups and the failures seen most often
in production.

The deployment consists of three containers defined in
`deploy/coolify/docker-compose.yml`:

| Service | Image | Purpose |
| --- | --- | --- |
| `tokenrouter` | `${TOKENROUTER_IMAGE}` | Application and embedded admin UI, listens on 8080 |
| `postgres` | `postgres:18-alpine` | Primary datastore for users, keys, usage and configuration |
| `redis` | `redis:8-alpine` | Cache, locks, sessions and rate-limit state |

All three run on a private bridge network. Only the application is reachable
from outside, and only through the Coolify proxy. PostgreSQL and Redis publish
no host ports.

Core principles:

- No fixed host ports are declared. The Coolify proxy routes to the container
  port using the `expose` declaration and the Coolify-managed FQDN variable.
- All state lives on named volumes. Recreating containers never destroys data.
- Missing required secrets abort the deployment instead of starting a
  misconfigured instance.
- The image runs as a non-root user and the entrypoint drops privileges
  immediately after repairing volume ownership.

## Prerequisites

| Requirement | Detail |
| --- | --- |
| Coolify | A working installation with a server connected and reachable |
| Docker | Version 24 or newer on the target server, with the Compose v2 plugin |
| Resources | At least 2 vCPU, 4 GiB RAM and 20 GiB disk for a small production instance |
| Network | Outbound HTTPS to the container registry and to the LLM providers you route to |
| Repository access | Read access to the image registry, or the repository if you build on the host |
| Domain | A DNS name pointing at the Coolify server, for example `tokenrouter.example.com` |

Decide the deployment shape before starting:

| Shape | When to use it | How |
| --- | --- | --- |
| Pull a prebuilt image | Normal production deployments | Set `TOKENROUTER_IMAGE` to a release tag or digest |
| Build on the Coolify host | Air-gapped or pinned-source environments | Uncomment the `build:` block in the compose file |

## Creating the resource

1. In Coolify, open the target project and environment.
2. Choose **New Resource** and select **Docker Compose**.
3. Point the resource at this repository, or at a Git repository that contains
   the `deploy/coolify` directory.
4. Set the Compose file location to `deploy/coolify/docker-compose.yml`.
5. Do not enable any automatic port publishing option. The compose file
   deliberately declares no host ports.
6. Save the resource. Do not deploy yet: the environment variables must be set
   first, otherwise the deployment aborts on the missing secret guards.

## Setting environment variables

Open the resource's **Environment Variables** tab. Paste the values you want,
one per line, using the names from `deploy/coolify/.env.example` in this
directory. That file documents every variable the stack reads.

Variables that must be set before the first deployment:

| Variable | Required | Notes |
| --- | --- | --- |
| `TOKENROUTER_IMAGE` | Yes in practice | Pin a release tag or an image digest, not `latest` |
| `POSTGRES_PASSWORD` | Yes | No deployment without it |
| `JWT_SECRET` | Yes | No deployment without it |
| `TOTP_ENCRYPTION_KEY` | If 2FA is used | Set it before users enroll in two-factor auth |
| `ADMIN_EMAIL` | Recommended | Login for the first administrator |
| `ADMIN_PASSWORD` | Optional | Leave empty to have the first run generate one |
| `REDIS_PASSWORD` | Recommended | Redis is internal-only, but keep it authenticated |
| `TZ` | Recommended | Container timezone, for example `UTC` |

Coolify-injected variables such as `SERVICE_FQDN_TOKENROUTER_8080` are managed
by the platform. Do not overwrite them manually.

## Generating secrets once

Generate each secret exactly once, store it in your password manager, and keep
it stable for the lifetime of the deployment. Rotating `JWT_SECRET` logs every
user out. Rotating `TOTP_ENCRYPTION_KEY` invalidates every enrolled TOTP
device.

```sh
openssl rand -hex 32   # POSTGRES_PASSWORD
openssl rand -hex 32   # JWT_SECRET
openssl rand -hex 32   # TOTP_ENCRYPTION_KEY
openssl rand -hex 32   # REDIS_PASSWORD
openssl rand -base64 24  # ADMIN_PASSWORD, only if you set it explicitly
```

The first four are hexadecimal strings and contain no characters that need
quoting or escaping. `ADMIN_PASSWORD` is base64 and may contain `+`, `/` and
`=`, all of which are safe in a Compose environment value.

## First run and admin bootstrap

The application initializes itself on the first start. It applies database
migrations, writes its default configuration to `/app/data/config.yaml`,
creates the administrator account and then begins serving traffic.

1. Deploy the resource from the Coolify UI.
2. Watch the logs in the Coolify log viewer, or read them from the shell:

   ```sh
   docker logs -f <tokenrouter-container-name>
   ```

3. Look for the bootstrap line that reports the generated administrator
   credentials. It appears only on the very first start:

   ```
   admin account created: admin@tokenrouter.local
   admin password: <generated-password>
   ```

4. Log in at your FQDN with the admin email and that password, then change the
   password immediately from the account settings.

If `ADMIN_PASSWORD` was set explicitly, no password is printed. Use the value
you configured.

The generated password is not stored anywhere except the database, hashed. If
you miss it, reset it with the application's password reset flow or by entering
a new one through the database; the log line never appears a second time.

## Health checks

| Check | Where | Expected result |
| --- | --- | --- |
| Application | `GET /health` on port 8080 inside the container | HTTP 200 with a healthy body |
| Container healthcheck | Compose `healthcheck` on `tokenrouter` | Reports `healthy` after the start period |
| PostgreSQL | `pg_isready` plus a `SELECT 1` probe | Reports `healthy` before the application starts |
| Redis | `redis-cli ping` | Reports `healthy` before the application starts |

The application container uses a **60 second** start period because the first
run applies migrations. On a cold database this can take longer than a normal
restart. The container is not considered failing until the start period has
elapsed and the retry count is exhausted.

The dependency condition is `service_healthy` for both PostgreSQL and Redis, so
the application never starts against a database that is still initializing.

## Attaching a domain

1. Open the resource in Coolify and go to the application service.
2. Set the FQDN to your domain, for example `https://tokenrouter.example.com`.
3. Coolify provisions a TLS certificate and populates the FQDN environment
   variable that the compose file declares.
4. Confirm DNS points at the Coolify server before requesting the certificate.
5. Verify after the certificate is issued:

   ```sh
   curl -fsS https://tokenrouter.example.com/health
   ```

The proxy forwards to the container port over the internal network. There is no
host port to expose and no firewall rule to add for the application.

## Upgrades

Upgrades replace the application container only. Database and Redis data live
on named volumes and are untouched by a redeploy.

Image-based upgrade:

1. Set `TOKENROUTER_IMAGE` to the new release tag or digest.
2. Take a database backup first; see the next section.
3. Trigger a redeploy from the Coolify UI.
4. Watch the logs for migration completion and a healthy health check.
5. Verify in the admin UI that the version and license text match the release.

Rollback:

1. Set `TOKENROUTER_IMAGE` back to the previous tag or digest.
2. Redeploy.
3. If the release included a database migration that is not backward
   compatible, restore the pre-upgrade backup as well.

Because the compose file uses `pull_policy: always`, a redeploy re-fetches the
configured tag. Pinning a digest is the most reproducible option.

## Volume backups

Three named volumes hold persistent state:

| Volume | Contents | Backup priority |
| --- | --- | --- |
| `postgres_data` | All application data | Critical |
| `tokenrouter_data` | `config.yaml`, logs, generated files | Important |
| `redis_data` | Cache and transient state | Low, rebuilds from empty |

Logical backup of the database, using the version-matched client tools:

```sh
docker exec <postgres-container-name> \
  pg_dump -U tokenrouter -d tokenrouter -Fc -f /tmp/backup.dump
docker cp <postgres-container-name>:/tmp/backup.dump ./tokenrouter-$(date +%F).dump
```

The application image also ships `pg_dump` and `psql`, so backups can be run
from the application container if the database container is unavailable.

Restore:

```sh
docker cp ./tokenrouter-2026-01-01.dump <postgres-container-name>:/tmp/restore.dump
docker exec <postgres-container-name> \
  pg_restore -U tokenrouter -d tokenrouter --clean --if-exists /tmp/restore.dump
```

Application data volume, for the configuration file and generated artifacts:

```sh
docker run --rm \
  -v tokenrouter_tokenrouter_data:/data \
  -v "$PWD":/backup \
  alpine tar czf /backup/tokenrouter-data-$(date +%F).tar.gz -C /data .
```

Schedule these as Coolify scheduled tasks or an external cron job, and store
the results off the host. A backup on the same disk as the database is not a
backup.

## Resource sizing

Sizes assume a single application replica with the default connection pool and
moderate streaming traffic. Scale vertically first; the application is
CPU-light relative to the memory it holds for concurrent streams.

| Tier | vCPU | RAM | Disk | Application limit | PostgreSQL limit | Redis limit |
| --- | --- | --- | --- | --- | --- | --- |
| Small | 2 | 4 GiB | 20 GiB | 2g | 1g | 512m |
| Medium | 4 | 8 GiB | 80 GiB | 4g | 3g | 1g |
| Large | 8 | 24 GiB | 250 GiB | 10g | 10g | 2g |

Tuning notes per tier:

- Small: `POSTGRES_MAX_CONNECTIONS=100`, `POSTGRES_SHARED_BUFFERS=256MB`,
  `GOMEMLIMIT=1GiB`, `TOKENROUTER_MEM_LIMIT=2g`.
- Medium: the defaults in `.env.example`, with `POSTGRES_MAX_CONNECTIONS=200`
  and `GOMEMLIMIT=3GiB`.
- Large: raise `DATABASE_MAX_OPEN_CONNS` to 100 and `POSTGRES_MAX_CONNECTIONS`
  to 400 together, so the server limit stays above the sum of application
  pools plus headroom.

Keep container memory limits below the host's physical memory, and leave room
for the host operating system. When the application limit is raised, raise
`GOMEMLIMIT` with it; the runtime target should stay roughly 75 percent of the
container limit.

## Troubleshooting

### Deployment fails immediately with a missing password

Symptom: Compose aborts with a message naming `POSTGRES_PASSWORD`.

Cause: the `:?` guard on the variable fired because it is unset or empty in the
Coolify environment.

Resolution:

1. Open the resource's environment variables in Coolify.
2. Set `POSTGRES_PASSWORD` to the value generated with `openssl rand -hex 32`.
3. Redeploy. Confirm the container environment, not just the UI, contains the
   value: `docker inspect <postgres-container-name>` and read the `Env` list.

### Deployment fails with a missing JWT secret

Symptom: Compose aborts with a message naming `JWT_SECRET`.

Cause: the same guard fired for the session signing key.

Resolution: set `JWT_SECRET` in the Coolify UI and redeploy. Do not invent a
throwaway value. If this deployment previously ran with a different secret, all
existing sessions are already invalid; users log in again.

### Volume permission errors

Symptom: the application logs permission denied errors on `/app/data`, or the
container restarts repeatedly while the entrypoint reports a failed chown.

Cause: the named volume was created by another user, or an operator manually
populated it with root-owned files.

Resolution:

1. Check the container logs for the entrypoint ownership message.
2. Confirm the volume contents: the application expects uid and gid 1000 to own
   `/app/data`. The entrypoint repairs this automatically when the container
   starts as root, which is the default.
3. If the volume contains files owned by another uid, fix them:

   ```sh
   docker run --rm -v tokenrouter_tokenrouter_data:/data alpine \
     chown -R 1000:1000 /data
   ```

4. Confirm no read-only bind mount is shadowing a file the application must
   write. A `config.yaml` mount must be read-only, but the surrounding
   directory must stay writable.

### Health check failures

Symptom: the container never reports healthy, or Coolify marks the deployment
unhealthy and rolls it back.

Check the layers in order:

| Layer | Command | What to look for |
| --- | --- | --- |
| Process | `docker logs <tokenrouter-container-name>` | Migration errors, crash loops, configuration parse failures |
| Local endpoint | `docker exec <tokenrouter-container-name> wget -qO- http://localhost:8080/health` | A 200 response from inside the container |
| Database | `docker exec <postgres-container-name> pg_isready -U tokenrouter` | `accepting connections` |
| Redis | `docker exec <redis-container-name> redis-cli ping` | `PONG` |
| Startup time | Container status | Still inside the 60 second start period on first run |

Common causes:

- First run needs more time because migrations run before the server binds.
  Wait for the full start period before investigating.
- The database is not healthy, so `depends_on` blocks the application start.
  Fix the database first.
- Redis was started with an eviction policy other than `noeviction`, causing
  state loss that the application reports as an error. Confirm the running
  command with `docker inspect` and check `REDIS_MAXMEMORY_POLICY`.
- The connection pool exceeds the server limit. Compare
  `POSTGRES_MAX_CONNECTIONS` with the sum of `DATABASE_MAX_OPEN_CONNS` across
  replicas.

### Users are logged out after every redeploy

Cause: `JWT_SECRET` is unset or changes between deployments, so the application
generates a new random secret at each start.

Resolution: set a stable `JWT_SECRET` in the Coolify UI, generated once with
`openssl rand -hex 32`, and redeploy. Verify with `docker inspect` that the
container environment carries the same value across restarts.

### TOTP codes stop working after a redeploy

Cause: `TOTP_ENCRYPTION_KEY` changed, or was left empty so a new random key was
generated at startup.

Resolution: restore the original key from your password manager and redeploy.
If the key was never recorded, affected users must re-enroll their TOTP device
after logging in with a password.
