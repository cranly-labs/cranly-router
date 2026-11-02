#!/bin/sh
# =============================================================================
# TokenRouter hardened container entrypoint
# =============================================================================
# Responsibilities, in order:
#   1. Repair ownership of the persistent data directory when running as root.
#   2. Drop privileges to the tokenrouter user (uid/gid 1000) via su-exec.
#   3. Run optional init hooks from /docker-entrypoint-initdb.d (sorted, optional).
#   4. Validate that the application binary exists.
#   5. exec the application so it becomes PID 1 and receives signals directly.
#
# The script is POSIX sh and is executed by Alpine /bin/sh (busybox ash).
# No bash-only constructs are used.
# =============================================================================

set -eu

APP_USER="tokenrouter"
APP_UID="1000"
APP_BINARY="/app/tokenrouter"
DATA_DIR="/app/data"
HOOKS_DIR="/docker-entrypoint-initdb.d"

log() {
    printf '%s [entrypoint] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

# -----------------------------------------------------------------------------
# 1. Ownership repair (root only)
# -----------------------------------------------------------------------------
# Docker named volumes and host bind mounts are frequently owned by root, which
# prevents the non-root application user from writing. When started as root we
# fix the data directory, then re-exec this same script as the application user
# so every later step runs with the final privileges.
#
# The chown is best-effort: a read-only bind mount of an individual file inside
# /app/data (for example config.yaml mounted with :ro) must not abort startup.
if [ "$(id -u)" = "0" ]; then
    mkdir -p "${DATA_DIR}"
    chown -R "${APP_USER}:${APP_USER}" "${DATA_DIR}" 2>/dev/null || true

    if ! command -v su-exec >/dev/null 2>&1; then
        log "error: su-exec is not available; cannot drop privileges"
        exit 1
    fi

    exec su-exec "${APP_USER}" "$0" "$@"
fi

# -----------------------------------------------------------------------------
# 2. Privilege verification
# -----------------------------------------------------------------------------
# From this point onward the script must run as the unprivileged application
# user. Running as root here would mean su-exec silently failed.
if [ "$(id -u)" = "0" ]; then
    log "error: still running as root after privilege drop"
    exit 1
fi

# -----------------------------------------------------------------------------
# 3. Optional init hooks
# -----------------------------------------------------------------------------
# Any *.sh file placed in /docker-entrypoint-initdb.d is executed once per
# container start, in sorted filename order, before the application starts.
# The directory is optional: if it does not exist, or contains no matching
# files, this step is skipped silently. This mirrors the PostgreSQL image
# convention so operators already familiar with it can reuse the pattern.
if [ -d "${HOOKS_DIR}" ]; then
    hook_count=0
    for hook in "${HOOKS_DIR}"/*.sh; do
        # Guard against the literal glob when no *.sh file exists.
        [ -e "${hook}" ] || continue
        case "${hook}" in
            *.sh)
                if [ ! -r "${hook}" ]; then
                    log "error: hook is not readable: ${hook}"
                    exit 1
                fi
                log "running init hook: ${hook}"
                # Run hooks in a subshell so their shell state cannot leak.
                sh "${hook}"
                hook_count=$((hook_count + 1))
                ;;
        esac
    done
    if [ "${hook_count}" -gt 0 ]; then
        log "executed ${hook_count} init hook(s)"
    fi
fi

# -----------------------------------------------------------------------------
# 4. Default command compatibility
# -----------------------------------------------------------------------------
# When the image is invoked with a leading flag (for example --help or
# --version), prepend the application binary so the invocation behaves the same
# way as the historical ENTRYPOINT ["/app/tokenrouter"] form.
if [ "${1#-}" != "$1" ]; then
    set -- "${APP_BINARY}" "$@"
fi

# -----------------------------------------------------------------------------
# 5. Binary validation
# -----------------------------------------------------------------------------
if [ ! -x "${APP_BINARY}" ]; then
    log "error: application binary not found or not executable: ${APP_BINARY}"
    exit 1
fi

# -----------------------------------------------------------------------------
# 6. Execute
# -----------------------------------------------------------------------------
# exec replaces the shell, so the application becomes PID 1 and receives
# SIGTERM/SIGINT directly. This preserves correct shutdown semantics inside
# containers and avoids the extra shell process eating signals.
log "starting: $*"
exec "$@"
