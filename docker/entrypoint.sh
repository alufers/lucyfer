#!/usr/bin/env bash
# Container entrypoint: optionally run the bundled Statime PTP daemon alongside
# lucyfer, then supervise both.
#
# LUCYFER_PTP=1|true|yes|on  -> start Statime (default: off, lucyfer only)
# LUCYFER_PTP_CONFIG=<path>  -> Statime config (default: /etc/lucyfer/statime.toml)
#
# The daemon publishes its media clock on a usrvclock socket. Because it runs in
# this very container there is no volume and no TMPDIR to line up: the socket path
# in statime.toml just has to match `dante.clock_path` (both default to
# /tmp/ptp-usrvclock).

set -uo pipefail

log() { printf '%s entrypoint: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2; }

statime_pid=
lucyfer_pid=

terminate() {
  trap - TERM INT
  [[ -n $statime_pid ]] && kill -TERM "$statime_pid" 2>/dev/null
  [[ -n $lucyfer_pid ]] && kill -TERM "$lucyfer_pid" 2>/dev/null
  wait
}

on_signal() {
  local signal=$1
  log "received SIG$signal, shutting down"
  terminate
  exit $(($2 + 128))
}
trap 'on_signal TERM 15' TERM
trap 'on_signal INT 2' INT

case "${LUCYFER_PTP:-}" in
  [1yY] | [tT][rR][uU][eE] | [yY][eE][sS] | [oO][nN])
    ptp_config="${LUCYFER_PTP_CONFIG:-/etc/lucyfer/statime.toml}"
    if [[ ! -f $ptp_config ]]; then
      log "LUCYFER_PTP is enabled but the Statime config '$ptp_config' does not exist."
      log "Mount one there, or point LUCYFER_PTP_CONFIG at it."
      exit 1
    fi
    log "starting Statime PTP daemon with config '$ptp_config'"
    statime -c "$ptp_config" &
    statime_pid=$!
    ;;
  *)
    log "PTP daemon disabled (set LUCYFER_PTP=1 to enable); expecting an external media clock"
    ;;
esac

if [[ $# -gt 0 ]]; then
  lucyfer "$@" &
else
  lucyfer --config /etc/lucyfer/config.yaml &
fi
lucyfer_pid=$!

# Whichever process exits first takes the container down: a dead clock must not
# leave lucyfer silently waiting for one forever. `restart: unless-stopped` then
# brings the pair back up.
wait -n
status=$?

if [[ -z $statime_pid ]]; then
  log "lucyfer exited (status $status)"
elif kill -0 "$statime_pid" 2>/dev/null; then
  log "lucyfer exited (status $status), stopping Statime"
else
  log "Statime exited (status $status), stopping lucyfer"
fi

terminate
exit "$status"
