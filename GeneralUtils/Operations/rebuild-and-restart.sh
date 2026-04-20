#!/usr/bin/env bash
# rebuild-and-restart.sh
# Builds the React frontend (with memory cap) and restarts Frappe.
# Run from Git Bash: bash rebuild-and-restart.sh
# Optional PEM override: bash rebuild-and-restart.sh /path/to/key.pem

set -euo pipefail

KEYS="/c/MyWorkSpace/Ourlife/Apps/keys"
SERVER="ubuntu@34.223.1.167"
PEM="${1:-$KEYS/winwizedev.pem}"

FRONTEND_REMOTE="/home/ubuntu/frontend/winrich-stock-manager-ui"
BUILD_DEST="/var/www/prod"

SSH="ssh -i $PEM -o StrictHostKeyChecking=no"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  Winrich — Rebuild + Restart Frappe                  ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── 1. Add swap if not already present (prevents OOM during build) ────────────
echo "▶  [1/4] Ensuring swap is available..."
$SSH $SERVER "bash -c '
  if swapon --show | grep -q /swapfile; then
    echo \"   swap already active\"
  else
    sudo fallocate -l 2G /swapfile 2>/dev/null || sudo dd if=/dev/zero of=/swapfile bs=1M count=2048 status=none
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile -q
    sudo swapon /swapfile
    echo \"   swap created and enabled (2 GB)\"
  fi
'"

# ── 2. Build React with Node memory capped at 1.5 GB ─────────────────────────
echo "▶  [2/4] Building React frontend (memory-capped, ~2-3 min)..."
$SSH $SERVER "bash -c '
  cd $FRONTEND_REMOTE
  export NODE_OPTIONS=\"--max-old-space-size=1536\"
  export CI=false
  npm run build 2>&1 | tail -10
  echo \"   build exit: \$?\"
'"
echo "   ✓ Build complete"

# ── 3. Deploy build to web root ───────────────────────────────────────────────
echo "▶  [3/4] Deploying build to $BUILD_DEST..."
$SSH $SERVER "sudo cp -r $FRONTEND_REMOTE/build/* $BUILD_DEST/"
echo "   ✓ Deployed to $BUILD_DEST"

# ── 4. Start / restart Frappe ─────────────────────────────────────────────────
echo "▶  [4/4] Starting Frappe..."
$SSH $SERVER "bash -c '
  if pm2 list | grep -q frappe-dev; then
    pm2 restart frappe-dev
    echo \"   restarted frappe-dev\"
  else
    pm2 start \"honcho start\" --name frappe-dev --cwd /home/ubuntu/winrich/frappe-bench
    echo \"   started frappe-dev via honcho\"
  fi
  pm2 save
'"

echo "   Waiting 15 s for Frappe to come up..."
sleep 15

$SSH $SERVER "curl -s http://localhost:8000/api/method/frappe.ping" | grep -q "pong" \
  && echo "   ✓ Frappe is up" \
  || echo "   ⚠ Frappe ping failed — run: ssh -i $PEM $SERVER 'pm2 logs frappe-dev --lines 30'"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  Done — http://34.223.1.167                          ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
