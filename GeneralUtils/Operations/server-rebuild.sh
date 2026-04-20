#!/usr/bin/env bash
# server-rebuild.sh — run this directly on the server
# scp it over, then: chmod +x server-rebuild.sh && ./server-rebuild.sh

set -euo pipefail

FRONTEND="/home/ubuntu/frontend/winrich-stock-manager-ui"
BUILD_DEST="/var/www/prod"
FRAPPE_BENCH="/home/ubuntu/winrich/frappe-bench"

echo ""
echo "=== [1/3] Building React ==="
cd "$FRONTEND"
GENERATE_SOURCEMAP=false CI=false npm run build:dev 2>&1 | tail -15
echo "Build done"

echo ""
echo "=== [2/3] Deploying build to $BUILD_DEST ==="
sudo cp -r "$FRONTEND/build/"* "$BUILD_DEST/"
echo "Deployed"

echo ""
echo "=== [3/3] Starting / restarting Frappe ==="
if pm2 list | grep -q frappe-dev; then
  pm2 restart frappe-dev
  echo "Restarted frappe-dev"
else
  pm2 start "honcho start" --name frappe-dev --cwd "$FRAPPE_BENCH"
  echo "Started frappe-dev via honcho"
fi
pm2 save

echo ""
echo "Waiting 15s for Frappe..."
sleep 15
curl -s http://localhost:8000/api/method/frappe.ping | grep -q "pong" \
  && echo "Frappe is up" \
  || echo "WARNING: Frappe ping failed — check: pm2 logs frappe-dev --lines 30"

echo ""
echo "=== Done ==="
