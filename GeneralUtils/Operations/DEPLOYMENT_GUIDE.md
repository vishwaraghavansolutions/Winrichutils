# Winrich Deployment Guide

## Roles

| Role | Who | Responsibility |
|------|-----|----------------|
| **Developer** | Rohith, Poojitha | Build features, test on Test server, merge to develop |
| **Deployment Champion** | Team Lead | Create manifest, deploy to Test, validate, deploy to Prod |

---

## Overall Flow

```
Feature Branch  →  Test Server (SCP)  →  Validate  →  Merge to develop
                                                              ↓
                                               Deployment Champion creates manifest
                                                              ↓
                                                    Deploy to Test (deploy.ps1)
                                                              ↓
                                                    Manual validation on Test
                                                              ↓
                                                    Deploy to Prod (deploy.ps1)
```

---

## Prerequisites — One-time setup (all team members)

### 1. Get SSH keys from Zoho Workspace
The `.pem` key files are stored in the team's Zoho Workspace. Download them to a local `keys` folder of your choice, then update the `$KEYS_ROOT` variable at the top of `deploy.ps1` to point to that folder:

```powershell
$KEYS_ROOT = "C:\<your-path>\keys"   # e.g. C:\Dev\keys
```

Expected key filenames (do not rename them):
- `winwizedev.pem` — Test server
- `winwinzeprod.pem` — Prod server

### 2. Clone the repositories
```bash
git clone git@github.com:WinrichProfessionalServices/winrich-stock-manager-ui.git <YOUR_WORKSPACE_ROOT>\Winrich\winrich-stock-manager-ui
git clone git@github.com:WinrichProfessionalServices/winrich-stock-manager.git <YOUR_WORKSPACE_ROOT>\Winrich\winrich-stock-manager
```

### 3. PowerShell execution policy (run once as Administrator)
```powershell
Set-ExecutionPolicy RemoteSigned
```

---

## Developer Workflow (Rohith / Poojitha)

### Step 1 — Create a feature branch
```bash
git checkout develop
git pull origin develop
git checkout -b feature/<your-feature-name>
```

### Step 2 — Make your changes locally

Edit files in your local repo. Keep track of every file you change — you will need this list when handing off to the Deployment Champion.

### Step 3 — SCP changed files to Test server for validation

For each file you changed, copy it to the Test server:

**Frontend file:**
```powershell
scp -i <YOUR_WORKSPACE_ROOT>\keys\winwizedev.pem `
  <YOUR_WORKSPACE_ROOT>\Winrich\winrich-stock-manager-ui\<relative-path> `
  ubuntu@34.223.1.167:/home/ubuntu/frontend/winrich-stock-manager-ui/<relative-path>
```

**Backend file:**
```powershell
scp -i <YOUR_WORKSPACE_ROOT>\keys\winwizedev.pem `
  <YOUR_WORKSPACE_ROOT>\Winrich\winrich-stock-manager\<relative-path> `
  ubuntu@34.223.1.167:/home/ubuntu/winrich/frappe-bench/apps/stock_portfolio_management/<relative-path>
```

**Example:**
```powershell
scp -i <YOUR_WORKSPACE_ROOT>\keys\winwizedev.pem `
  "<YOUR_WORKSPACE_ROOT>\Winrich\winrich-stock-manager\stock_portfolio_management\api.py" `
  ubuntu@34.223.1.167:/home/ubuntu/winrich/frappe-bench/apps/stock_portfolio_management/stock_portfolio_management/api.py
```

### Step 4 — Restart Frappe on Test (backend changes only)
If you changed backend Python files, restart Frappe to pick up the changes:
```bash
ssh -i <YOUR_WORKSPACE_ROOT>\keys\winwizedev.pem ubuntu@34.223.1.167 "pm2 restart frappe-dev"
```

For frontend changes, the Deployment Champion will run the full build. You can preview by rebuilding manually if needed:
```bash
ssh -i <YOUR_WORKSPACE_ROOT>\keys\winwizedev.pem ubuntu@34.223.1.167 "bash -lc 'cd /home/ubuntu/frontend/winrich-stock-manager-ui && npm run build:prod && sudo cp -r build/* /var/www/prod/'"
```

### Step 5 — Validate your feature on Test

Open the test site and verify your feature works end to end.

### Step 6 — Push your branch to GitHub and raise a PR to develop
```bash
git add <changed files>
git commit -m "short description of change"
git push origin feature/<your-feature-name>
```

Then open a Pull Request on GitHub: `feature/<your-feature-name>` → `develop`.

### Step 7 — Hand off to Deployment Champion

Notify the Deployment Champion with:
- A list of all files you changed (relative paths)
- A brief description of what was changed and why
- Confirmation that you have tested on Test

---

## Deployment Champion Workflow

### Step 1 — Review and merge the PR

Review the developer's PR on GitHub and merge it into `develop`.

### Step 2 — Create or update the manifest

Open (or create) `GeneralUtils/Operations/Deploy_<N>.manifest`. Pick the next available manifest number. Add every file from the merged PR:

```
# Deploy_4.manifest — <brief description>
# Files changed by: Rohith / Poojitha
# Date: YYYY-MM-DD

winrich-stock-manager-ui:src/Components/Billing/billing.js
winrich-stock-manager:stock_portfolio_management/api.py
winrich-stock-manager:stock_portfolio_management/portfolio_master/doctype/invoice_email_history/api.py
```

**Format rules:**
- Project is either `winrich-stock-manager-ui` or `winrich-stock-manager`
- Use forward slashes in paths
- Lines starting with `#` are comments

### Step 3 — Deploy to Test

```powershell
cd C:\MyWorkSpace\Ourlife\Apps\Python\Winrichutils\GeneralUtils\Operations

.\deploy.ps1 -Env Test -Manifest <N> -Step Copy
.\deploy.ps1 -Env Test -Manifest <N> -Step Deploy
```

The Copy step pulls only the manifest files from `develop` on GitHub onto the Test server.
The Deploy step builds the React app (if UI files changed) and restarts Frappe.

### Step 4 — Validate on Test

Open the test site. Verify:
- The new feature works as expected
- Existing features are not broken
- No errors in the browser console

### Step 5 — Deploy to Prod (only after Test is validated)

```powershell
.\deploy.ps1 -Env Prod -Manifest <N> -Step Copy
.\deploy.ps1 -Env Prod -Manifest <N> -Step Deploy
```

### Step 6 — Validate on Prod

Open https://advisory.winwizeresearch.in and confirm the changes are live and working.

---

## Server Details

| Environment | Server IP | Key file |
|-------------|-----------|----------|
| Test | ubuntu@34.223.1.167 | winwizedev.pem |
| Prod | ubuntu@52.27.234.146 | winwinzeprod.pem |

Remote paths on both servers:
| Project | Remote path |
|---------|-------------|
| Frontend | `/home/ubuntu/frontend/winrich-stock-manager-ui/` |
| Backend | `/home/ubuntu/winrich/frappe-bench/apps/stock_portfolio_management/` |

---

## Manifest Numbering

Each deployment batch gets its own manifest number. When multiple developers hand off changes at the same time, the Deployment Champion can combine them into one manifest or use separate manifests and deploy them in sequence.

| Manifest | Use |
|----------|-----|
| Deploy_1.manifest | First batch |
| Deploy_2.manifest | Second batch |
| Deploy_N.manifest | ... |

---

## Troubleshooting

### Site not reachable after Deploy
```bash
ssh -i <YOUR_WORKSPACE_ROOT>\keys\winwizedev.pem ubuntu@34.223.1.167 "pm2 logs --lines 30 --nostream"
```
If Frappe is not running: `pm2 restart all`

### "SSH failed" during Copy step
Confirm the server's SSH key is registered with GitHub:
```bash
ssh -i <key.pem> ubuntu@<server-ip> "ssh-keyscan github.com >> ~/.ssh/known_hosts"
```

### Build fails with "Missing script: build"
Always use `npm run build:prod`, not `npm run build`.

### Git fetch fails with HTTPS credential error
The server remote must use SSH:
```bash
git remote set-url origin git@github.com:WinrichProfessionalServices/<repo>.git
```

### "watch.1 sass error" in pm2 logs
Harmless. Only `web.1` needs to be running for the site to work.

---

## Quick Reference

```powershell
# Developer — SCP a backend file to Test
scp -i <YOUR_WORKSPACE_ROOT>\keys\winwizedev.pem <local-path> ubuntu@34.223.1.167:<remote-path>

# Developer — Restart Frappe on Test after backend change
ssh -i <YOUR_WORKSPACE_ROOT>\keys\winwizedev.pem ubuntu@34.223.1.167 "pm2 restart frappe-dev"

# Deployment Champion — Deploy to Test
.\deploy.ps1 -Env Test -Manifest <N> -Step Copy
.\deploy.ps1 -Env Test -Manifest <N> -Step Deploy

# Deployment Champion — Deploy to Prod
.\deploy.ps1 -Env Prod -Manifest <N> -Step Copy
.\deploy.ps1 -Env Prod -Manifest <N> -Step Deploy
```
