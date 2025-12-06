# 🎯 Risk Tool

> Detect open-source maintainer risk before it becomes your problem.

A CLI tool that analyzes GitHub repositories for **bus factor risk** — identifying projects that are dangerously dependent on a small number of contributors.

![Risk Explorer TUI](https://img.shields.io/badge/TUI-k9s%20style-blue?style=flat-square)
![Python](https://img.shields.io/badge/python-3.13+-green?style=flat-square)
![License](https://img.shields.io/badge/license-MIT-yellow?style=flat-square)

## ✨ Features

- **Multi-Registry Scanning** — NPM, PyPI, and Maven package registries
- **GitHub Repository Scanning** — Search and analyze repos by org, language, stars, or custom queries
- **Risk Metrics** — Gini coefficient, velocity ratio, top contributor concentration
- **Interactive TUI** — k9s-style terminal explorer with filtering, sorting, and details
- **Command Mode** — Filter by registry with `:npm`, `:pypi`, `:maven` commands
- **Parallel Scanning** — Batch scan thousands of repos across multiple registries
- **SQLite Storage** — Persistent database with automatic deduplication and updates
- **Rich Output** — Color-coded risk levels with clickable GitHub links

## 🚀 Quick Start

### Installation

```bash
# Clone the repo
git clone https://github.com/CKristensen/open-source-maintainer-risk
cd attensi-risk-tool

# Install with uv (recommended)
uv sync
```

### Quick setup
```bash
sh parallel_scan.sh
uv run risk-tool explore
```

### Usage

#### 1. Scan repositories

```bash
# Option A: Set your GitHub token as an environment variable
export GITHUB_TOKEN=ghp_your_token_here

# Option B: Pass token directly (overrides env var)
uv run risk-tool scan --token ghp_your_token --query "org:facebook" --limit 50

# Scan repositories matching a query
uv run risk-tool scan --query "org:facebook" --limit 50

# Scan a specific org
uv run risk-tool scan --query "org:vercel" --limit 100

# Scan popular repos
uv run risk-tool scan --query "stars:>10000" --limit 20
```

#### 1b. Scan NPM packages

```bash
# Scan top 1000 NPM packages (uses weekly cache by default)
uv run risk-tool scan-npm --limit 1000

# Scan with minimum download filter
uv run risk-tool scan-npm --limit 500 --min-downloads 100000

# Force fresh data (skip cache)
uv run risk-tool scan-npm --limit 100 --no-cache
```

#### 1c. Scan PyPI packages

```bash
# Scan top 1000 PyPI packages (uses weekly cache by default)
uv run risk-tool scan-pypi --limit 1000

# Scan with minimum download filter (monthly downloads)
uv run risk-tool scan-pypi --limit 500 --min-downloads 500000

# Force fresh data (skip cache)
uv run risk-tool scan-pypi --limit 100 --no-cache
```

#### 1d. Scan Maven packages

```bash
# Requires Libraries.io API key (free at https://libraries.io/api)
export LIBRARIES_IO_API_KEY=your_key_here

# Scan top 500 Maven packages (uses weekly cache by default)
uv run risk-tool scan-maven --limit 500

# Scan with minimum dependents filter
uv run risk-tool scan-maven --limit 500 --min-dependents 1000

# Force fresh data (skip cache)
uv run risk-tool scan-maven --limit 100 --no-cache
```

> **Note**: Maven Central doesn't expose download statistics, so we use `dependents_count` from Libraries.io as a popularity metric.

#### 1e. Scan all registries (recommended)

```bash
# Set required tokens
export GITHUB_TOKEN=ghp_your_token_here
export LIBRARIES_IO_API_KEY=your_key_here  # Optional, for Maven

# Use the convenience script to scan all registries in parallel
./parallel_scan.sh

# Or scan them separately
uv run risk-tool scan-npm --limit 1500 --min-downloads 100000
uv run risk-tool scan-pypi --limit 1500 --min-downloads 500000
uv run risk-tool scan-maven --limit 500 --min-dependents 1000
```

#### 2. Explore results

```bash
# Launch the interactive TUI explorer
uv run risk-tool explore
```

## 🖥️ Explorer TUI

A k9s-style terminal UI for exploring your risk database:
<img width="3069" height="2244" alt="image" src="https://github.com/user-attachments/assets/5130edc9-5ab1-4725-accf-7719afc7826f" />



### Keybindings

| Key | Action |
|-----|--------|
| `?` | Show help |
| `j` / `↓` | Move down |
| `k` / `↑` | Move up |
| `g` | Jump to top |
| `G` | Jump to bottom |
| `/` | Focus search |
| `:` | Command mode |
| `Escape` | Clear search & filters |
| `d` | Toggle detail panel |
| `s` | Sort by risk score |
| `c` | Sort by contributors |
| `n` | Sort by name |
| `w` | Sort by downloads |
| `r` | Refresh data |
| `o` | Open repo in browser |
| `q` | Quit |

### Command Mode (k9s style)

Press `:` to enter command mode and filter by registry:

| Command | Action |
|---------|--------|
| `:npm` | Show only NPM packages |
| `:pypi` | Show only PyPI packages |
| `:maven` | Show only Maven packages |
| `:all` | Show all packages |

## 📊 Risk Metrics

### Risk Score (0-10)

Combined score based on multiple factors. Higher = riskier.

| Level | Score | Meaning |
|-------|-------|--------|
| 🔴 CRITICAL | 8-10 | Immediate attention needed |
| 🟠 HIGH | 6-8 | Significant risk |
| 🟡 MEDIUM | 4-6 | Moderate concern |
| 🟢 LOW | 0-4 | Healthy project |

### Velocity Ratio

Recent commits (13 weeks) vs older commits (13 weeks).

- `> 1.0x` — Growing activity ✅
- `< 1.0x` — Declining activity ⚠️
- `< 0.25x` — Severely declining 🚨

### Gini Coefficient

Measures contribution inequality (like wealth inequality).

- `0.0` — Perfect equality (all contributors equal)
- `1.0` — Perfect inequality (one person does everything)
- `> 0.75` — High concentration risk 🚨

### Top 1% / Top 3%

Percentage of commits by top contributors.

- **Top 1 > 50%** — Single point of failure 🚨
- **Top 3 > 80%** — Bus factor of 3 or less ⚠️

## 🔧 Configuration

### GitHub Token

You need a GitHub Personal Access Token (PAT) with `public_repo` scope:

1. Go to [GitHub Settings → Tokens](https://github.com/settings/tokens)
2. Generate new token (classic)
3. Select `public_repo` scope
4. Copy and use via environment variable or CLI flag:

```bash
# Recommended: Set as environment variable (no --token needed)
export GITHUB_TOKEN=ghp_your_token_here
uv run risk-tool scan --query "org:vercel" --limit 50

# Alternative: Pass directly via --token flag
uv run risk-tool scan --token ghp_your_token_here --query "org:vercel" --limit 50
```

### Database

Results are stored in `risk_report.db` (SQLite). The database:

- Automatically appends new repos
- Updates existing repos on rescan
- Tracks `updated_at` timestamps

Query directly:

```bash
sqlite3 risk_report.db "SELECT repo, risk_level FROM risk_report WHERE risk_level='CRITICAL'"
```

## 🏗️ Architecture

```
src/
├── cli.py              # Typer CLI commands (scan, scan-npm, scan-pypi, scan-maven, explore)
├── ingestion.py        # Async GitHub API client
├── registry_clients.py # NPM, PyPI & Maven Registry clients with caching
├── processing.py       # Risk metric calculations (Gini, etc.)
└── explorer.py         # Textual TUI application
```

## ⚠️ Limitations

### NPM Package Scanning
- **GitHub-only**: Packages hosted on GitLab, Bitbucket, or other platforms are skipped
- **Repository detection**: Some packages have malformed or missing repository URLs
- **Download counts**: Weekly downloads may be delayed by ~24 hours

### PyPI Package Scanning
- **GitHub-only**: Packages hosted on GitLab, Bitbucket, or other platforms are skipped
- **Repository detection**: Relies on package metadata (`project_urls`, `home_page`)
- **Download counts**: Uses 30-day download statistics from top-pypi-packages dataset
- **Data source**: Uses [hugovk/top-pypi-packages](https://hugovk.github.io/top-pypi-packages/) for popularity data

### Maven Package Scanning
- **Requires API key**: Libraries.io API key needed (free at https://libraries.io/api)
- **GitHub-only**: Packages hosted on GitLab, Bitbucket, or other platforms are skipped
- **No download counts**: Maven Central doesn't expose download stats; uses `dependents_count` instead
- **Rate limiting**: Libraries.io free tier allows 60 requests/minute
- **POM fallback**: For packages without repository URL in Libraries.io, attempts to parse POM files

### General
- **GitHub API rate limits**: 5,000 requests/hour with authentication
- **Statistics computation**: GitHub may return 202 (pending) for recently active repos
- **Contributor data**: Some repos may not have contributor statistics available

## 📦 Dependencies

- **typer** — CLI framework
- **httpx** — Async HTTP client
- **polars** — Fast DataFrames
- **textual** — TUI framework
- **rich** — Terminal formatting

## 🤝 Contributing

PRs welcome! Some ideas:

- [ ] Add more risk signals (issues, PRs, releases)
- [ ] Export to CSV/JSON
- [ ] GitHub Actions integration
- [ ] Slack/Discord alerts
- [ ] Historical trend tracking

## 👤 Author

**Carl Kristensen** — [ghe@cjckris.com](mailto:ghe@cjckris.com)

## 📄 License

MIT
