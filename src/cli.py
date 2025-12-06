import typer
import asyncio
import os
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich import print
from src.ingestion import GitHubClient
from src.processing import compute_risk_metrics
from src.explorer import run_explorer
from src.npm_client import NPMClient

app = typer.Typer()
console = Console()


@app.command()
def explore(
    db: str = typer.Option("risk_report.db", help="Path to the SQLite database")
):
    """
    Interactive TUI to explore the risk database (k9s-style).
    
    Keybindings:
      /        - Focus search
      Escape   - Clear search
      d        - Toggle detail panel
      s        - Sort by risk score
      c        - Sort by contributors
      n        - Sort by name
      r        - Refresh data
      q        - Quit
    """
    if not os.path.exists(db):
        console.print(f"[red]Database not found: {db}[/red]")
        console.print("[dim]Run 'risk-tool scan' first to populate the database.[/dim]")
        raise typer.Exit(1)
    
    run_explorer(db)


@app.command()
def scan(
    token: str = typer.Option(..., envvar="GITHUB_TOKEN", help="GitHub PAT"),
    limit: int = typer.Option(100, help="Number of repos to scan (for demo)"),
    query: str = typer.Option("stars:>1000", help="GitHub search query for repositories")
):
    """
    Scans repositories for maintainer risk.
    """
    asyncio.run(_scan_async(token, limit, query))


async def _scan_async(token: str, limit: int, query: str):
    """Async implementation of the scan command."""
    # 1. Cohort Selection
    console.print(f"[bold blue]Fetching repositories matching: {query}...[/bold blue]")
    
    client = GitHubClient(token)
    
    try:
        repo_list = await client.search_repositories(query=query, max_results=limit)
        
        if not repo_list:
            console.print("[red]No repositories found. Check your query or token permissions.[/red]")
            return
        
        console.print(f"[bold blue]Starting Risk Scan for {len(repo_list)} repositories...[/bold blue]")

        # 2. Ingestion
        results = await client.fetch_batch(repo_list)
    finally:
        await client.close()
    
    # 3. Processing
    df = compute_risk_metrics(results)
    
    if df.is_empty():
        console.print("[red]No valid data to process.[/red]")
        return
    
    # 4. Visualization (The Dashboard)
    table = Table(title="Open Source Maintainer Risk Report")
    
    table.add_column("Repository", style="cyan", no_wrap=True)
    table.add_column("Lang", justify="left")
    table.add_column("Risk", justify="right")
    table.add_column("Level", justify="center")
    table.add_column("Velocity", justify="right")
    table.add_column("Gini", justify="right")
    table.add_column("Top 1", justify="right")
    table.add_column("Top 3", justify="right")
    table.add_column("Contribs", justify="right")
    table.add_column("Commits (1Y)", justify="right")
    table.add_column("Recent (3M)", justify="right")
    
    # Take top 20 riskiest
    top_risk = df.head(20)
    
    for row in top_risk.iter_rows(named=True):
        # Color coding the output based on risk level
        level_color = "green"
        if row["risk_level"] == "CRITICAL":
            level_color = "red bold"
        elif row["risk_level"] == "HIGH":
            level_color = "orange1"
        elif row["risk_level"] == "MEDIUM":
            level_color = "yellow"
        
        # Check if we have valid contributor data (use the explicit flag or check for None values)
        has_contributor_data = row.get("contributor_data_available", False) or (
            row["contributor_count"] is not None and 
            row["gini_coefficient"] is not None
        )
        
        # Color code Gini (higher = more concentrated = riskier)
        if has_contributor_data and row["gini_coefficient"] is not None:
            gini = row["gini_coefficient"]
            gini_color = "green" if gini < 0.5 else "yellow" if gini < 0.75 else "red"
            gini_str = f"[{gini_color}]{gini:.2f}[/{gini_color}]"
            top1_str = f"{row['top1_share']:.0%}" if row['top1_share'] is not None else "[dim]N/A[/dim]"
            top3_str = f"{row['top3_share']:.0%}" if row['top3_share'] is not None else "[dim]N/A[/dim]"
            contrib_str = str(row["contributor_count"]) if row["contributor_count"] is not None else "[dim]?[/dim]"
        else:
            gini_str = "[dim]N/A[/dim]"
            top1_str = "[dim]N/A[/dim]"
            top3_str = "[dim]N/A[/dim]"
            contrib_str = "[dim]?[/dim]"
        
        # Create clickable link to GitHub repo
        repo_name = row["repo"]
        repo_url = f"https://github.com/{repo_name}"
        repo_link = f"[link={repo_url}]{repo_name}[/link]"
            
        table.add_row(
            repo_link,
            row.get("language", "?"),
            f"{row['total_risk_score']:.1f}",
            f"[{level_color}]{row['risk_level']}[/{level_color}]",
            f"{row['velocity_ratio']:.2f}x",
            gini_str,
            top1_str,
            top3_str,
            contrib_str,
            str(row.get("total_commits", "?")),
            str(row.get("recent_commits", "?")),
        )
        
    console.print(table)
    
    # Legend
    console.print("\n[bold]Legend:[/bold]")
    console.print("[dim]• Commits (1Y): Total commits in the last 52 weeks (1 year)[/dim]")
    console.print("[dim]• Recent (3M): Commits in the last 13 weeks (~3 months)[/dim]")
    console.print("[dim]• Velocity: Recent (13 wks) vs older (13 wks) commits. >1x = growing, <1x = declining[/dim]")
    console.print("[dim]• Gini: Contribution inequality (0 = equal, 1 = one person). >0.75 = high concentration[/dim]")
    console.print("[dim]• Top 1/3: % of commits by top contributors. >50% (top1) or >80% (top3) = bus factor risk[/dim]")
    console.print("[dim]• Contribs: Total unique contributors. More = lower bus factor risk[/dim]")
    console.print("[dim]• N/A: GitHub stats still computing or unavailable (retry later)[/dim]")
    
    # 5. Export to SQLite (append mode, skip duplicates by repo name)
    import sqlite3
    output_path = "risk_report.db"
    
    # Use WAL mode for better concurrent access (multiple scans can run in parallel)
    conn = sqlite3.connect(output_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")  # Wait up to 30s for locks
    
    # Use INSERT OR REPLACE to update existing repos (repo as unique key)
    pdf = df.to_pandas()
    pdf["updated_at"] = pd.Timestamp.now()
    
    # Retry logic for concurrent writes
    max_retries = 5
    for attempt in range(max_retries):
        try:
            pdf.to_sql("risk_report_temp", conn, if_exists="replace", index=False)
            
            # Create main table from temp if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS risk_report AS 
                SELECT * FROM risk_report_temp WHERE 0
            """)
            
            # Add unique index on repo if not exists
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_repo ON risk_report(repo)")
            
            # Insert or replace
            cols = ", ".join(pdf.columns)
            conn.execute(f"""
                INSERT OR REPLACE INTO risk_report ({cols})
                SELECT {cols} FROM risk_report_temp
            """)
            conn.execute("DROP TABLE risk_report_temp")
            conn.commit()
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_retries - 1:
                import time
                console.print(f"[yellow]Database busy, retrying ({attempt + 1}/{max_retries})...[/yellow]")
                time.sleep(1.0 * (attempt + 1))  # Exponential backoff
            else:
                raise
    
    conn.close()
    console.print(f"\n[dim]Full dataset saved to {output_path} (table: risk_report)[/dim]")


@app.command("scan-npm")
def scan_npm(
    token: str = typer.Option(..., envvar="GITHUB_TOKEN", help="GitHub PAT"),
    limit: int = typer.Option(1000, help="Number of top NPM packages to scan"),
    min_downloads: int = typer.Option(10000, help="Minimum weekly downloads filter"),
    no_cache: bool = typer.Option(False, help="Skip cache and fetch fresh data from NPM"),
):
    """
    Scan top NPM packages for maintainer risk.
    
    Fetches the most popular NPM packages by downloads, maps them to
    their GitHub repositories, and analyzes maintainer risk.
    
    Note: Packages without a GitHub repository are skipped.
    """
    asyncio.run(_scan_npm_async(token, limit, min_downloads, use_cache=not no_cache))


async def _scan_npm_async(token: str, limit: int, min_downloads: int, use_cache: bool = True):
    """Async implementation of the scan-npm command."""
    npm_client = NPMClient()
    github_client = GitHubClient(token)
    
    try:
        # 1. Fetch popular NPM packages
        packages = await npm_client.search_popular_packages(
            max_results=limit,
            use_cache=use_cache
        )
        
        if not packages:
            console.print("[red]No NPM packages found.[/red]")
            return
        
        # 2. Filter to GitHub-hosted packages
        filtered_packages, skipped_count = npm_client.filter_github_packages(
            packages, 
            min_downloads=min_downloads
        )
        
        if skipped_count > 0:
            console.print(f"[yellow]Skipped {skipped_count} packages (no GitHub repo or below {min_downloads:,} weekly downloads)[/yellow]")
        
        if not filtered_packages:
            console.print("[red]No packages with GitHub repositories found.[/red]")
            return
        
        # 3. Convert to repo list format for GitHubClient
        repo_list = npm_client.to_repo_list(filtered_packages)
        console.print(f"[bold blue]Scanning {len(repo_list)} NPM packages (from {len(packages)} total)...[/bold blue]")
        
        # Create a mapping of repo -> package info for enrichment
        repo_to_package = {
            r["name"]: {
                "package_name": r.get("package_name"),
                "weekly_downloads": r.get("weekly_downloads", 0),
                "registry": r.get("registry", "npm"),
            }
            for r in repo_list
        }
        
        # 4. Fetch GitHub stats
        results = await github_client.fetch_batch(repo_list)
        
        # 5. Enrich results with NPM package info
        for result in results:
            repo_name = result.get("repo")
            if repo_name in repo_to_package:
                result["package_name"] = repo_to_package[repo_name]["package_name"]
                result["weekly_downloads"] = repo_to_package[repo_name]["weekly_downloads"]
                result["registry"] = repo_to_package[repo_name]["registry"]
        
    finally:
        await npm_client.close()
        await github_client.close()
    
    # 6. Process and compute risk metrics
    df = compute_risk_metrics(results)
    
    if df.is_empty():
        console.print("[red]No valid data to process.[/red]")
        return
    
    # 7. Display results
    _display_npm_results(df)
    
    # 8. Export to SQLite
    _export_to_sqlite(df, "risk_report.db")


def _display_npm_results(df):
    """Display NPM scan results in a rich table."""
    table = Table(title="NPM Package Maintainer Risk Report")
    
    table.add_column("Package", style="cyan", no_wrap=True)
    table.add_column("Repository", style="dim")
    table.add_column("Downloads/wk", justify="right")
    table.add_column("Risk", justify="right")
    table.add_column("Level", justify="center")
    table.add_column("Velocity", justify="right")
    table.add_column("Gini", justify="right")
    table.add_column("Top 1", justify="right")
    table.add_column("Contribs", justify="right")
    
    # Take top 30 riskiest
    top_risk = df.head(30)
    
    for row in top_risk.iter_rows(named=True):
        # Color coding the output based on risk level
        level_color = "green"
        if row["risk_level"] == "CRITICAL":
            level_color = "red bold"
        elif row["risk_level"] == "HIGH":
            level_color = "orange1"
        elif row["risk_level"] == "MEDIUM":
            level_color = "yellow"
        
        # Check if we have valid contributor data
        has_contributor_data = row.get("contributor_data_available", False) or (
            row["contributor_count"] is not None and 
            row["gini_coefficient"] is not None
        )
        
        # Format Gini
        if has_contributor_data and row["gini_coefficient"] is not None:
            gini = row["gini_coefficient"]
            gini_color = "green" if gini < 0.5 else "yellow" if gini < 0.75 else "red"
            gini_str = f"[{gini_color}]{gini:.2f}[/{gini_color}]"
            top1_str = f"{row['top1_share']:.0%}" if row['top1_share'] is not None else "[dim]N/A[/dim]"
            contrib_str = str(row["contributor_count"]) if row["contributor_count"] is not None else "[dim]?[/dim]"
        else:
            gini_str = "[dim]N/A[/dim]"
            top1_str = "[dim]N/A[/dim]"
            contrib_str = "[dim]?[/dim]"
        
        # Package name and repo link
        package_name = row.get("package_name", "")
        repo_name = row["repo"]
        repo_url = f"https://github.com/{repo_name}"
        repo_link = f"[link={repo_url}]{repo_name}[/link]"
        
        # Format downloads
        downloads = row.get("weekly_downloads", 0)
        if downloads >= 1_000_000:
            dl_str = f"{downloads / 1_000_000:.1f}M"
        elif downloads >= 1_000:
            dl_str = f"{downloads / 1_000:.0f}K"
        else:
            dl_str = str(downloads)
        
        table.add_row(
            package_name or "[dim]?[/dim]",
            repo_link,
            dl_str,
            f"{row['total_risk_score']:.1f}",
            f"[{level_color}]{row['risk_level']}[/{level_color}]",
            f"{row['velocity_ratio']:.2f}x",
            gini_str,
            top1_str,
            contrib_str,
        )
    
    console.print(table)
    
    # Legend
    console.print("\n[bold]Legend:[/bold]")
    console.print("[dim]• Downloads/wk: Weekly NPM downloads (higher = more critical if risky)[/dim]")
    console.print("[dim]• Velocity: Recent (13 wks) vs older (13 wks) commits. >1x = growing, <1x = declining[/dim]")
    console.print("[dim]• Gini: Contribution inequality (0 = equal, 1 = one person). >0.75 = high concentration[/dim]")
    console.print("[dim]• Top 1: % of commits by top contributor. >50% = bus factor risk[/dim]")


def _export_to_sqlite(df, output_path: str):
    """Export DataFrame to SQLite with upsert logic."""
    import sqlite3
    
    # Use WAL mode for better concurrent access
    conn = sqlite3.connect(output_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    
    pdf = df.to_pandas()
    pdf["updated_at"] = pd.Timestamp.now()
    
    # Retry logic for concurrent writes
    max_retries = 5
    for attempt in range(max_retries):
        try:
            pdf.to_sql("risk_report_temp", conn, if_exists="replace", index=False)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS risk_report AS 
                SELECT * FROM risk_report_temp WHERE 0
            """)
            
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_repo ON risk_report(repo)")
            
            cols = ", ".join(pdf.columns)
            conn.execute(f"""
                INSERT OR REPLACE INTO risk_report ({cols})
                SELECT {cols} FROM risk_report_temp
            """)
            conn.execute("DROP TABLE risk_report_temp")
            conn.commit()
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_retries - 1:
                import time
                console.print(f"[yellow]Database busy, retrying ({attempt + 1}/{max_retries})...[/yellow]")
                time.sleep(1.0 * (attempt + 1))
            else:
                raise
    
    conn.close()
    console.print(f"\n[dim]Full dataset saved to {output_path} (table: risk_report)[/dim]")


if __name__ == "__main__":
    app()
