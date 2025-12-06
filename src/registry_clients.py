"""
Package Registry Clients for fetching popular packages and mapping to GitHub repos.

This module provides async clients for:
- NPM Registry (JavaScript/TypeScript packages)
- PyPI (Python packages)
- Maven Central via Libraries.io (Java/Kotlin packages)

Common functionality:
- Search for most popular packages by download/dependents count
- Extract GitHub repository URLs from package metadata
- Cache results to minimize API calls (weekly refresh)
"""

import asyncio
import httpx
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from rich.console import Console
from rich.progress import Progress, TaskID

console = Console()

# Cache configuration
DEFAULT_CACHE_DIR = Path.home() / ".cache" / "risk-tool"
CACHE_TTL_DAYS = 7  # Weekly cache refresh


class PackageRegistryClient:
    """
    Base class for package registry clients (NPM, PyPI, Maven, etc.).
    Provides common functionality for caching and GitHub repo extraction.
    """
    
    def __init__(self, concurrency: int = 10, cache_dir: Optional[Path] = None):
        self.sem = asyncio.Semaphore(concurrency)
        self.timeout = 30.0
        self.client: Optional[httpx.AsyncClient] = None
        self.cache_dir = cache_dir or DEFAULT_CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Lazily create the HTTP client."""
        if self.client is None:
            self.client = httpx.AsyncClient(timeout=self.timeout)
        return self.client
    
    async def close(self):
        """Close the HTTP client."""
        if self.client is not None:
            await self.client.aclose()
            self.client = None
    
    def _get_cache_path(self, cache_key: str) -> Path:
        """Get the cache file path for a given key."""
        return self.cache_dir / f"{cache_key}.json"
    
    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cache exists and is within TTL."""
        if not cache_path.exists():
            return False
        
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        return datetime.now() - mtime < timedelta(days=CACHE_TTL_DAYS)
    
    def _load_cache(self, cache_key: str) -> Optional[List[Dict[str, Any]]]:
        """Load data from cache if valid."""
        cache_path = self._get_cache_path(cache_key)
        if self._is_cache_valid(cache_path):
            try:
                with open(cache_path, "r") as f:
                    data = json.load(f)
                    console.print(f"[dim]Loaded {len(data)} packages from cache ({cache_path})[/dim]")
                    return data
            except (json.JSONDecodeError, IOError):
                pass
        return None
    
    def _save_cache(self, cache_key: str, data: List[Dict[str, Any]]):
        """Save data to cache."""
        cache_path = self._get_cache_path(cache_key)
        try:
            with open(cache_path, "w") as f:
                json.dump(data, f)
            console.print(f"[dim]Cached {len(data)} packages to {cache_path}[/dim]")
        except IOError as e:
            console.print(f"[yellow]Warning: Could not save cache: {e}[/yellow]")
    
    @staticmethod
    def parse_github_url(repo_url: str) -> Optional[str]:
        """
        Extract owner/repo from various GitHub URL formats.
        
        Handles:
        - https://github.com/owner/repo
        - git+https://github.com/owner/repo.git
        - git://github.com/owner/repo.git
        - git@github.com:owner/repo.git
        - github:owner/repo
        """
        if not repo_url:
            return None
        
        # Normalize the URL
        url = repo_url.strip()
        
        # Handle shorthand github:owner/repo
        if url.startswith("github:"):
            url = url[7:]
            if "/" in url:
                return url.split("/")[0] + "/" + url.split("/")[1].replace(".git", "")
        
        # Pattern to match GitHub URLs
        patterns = [
            r"github\.com[:/]([^/]+)/([^/\s#?.]+)",  # Standard URLs
            r"^([^/]+)/([^/\s#?.]+)$",  # Simple owner/repo format
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                owner, repo = match.groups()
                # Clean up .git suffix
                repo = repo.replace(".git", "")
                return f"{owner}/{repo}"
        
        return None


class NPMClient(PackageRegistryClient):
    """
    Async client for NPM Registry API.
    
    Fetches popular packages and maps them to GitHub repositories.
    Uses weekly caching to minimize API calls.
    """
    
    REGISTRY_URL = "https://registry.npmjs.org"
    SEARCH_URL = "https://registry.npmjs.org/-/v1/search"
    DOWNLOADS_URL = "https://api.npmjs.org/downloads"
    
    def __init__(self, concurrency: int = 10, cache_dir: Optional[Path] = None):
        super().__init__(concurrency, cache_dir)
        self.registry_name = "npm"
        self.page_size = 250  # NPM API max per request
    
    async def search_popular_packages(
        self, 
        max_results: int = 1000,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Fetch top NPM packages sorted by popularity.
        
        Args:
            max_results: Maximum number of packages to fetch
            use_cache: Whether to use cached results (weekly refresh)
        
        Returns:
            List of package dicts with name, downloads, github_repo
        """
        cache_key = f"npm_popular_{max_results}"
        
        # Check cache first
        if use_cache:
            cached = self._load_cache(cache_key)
            if cached:
                return cached
        
        console.print(f"[bold blue]Fetching top {max_results} NPM packages...[/bold blue]")
        
        packages = []
        seen_packages = set()  # Dedupe across search terms
        
        # NPM API requires text parameter - search multiple broad terms to get diverse packages
        # These are common keywords/patterns in JavaScript packages
        search_terms = [
            "keywords:javascript",
            "keywords:typescript", 
            "keywords:nodejs",
            "keywords:react",
            "keywords:vue",
            "keywords:util",
            "keywords:cli",
            "keywords:library",
        ]
        
        client = await self._get_client()
        packages_per_term = max(250, max_results // len(search_terms) + 100)  # Over-fetch to account for dedupes
        
        with Progress() as progress:
            task = progress.add_task("[cyan]Searching NPM registry...", total=max_results)
            
            for search_term in search_terms:
                if len(packages) >= max_results:
                    break
                    
                from_offset = 0
                term_packages = 0
                
                while term_packages < packages_per_term and len(packages) < max_results:
                    params = {
                        "text": search_term,
                        "size": min(self.page_size, packages_per_term - term_packages),
                        "from": from_offset,
                        "popularity": 1.0,  # Sort by popularity
                        "quality": 0.0,
                        "maintenance": 0.0,
                    }
                    
                    async with self.sem:
                        try:
                            response = await client.get(self.SEARCH_URL, params=params)
                            
                            if response.status_code == 200:
                                data = response.json()
                                objects = data.get("objects", [])
                                
                                if not objects:
                                    break
                                
                                for obj in objects:
                                    pkg = obj.get("package", {})
                                    pkg_name = pkg.get("name")
                                    
                                    # Skip if already seen
                                    if pkg_name in seen_packages:
                                        continue
                                    seen_packages.add(pkg_name)
                                    
                                    # Get downloads from response (new NPM API includes it)
                                    downloads_info = obj.get("downloads", {})
                                    weekly_downloads = downloads_info.get("weekly", 0)
                                    
                                    packages.append({
                                        "name": pkg_name,
                                        "version": pkg.get("version"),
                                        "description": pkg.get("description", ""),
                                        "keywords": pkg.get("keywords", []),
                                        "repository": pkg.get("links", {}).get("repository"),
                                        "npm_url": pkg.get("links", {}).get("npm"),
                                        "score": obj.get("score", {}).get("final", 0),
                                        "popularity_score": obj.get("score", {}).get("detail", {}).get("popularity", 0),
                                        "weekly_downloads": weekly_downloads,
                                    })
                                    term_packages += 1
                                
                                progress.update(task, completed=min(len(packages), max_results))
                                from_offset += self.page_size
                                
                                # Rate limiting: small delay between requests
                                await asyncio.sleep(0.1)
                            else:
                                console.print(f"[yellow]NPM API returned {response.status_code} for '{search_term}'[/yellow]")
                                break
                        except httpx.RequestError as e:
                            console.print(f"[red]NPM API error: {e}[/red]")
                            break
        
        # Sort by popularity (weekly downloads) and trim to max_results
        packages.sort(key=lambda x: x.get("weekly_downloads", 0), reverse=True)
        packages = packages[:max_results]
        
        console.print(f"[green]Found {len(packages)} unique packages[/green]")
        
        # Resolve GitHub repos from package metadata
        packages = await self._resolve_github_repos(packages)
        
        # Save to cache
        if use_cache:
            self._save_cache(cache_key, packages)
        
        return packages
    
    async def _resolve_github_repos(self, packages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Resolve GitHub repository URLs from package metadata.
        Fetches full package info for packages without repository links.
        """
        client = await self._get_client()
        
        packages_needing_lookup = []
        for i, pkg in enumerate(packages):
            repo_url = pkg.get("repository")
            if repo_url:
                github_repo = self.parse_github_url(repo_url)
                packages[i]["github_repo"] = github_repo
            else:
                packages_needing_lookup.append(i)
        
        # Fetch full metadata for packages without repository in search results
        if packages_needing_lookup:
            with Progress() as progress:
                task = progress.add_task(
                    "[cyan]Resolving GitHub repos...", 
                    total=len(packages_needing_lookup)
                )
                
                for idx in packages_needing_lookup:
                    pkg_name = packages[idx].get("name")
                    if not pkg_name:
                        continue
                    
                    async with self.sem:
                        try:
                            url = f"{self.REGISTRY_URL}/{pkg_name}"
                            response = await client.get(url)
                            
                            if response.status_code == 200:
                                data = response.json()
                                repo = data.get("repository", {})
                                
                                if isinstance(repo, dict):
                                    repo_url = repo.get("url", "")
                                elif isinstance(repo, str):
                                    repo_url = repo
                                else:
                                    repo_url = ""
                                
                                packages[idx]["github_repo"] = self.parse_github_url(repo_url)
                            
                            progress.update(task, advance=1)
                            await asyncio.sleep(0.02)
                            
                        except httpx.RequestError:
                            pass
        
        return packages
    
    def filter_github_packages(
        self, 
        packages: List[Dict[str, Any]],
        min_downloads: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Filter packages to only those with GitHub repos and optional minimum downloads.
        
        Returns:
            Tuple of (filtered packages, count of skipped packages)
        """
        filtered = []
        skipped = 0
        
        for pkg in packages:
            if not pkg.get("github_repo"):
                skipped += 1
                continue
            
            if pkg.get("weekly_downloads", 0) < min_downloads:
                skipped += 1
                continue
            
            filtered.append(pkg)
        
        return filtered, skipped
    
    def to_repo_list(self, packages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convert NPM packages to the format expected by GitHubClient.fetch_batch().
        
        Returns list of dicts with:
        - name: owner/repo
        - language: "JavaScript" (assumed for NPM)
        - package_name: original NPM package name
        - weekly_downloads: download count
        - registry: "npm"
        """
        repo_list = []
        seen_repos = set()  # Deduplicate repos (multiple packages can share a repo)
        
        for pkg in packages:
            github_repo = pkg.get("github_repo")
            if not github_repo or github_repo in seen_repos:
                continue
            
            seen_repos.add(github_repo)
            repo_list.append({
                "name": github_repo,
                "language": "JavaScript",  # Default for NPM packages
                "package_name": pkg.get("name"),
                "weekly_downloads": pkg.get("weekly_downloads", 0),
                "registry": "npm",
            })
        
        return repo_list


class PyPIClient(PackageRegistryClient):
    """
    Async client for PyPI API.
    
    Fetches popular Python packages using the top-pypi-packages dataset
    and maps them to GitHub repositories.
    Uses weekly caching to minimize API calls.
    """
    
    # Uses the excellent top-pypi-packages dataset maintained by hugovk
    TOP_PACKAGES_URL = "https://hugovk.github.io/top-pypi-packages/top-pypi-packages-30-days.min.json"
    PYPI_API_URL = "https://pypi.org/pypi"
    
    def __init__(self, concurrency: int = 10, cache_dir: Optional[Path] = None):
        super().__init__(concurrency, cache_dir)
        self.registry_name = "pypi"
    
    async def search_popular_packages(
        self, 
        max_results: int = 1000,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Fetch top PyPI packages sorted by downloads.
        
        Args:
            max_results: Maximum number of packages to fetch
            use_cache: Whether to use cached results (weekly refresh)
        
        Returns:
            List of package dicts with name, downloads, github_repo
        """
        cache_key = f"pypi_popular_{max_results}"
        
        # Check cache first
        if use_cache:
            cached = self._load_cache(cache_key)
            if cached:
                return cached
        
        console.print(f"[bold blue]Fetching top {max_results} PyPI packages...[/bold blue]")
        
        client = await self._get_client()
        
        # 1. Fetch top packages list from hugovk's dataset
        try:
            response = await client.get(self.TOP_PACKAGES_URL)
            if response.status_code != 200:
                console.print(f"[red]Failed to fetch top packages list: HTTP {response.status_code}[/red]")
                return []
            
            data = response.json()
            rows = data.get("rows", [])[:max_results]
            
        except httpx.RequestError as e:
            console.print(f"[red]Failed to fetch top packages list: {e}[/red]")
            return []
        
        console.print(f"[dim]Found {len(rows)} packages in top packages list[/dim]")
        
        # 2. Fetch detailed info for each package (including repo URL)
        packages = []
        
        with Progress() as progress:
            task = progress.add_task("[cyan]Fetching PyPI package details...", total=len(rows))
            
            # Process in batches to respect rate limits
            batch_size = 50
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                tasks = [
                    self._fetch_package_details(client, pkg["project"], pkg["download_count"])
                    for pkg in batch
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, dict):
                        packages.append(result)
                    progress.update(task, advance=1)
                
                # Small delay between batches to be nice to PyPI
                if i + batch_size < len(rows):
                    await asyncio.sleep(0.5)
        
        console.print(f"[green]Fetched details for {len(packages)} packages[/green]")
        
        # Save to cache
        if use_cache:
            self._save_cache(cache_key, packages)
        
        return packages
    
    async def _fetch_package_details(
        self, 
        client: httpx.AsyncClient, 
        package_name: str,
        download_count: int
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch detailed package info from PyPI JSON API.
        """
        async with self.sem:
            try:
                url = f"{self.PYPI_API_URL}/{package_name}/json"
                response = await client.get(url)
                
                if response.status_code != 200:
                    return None
                
                data = response.json()
                info = data.get("info", {})
                
                # Extract project URLs
                project_urls = info.get("project_urls") or {}
                home_page = info.get("home_page", "")
                
                # Try to find GitHub repo from various sources
                github_repo = None
                
                # Check project_urls for common keys
                for key in ["Source", "Source Code", "Repository", "GitHub", "Homepage", "Code"]:
                    if key in project_urls:
                        github_repo = self.parse_github_url(project_urls[key])
                        if github_repo:
                            break
                
                # Fallback to home_page
                if not github_repo and home_page:
                    github_repo = self.parse_github_url(home_page)
                
                # Check all project_urls values as last resort
                if not github_repo:
                    for url in project_urls.values():
                        github_repo = self.parse_github_url(url)
                        if github_repo:
                            break
                
                return {
                    "name": package_name,
                    "version": info.get("version"),
                    "description": (info.get("summary") or "")[:200],
                    "author": info.get("author"),
                    "license": info.get("license"),
                    "weekly_downloads": download_count,  # Actually 30-day but close enough
                    "pypi_url": f"https://pypi.org/project/{package_name}/",
                    "github_repo": github_repo,
                    "requires_python": info.get("requires_python"),
                }
                
            except (httpx.RequestError, json.JSONDecodeError):
                return None
    
    def filter_github_packages(
        self, 
        packages: List[Dict[str, Any]],
        min_downloads: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Filter packages to only those with GitHub repos and optional minimum downloads.
        
        Returns:
            Tuple of (filtered packages, count of skipped packages)
        """
        filtered = []
        skipped = 0
        
        for pkg in packages:
            if not pkg.get("github_repo"):
                skipped += 1
                continue
            
            if pkg.get("weekly_downloads", 0) < min_downloads:
                skipped += 1
                continue
            
            filtered.append(pkg)
        
        return filtered, skipped
    
    def to_repo_list(self, packages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convert PyPI packages to the format expected by GitHubClient.fetch_batch().
        
        Returns list of dicts with:
        - name: owner/repo
        - language: "Python"
        - package_name: original PyPI package name
        - weekly_downloads: download count
        - registry: "pypi"
        """
        repo_list = []
        seen_repos = set()  # Deduplicate repos (multiple packages can share a repo)
        
        for pkg in packages:
            github_repo = pkg.get("github_repo")
            if not github_repo or github_repo in seen_repos:
                continue
            
            seen_repos.add(github_repo)
            repo_list.append({
                "name": github_repo,
                "language": "Python",
                "package_name": pkg.get("name"),
                "weekly_downloads": pkg.get("weekly_downloads", 0),
                "registry": "pypi",
            })
        
        return repo_list


class MavenClient(PackageRegistryClient):
    """
    Async client for Maven Central via Libraries.io API.
    
    Fetches popular Java/Kotlin packages and maps them to GitHub repositories.
    Uses Libraries.io for popularity data (dependents_count) since Maven Central
    doesn't expose download statistics.
    
    Requires LIBRARIES_IO_API_KEY environment variable or api_key parameter.
    Get your free API key at: https://libraries.io/api
    
    Uses weekly caching to minimize API calls.
    """
    
    LIBRARIES_IO_URL = "https://libraries.io/api"
    MAVEN_CENTRAL_URL = "https://repo1.maven.org/maven2"
    MAVEN_SEARCH_URL = "https://search.maven.org/solrsearch/select"
    
    def __init__(
        self, 
        concurrency: int = 10, 
        cache_dir: Optional[Path] = None,
        api_key: Optional[str] = None
    ):
        super().__init__(concurrency, cache_dir)
        self.registry_name = "maven"
        self.api_key = api_key or os.environ.get("LIBRARIES_IO_API_KEY")
        self.page_size = 100  # Libraries.io max per request
    
    async def search_popular_packages(
        self, 
        max_results: int = 1000,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Fetch top Maven packages sorted by dependents count.
        
        Args:
            max_results: Maximum number of packages to fetch
            use_cache: Whether to use cached results (weekly refresh)
        
        Returns:
            List of package dicts with name, dependents_count, github_repo
        """
        cache_key = f"maven_popular_{max_results}"
        
        # Check cache first
        if use_cache:
            cached = self._load_cache(cache_key)
            if cached:
                return cached
        
        if not self.api_key:
            console.print("[red]Error: Libraries.io API key required for Maven scanning.[/red]")
            console.print("[dim]Set LIBRARIES_IO_API_KEY environment variable or pass api_key parameter.[/dim]")
            console.print("[dim]Get your free API key at: https://libraries.io/api[/dim]")
            return []
        
        console.print(f"[bold blue]Fetching top {max_results} Maven packages from Libraries.io...[/bold blue]")
        
        packages = []
        client = await self._get_client()
        
        with Progress() as progress:
            task = progress.add_task("[cyan]Searching Libraries.io...", total=max_results)
            
            page = 1
            while len(packages) < max_results:
                params = {
                    "api_key": self.api_key,
                    "platforms": "Maven",
                    "sort": "dependents_count",
                    "per_page": min(self.page_size, max_results - len(packages)),
                    "page": page,
                }
                
                async with self.sem:
                    try:
                        response = await client.get(
                            f"{self.LIBRARIES_IO_URL}/search",
                            params=params
                        )
                        
                        if response.status_code == 200:
                            data = response.json()
                            
                            if not data:
                                break
                            
                            for pkg in data:
                                # Extract GitHub repo from repository_url
                                repo_url = pkg.get("repository_url", "")
                                github_repo = self.parse_github_url(repo_url) if repo_url else None
                                
                                # Maven packages use groupId:artifactId naming
                                name = pkg.get("name", "")
                                
                                packages.append({
                                    "name": name,
                                    "version": pkg.get("latest_release_number"),
                                    "description": (pkg.get("description") or "")[:200],
                                    "platform": pkg.get("platform"),
                                    "language": pkg.get("language", "Java"),
                                    "licenses": pkg.get("licenses"),
                                    "homepage": pkg.get("homepage"),
                                    "repository_url": repo_url,
                                    "github_repo": github_repo,
                                    "dependents_count": pkg.get("dependents_count", 0),
                                    "dependent_repos_count": pkg.get("dependent_repos_count", 0),
                                    "stars": pkg.get("stars", 0),
                                    "rank": pkg.get("rank", 0),
                                    "latest_release_published_at": pkg.get("latest_release_published_at"),
                                    # Use dependents_count as popularity metric
                                    "weekly_downloads": pkg.get("dependents_count", 0),
                                })
                            
                            progress.update(task, completed=min(len(packages), max_results))
                            page += 1
                            
                            # Rate limiting: Libraries.io allows 60 req/min
                            await asyncio.sleep(1.0)
                            
                        elif response.status_code == 401:
                            console.print("[red]Invalid Libraries.io API key.[/red]")
                            break
                        elif response.status_code == 429:
                            console.print("[yellow]Rate limited by Libraries.io. Waiting 60s...[/yellow]")
                            await asyncio.sleep(60)
                        else:
                            console.print(f"[yellow]Libraries.io API returned {response.status_code}[/yellow]")
                            break
                            
                    except httpx.RequestError as e:
                        console.print(f"[red]Libraries.io API error: {e}[/red]")
                        break
        
        # Trim to max_results
        packages = packages[:max_results]
        
        console.print(f"[green]Found {len(packages)} Maven packages[/green]")
        
        # Try to resolve GitHub repos for packages without repository_url
        packages = await self._resolve_github_repos_from_pom(packages)
        
        # Save to cache
        if use_cache and packages:
            self._save_cache(cache_key, packages)
        
        return packages
    
    async def _resolve_github_repos_from_pom(
        self, 
        packages: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Try to resolve GitHub repos by fetching POM files for packages
        that don't have a repository_url from Libraries.io.
        """
        packages_needing_lookup = [
            (i, pkg) for i, pkg in enumerate(packages) 
            if not pkg.get("github_repo") and ":" in pkg.get("name", "")
        ]
        
        if not packages_needing_lookup:
            return packages
        
        # Limit POM lookups to avoid excessive requests
        max_pom_lookups = min(len(packages_needing_lookup), 100)
        packages_needing_lookup = packages_needing_lookup[:max_pom_lookups]
        
        console.print(f"[dim]Resolving GitHub repos from POM files for {len(packages_needing_lookup)} packages...[/dim]")
        
        client = await self._get_client()
        
        with Progress() as progress:
            task = progress.add_task(
                "[cyan]Fetching POM files...", 
                total=len(packages_needing_lookup)
            )
            
            for idx, pkg in packages_needing_lookup:
                name = pkg.get("name", "")
                version = pkg.get("version")
                
                if ":" not in name or not version:
                    progress.update(task, advance=1)
                    continue
                
                group_id, artifact_id = name.split(":", 1)
                
                # Convert groupId to path (com.google.guava -> com/google/guava)
                group_path = group_id.replace(".", "/")
                pom_url = f"{self.MAVEN_CENTRAL_URL}/{group_path}/{artifact_id}/{version}/{artifact_id}-{version}.pom"
                
                async with self.sem:
                    try:
                        response = await client.get(pom_url)
                        
                        if response.status_code == 200:
                            pom_content = response.text
                            github_repo = self._parse_github_from_pom(pom_content)
                            if github_repo:
                                packages[idx]["github_repo"] = github_repo
                        
                        progress.update(task, advance=1)
                        await asyncio.sleep(0.05)
                        
                    except httpx.RequestError:
                        progress.update(task, advance=1)
        
        return packages
    
    def _parse_github_from_pom(self, pom_content: str) -> Optional[str]:
        """
        Extract GitHub repository URL from POM XML content.
        
        Looks for SCM URL, project URL, or issue tracker URL.
        """
        # Try SCM URL first (most reliable)
        scm_patterns = [
            r"<scm>.*?<url>([^<]+)</url>.*?</scm>",
            r"<scm>.*?<connection>([^<]+)</connection>.*?</scm>",
            r"<scm>.*?<developerConnection>([^<]+)</developerConnection>.*?</scm>",
        ]
        
        for pattern in scm_patterns:
            match = re.search(pattern, pom_content, re.DOTALL | re.IGNORECASE)
            if match:
                github_repo = self.parse_github_url(match.group(1))
                if github_repo:
                    return github_repo
        
        # Fallback to project URL
        url_match = re.search(r"<url>([^<]*github[^<]+)</url>", pom_content, re.IGNORECASE)
        if url_match:
            github_repo = self.parse_github_url(url_match.group(1))
            if github_repo:
                return github_repo
        
        # Try issue management URL
        issue_match = re.search(r"<issueManagement>.*?<url>([^<]+)</url>.*?</issueManagement>", 
                               pom_content, re.DOTALL | re.IGNORECASE)
        if issue_match:
            github_repo = self.parse_github_url(issue_match.group(1))
            if github_repo:
                return github_repo
        
        return None
    
    def filter_github_packages(
        self, 
        packages: List[Dict[str, Any]],
        min_downloads: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Filter packages to only those with GitHub repos and optional minimum dependents.
        
        Note: For Maven, min_downloads refers to dependents_count (not download count).
        
        Returns:
            Tuple of (filtered packages, count of skipped packages)
        """
        filtered = []
        skipped = 0
        
        for pkg in packages:
            if not pkg.get("github_repo"):
                skipped += 1
                continue
            
            if pkg.get("dependents_count", 0) < min_downloads:
                skipped += 1
                continue
            
            filtered.append(pkg)
        
        return filtered, skipped
    
    def to_repo_list(self, packages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convert Maven packages to the format expected by GitHubClient.fetch_batch().
        
        Returns list of dicts with:
        - name: owner/repo
        - language: "Java" (or from package metadata)
        - package_name: original Maven package name (groupId:artifactId)
        - weekly_downloads: dependents_count (used as popularity proxy)
        - registry: "maven"
        """
        repo_list = []
        seen_repos = set()  # Deduplicate repos (multiple packages can share a repo)
        
        for pkg in packages:
            github_repo = pkg.get("github_repo")
            if not github_repo or github_repo in seen_repos:
                continue
            
            seen_repos.add(github_repo)
            repo_list.append({
                "name": github_repo,
                "language": pkg.get("language", "Java"),
                "package_name": pkg.get("name"),
                "weekly_downloads": pkg.get("dependents_count", 0),
                "registry": "maven",
            })
        
        return repo_list
