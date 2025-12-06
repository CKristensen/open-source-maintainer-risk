"""
NPM Registry Client for fetching popular packages and mapping to GitHub repos.

This module provides async functionality to:
1. Search for most popular NPM packages by download count
2. Extract GitHub repository URLs from package metadata
3. Cache results to minimize API calls (weekly refresh)
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


# Future: PyPI client stub
class PyPIClient(PackageRegistryClient):
    """
    Async client for PyPI API.
    TODO: Implement for Python package analysis.
    """
    
    def __init__(self, concurrency: int = 10, cache_dir: Optional[Path] = None):
        super().__init__(concurrency, cache_dir)
        self.registry_name = "pypi"
    
    async def search_popular_packages(self, max_results: int = 1000) -> List[Dict[str, Any]]:
        """
        Fetch top PyPI packages.
        Note: PyPI doesn't have a direct popularity API.
        Options: Use https://hugovk.github.io/top-pypi-packages/ or libraries.io
        """
        # TODO: Implement PyPI package fetching
        raise NotImplementedError("PyPI client not yet implemented")


# Future: Maven client stub  
class MavenClient(PackageRegistryClient):
    """
    Async client for Maven Central API.
    TODO: Implement for Java package analysis.
    """
    
    def __init__(self, concurrency: int = 10, cache_dir: Optional[Path] = None):
        super().__init__(concurrency, cache_dir)
        self.registry_name = "maven"
    
    async def search_popular_packages(self, max_results: int = 1000) -> List[Dict[str, Any]]:
        """
        Fetch top Maven packages.
        Options: Use Maven Central search API or libraries.io
        """
        # TODO: Implement Maven package fetching
        raise NotImplementedError("Maven client not yet implemented")
