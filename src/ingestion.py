import asyncio
import httpx
import os
from typing import List, Dict, Any, Optional
from rich.progress import Progress
from rich.console import Console

console = Console()

class GitHubClient:
    """
    Asynchronous client for fetching GitHub repository statistics.
    Optimized for the /stats/participation endpoint to minimize API usage.
    """
    def __init__(self, token: str, concurrency: int = 20):
        self.base_url = "https://api.github.com"
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Attensi-Risk-Detector-v1"
        }
        # Semaphore limits the number of active coroutines
        self.sem = asyncio.Semaphore(concurrency)
        self.timeout = 30.0
        self.client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Lazily create the client."""
        if self.client is None:
            self.client = httpx.AsyncClient(headers=self.headers, timeout=self.timeout)
        return self.client

    async def fetch_contributor_stats(self, repo_name: str, retries: int = 5) -> Dict[str, Any]:
        """
        Fetches contributor statistics for a repository.
        Returns commit counts per contributor for Gini coefficient calculation.
        Retries on 202 (pending calculation) responses.
        """
        url = f"{self.base_url}/repos/{repo_name}/stats/contributors"
        client = await self._get_client()
        
        for attempt in range(retries):
            async with self.sem:
                try:
                    response = await client.get(url)
                    
                    if response.status_code == 200:
                        data = response.json()
                        # Extract total commits per contributor
                        contributions = [c.get("total", 0) for c in data] if data else []
                        return {
                            "repo": repo_name,
                            "contributions": contributions,
                            "contributor_count": len(contributions),
                            "contributor_data_available": True,
                            "status": "success"
                        }
                    elif response.status_code == 202:
                        # GitHub is computing stats, wait and retry with exponential backoff
                        if attempt < retries - 1:
                            await asyncio.sleep(2.0 * (attempt + 1))  # Exponential backoff
                            continue
                        return {"repo": repo_name, "status": "pending_calculation", "contributions": [], "contributor_data_available": False}
                    elif response.status_code == 404:
                        return {"repo": repo_name, "status": "not_found", "contributions": [], "contributor_data_available": False}
                    elif response.status_code == 403:
                        return {"repo": repo_name, "status": "rate_limited", "contributions": [], "contributor_data_available": False}
                    else:
                        return {"repo": repo_name, "status": "error", "contributions": [], "contributor_data_available": False}
                except httpx.RequestError as e:
                    return {"repo": repo_name, "status": "network_error", "contributions": [], "contributor_data_available": False, "error": str(e)}
        
        return {"repo": repo_name, "status": "pending_calculation", "contributions": [], "contributor_data_available": False}

    async def fetch_participation_stats(self, repo_name: str, retries: int = 5) -> Dict[str, Any]:
        """
        Fetches the weekly commit counts for the last 52 weeks.
        Returns a dictionary with raw data or error status.
        Retries on 202 (pending calculation) responses.
        """
        url = f"{self.base_url}/repos/{repo_name}/stats/participation"
        client = await self._get_client()
        
        for attempt in range(retries):
            async with self.sem:
                try:
                    response = await client.get(url)
                    
                    if response.status_code == 200:
                        return {
                            "repo": repo_name,
                            "data": response.json(),
                            "status": "success"
                        }
                    elif response.status_code == 202:
                        # 202 Accepted means GitHub is calculating stats in background.
                        # Wait with exponential backoff and retry.
                        if attempt < retries - 1:
                            await asyncio.sleep(2.0 * (attempt + 1))
                            continue
                        return {"repo": repo_name, "status": "pending_calculation"}
                    elif response.status_code == 404:
                        return {"repo": repo_name, "status": "not_found"}
                    elif response.status_code == 403:
                        return {"repo": repo_name, "status": "rate_limited"}
                    else:
                        return {"repo": repo_name, "status": "error", "code": response.status_code}
                except httpx.RequestError as e:
                    if attempt < retries - 1:
                        await asyncio.sleep(1.0)
                        continue
                    return {"repo": repo_name, "status": "network_error", "error": str(e)}
        
        return {"repo": repo_name, "status": "pending_calculation"}

    async def fetch_batch(self, repo_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Orchestrates the concurrent fetching of participation and contributor stats.
        Merges both results for each repository.
        repo_list: List of dicts with 'name' and 'language' keys.
        """
        async def fetch_both(repo_info: Dict[str, Any]) -> Dict[str, Any]:
            repo_name = repo_info["name"]
            language = repo_info.get("language", "Unknown")
            participation, contributors = await asyncio.gather(
                self.fetch_participation_stats(repo_name),
                self.fetch_contributor_stats(repo_name)
            )
            # Merge results
            result = participation.copy()
            result["contributions"] = contributors.get("contributions", [])
            result["contributor_count"] = contributors.get("contributor_count", 0)
            result["contributor_data_available"] = contributors.get("contributor_data_available", False)
            result["language"] = language
            return result
        
        tasks = [fetch_both(repo) for repo in repo_list]
        return await asyncio.gather(*tasks)

    async def close(self):
        if self.client is not None:
            await self.client.aclose()
            self.client = None

    async def search_repositories(self, query: str = "stars:>1000", per_page: int = 100, max_results: int = 5000) -> List[Dict[str, Any]]:
        """
        Searches GitHub repositories and returns a list of repo info dicts.
        Each dict contains 'name' (owner/repo) and 'language' (primary language).
        Default query returns popular repositories.
        """
        url = f"{self.base_url}/search/repositories"
        repos = []
        page = 1
        client = await self._get_client()
        
        while len(repos) < max_results:
            params = {
                "q": query,
                "sort": "stars",
                "order": "desc",
                "per_page": min(per_page, max_results - len(repos)),
                "page": page
            }
            
            try:
                response = await client.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    items = data.get("items", [])
                    
                    if not items:
                        break
                    
                    repos.extend([{
                        "name": item["full_name"],
                        "language": item.get("language") or "Unknown"
                    } for item in items])
                    
                    # Check if we've reached the last page
                    if len(items) < per_page or len(repos) >= max_results:
                        break
                    
                    page += 1
                elif response.status_code == 403:
                    console.print("[red]Rate limit exceeded[/red]")
                    break
                else:
                    break
            except httpx.RequestError:
                break
        
        return repos[:max_results]
