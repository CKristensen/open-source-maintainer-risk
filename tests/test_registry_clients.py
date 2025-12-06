"""
Simple tests for the registry clients (NPM, PyPI, Maven) and GitHub client.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from src.registry_clients import NPMClient, PyPIClient, MavenClient, PackageRegistryClient
from src.ingestion import GitHubClient


class TestParseGitHubUrl:
    """Tests for the parse_github_url static method."""
    
    def test_standard_https_url(self):
        url = "https://github.com/facebook/react"
        assert PackageRegistryClient.parse_github_url(url) == "facebook/react"
    
    def test_git_plus_https_url(self):
        url = "git+https://github.com/lodash/lodash.git"
        assert PackageRegistryClient.parse_github_url(url) == "lodash/lodash"
    
    def test_git_protocol_url(self):
        url = "git://github.com/expressjs/express.git"
        assert PackageRegistryClient.parse_github_url(url) == "expressjs/express"
    
    def test_ssh_url(self):
        url = "git@github.com:vuejs/vue.git"
        assert PackageRegistryClient.parse_github_url(url) == "vuejs/vue"
    
    def test_github_shorthand(self):
        url = "github:axios/axios"
        assert PackageRegistryClient.parse_github_url(url) == "axios/axios"
    
    def test_simple_owner_repo_format(self):
        url = "microsoft/typescript"
        assert PackageRegistryClient.parse_github_url(url) == "microsoft/typescript"
    
    def test_url_with_git_suffix(self):
        url = "https://github.com/webpack/webpack.git"
        assert PackageRegistryClient.parse_github_url(url) == "webpack/webpack"
    
    def test_empty_url(self):
        assert PackageRegistryClient.parse_github_url("") is None
        assert PackageRegistryClient.parse_github_url(None) is None
    
    def test_non_github_url(self):
        url = "https://gitlab.com/inkscape/inkscape"
        assert PackageRegistryClient.parse_github_url(url) is None


class TestNPMClient:
    """Tests for the NPM registry client."""
    
    @pytest.fixture
    def npm_client(self, tmp_path):
        return NPMClient(concurrency=5, cache_dir=tmp_path)
    
    def test_client_initialization(self, npm_client):
        assert npm_client.registry_name == "npm"
        assert npm_client.page_size == 250
        assert npm_client.sem._value == 5
    
    def test_filter_github_packages(self, npm_client):
        packages = [
            {"name": "react", "github_repo": "facebook/react", "weekly_downloads": 1000},
            {"name": "no-repo-pkg", "github_repo": None, "weekly_downloads": 500},
            {"name": "low-downloads", "github_repo": "owner/repo", "weekly_downloads": 10},
        ]
        
        filtered, skipped = npm_client.filter_github_packages(packages, min_downloads=100)
        
        assert len(filtered) == 1
        assert filtered[0]["name"] == "react"
        assert skipped == 2
    
    def test_to_repo_list(self, npm_client):
        packages = [
            {"name": "react", "github_repo": "facebook/react", "weekly_downloads": 1000},
            {"name": "react-dom", "github_repo": "facebook/react", "weekly_downloads": 800},  # Same repo
            {"name": "express", "github_repo": "expressjs/express", "weekly_downloads": 500},
        ]
        
        repo_list = npm_client.to_repo_list(packages)
        
        # Should deduplicate repos
        assert len(repo_list) == 2
        assert repo_list[0]["name"] == "facebook/react"
        assert repo_list[0]["language"] == "JavaScript"
        assert repo_list[0]["registry"] == "npm"


class TestPyPIClient:
    """Tests for the PyPI registry client."""
    
    @pytest.fixture
    def pypi_client(self, tmp_path):
        return PyPIClient(concurrency=5, cache_dir=tmp_path)
    
    def test_client_initialization(self, pypi_client):
        assert pypi_client.registry_name == "pypi"
    
    def test_filter_github_packages(self, pypi_client):
        packages = [
            {"name": "requests", "github_repo": "psf/requests", "weekly_downloads": 50000},
            {"name": "internal-pkg", "github_repo": None, "weekly_downloads": 100},
            {"name": "flask", "github_repo": "pallets/flask", "weekly_downloads": 30000},
        ]
        
        filtered, skipped = pypi_client.filter_github_packages(packages, min_downloads=1000)
        
        assert len(filtered) == 2
        assert filtered[0]["name"] == "requests"
        assert filtered[1]["name"] == "flask"
        assert skipped == 1
    
    def test_to_repo_list(self, pypi_client):
        packages = [
            {"name": "requests", "github_repo": "psf/requests", "weekly_downloads": 50000},
            {"name": "flask", "github_repo": "pallets/flask", "weekly_downloads": 30000},
        ]
        
        repo_list = pypi_client.to_repo_list(packages)
        
        assert len(repo_list) == 2
        assert repo_list[0]["name"] == "psf/requests"
        assert repo_list[0]["language"] == "Python"
        assert repo_list[0]["registry"] == "pypi"


class TestMavenClient:
    """Tests for the Maven registry client."""
    
    @pytest.fixture
    def maven_client(self, tmp_path):
        return MavenClient(concurrency=5, cache_dir=tmp_path, api_key="test_key")
    
    def test_client_initialization(self, maven_client):
        assert maven_client.registry_name == "maven"
        assert maven_client.api_key == "test_key"
        assert maven_client.page_size == 100
    
    def test_filter_github_packages(self, maven_client):
        packages = [
            {"name": "com.google.guava:guava", "github_repo": "google/guava", "dependents_count": 10000},
            {"name": "org.internal:pkg", "github_repo": None, "dependents_count": 50},
            {"name": "org.apache.commons:commons-lang3", "github_repo": "apache/commons-lang", "dependents_count": 5000},
        ]
        
        filtered, skipped = maven_client.filter_github_packages(packages, min_downloads=100)
        
        assert len(filtered) == 2
        assert skipped == 1
    
    def test_to_repo_list(self, maven_client):
        packages = [
            {"name": "com.google.guava:guava", "github_repo": "google/guava", "weekly_downloads": 10000, "language": "Java"},
            {"name": "org.jetbrains.kotlin:kotlin-stdlib", "github_repo": "JetBrains/kotlin", "weekly_downloads": 8000, "language": "Kotlin"},
        ]
        
        repo_list = maven_client.to_repo_list(packages)
        
        assert len(repo_list) == 2
        assert repo_list[0]["name"] == "google/guava"
        assert repo_list[0]["language"] == "Java"
        assert repo_list[0]["registry"] == "maven"


class TestGitHubClient:
    """Tests for the GitHub client."""
    
    @pytest.fixture
    def github_client(self):
        return GitHubClient(token="test_token", concurrency=5)
    
    def test_client_initialization(self, github_client):
        assert github_client.base_url == "https://api.github.com"
        assert "Authorization" in github_client.headers
        assert github_client.headers["Authorization"] == "token test_token"
        assert github_client.sem._value == 5
    
    @pytest.mark.asyncio
    async def test_fetch_contributor_stats_success(self, github_client):
        """Test successful contributor stats fetch."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"author": {"login": "user1"}, "total": 100},
            {"author": {"login": "user2"}, "total": 50},
        ]
        
        with patch.object(github_client, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_get_client.return_value = mock_client
            
            result = await github_client.fetch_contributor_stats("owner/repo")
            
            assert result["status"] == "success"
            assert result["contributions"] == [100, 50]
            assert result["contributor_count"] == 2
    
    @pytest.mark.asyncio
    async def test_fetch_contributor_stats_not_found(self, github_client):
        """Test 404 response handling."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        
        with patch.object(github_client, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_get_client.return_value = mock_client
            
            result = await github_client.fetch_contributor_stats("nonexistent/repo")
            
            assert result["status"] == "not_found"
            assert result["contributions"] == []
    
    @pytest.mark.asyncio
    async def test_fetch_participation_stats_success(self, github_client):
        """Test successful participation stats fetch."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "all": [10, 20, 30, 40] * 13,  # 52 weeks
            "owner": [5, 10, 15, 20] * 13,
        }
        
        with patch.object(github_client, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_get_client.return_value = mock_client
            
            result = await github_client.fetch_participation_stats("owner/repo")
            
            assert result["status"] == "success"
            assert "all" in result["data"]
            assert len(result["data"]["all"]) == 52


class TestCaching:
    """Tests for the caching functionality."""
    
    def test_cache_save_and_load(self, tmp_path):
        client = NPMClient(cache_dir=tmp_path)
        
        test_data = [
            {"name": "package1", "downloads": 1000},
            {"name": "package2", "downloads": 500},
        ]
        
        # Save to cache
        client._save_cache("test_cache", test_data)
        
        # Verify cache file exists
        cache_path = tmp_path / "test_cache.json"
        assert cache_path.exists()
        
        # Load from cache
        loaded = client._load_cache("test_cache")
        assert loaded == test_data
    
    def test_cache_invalid_when_missing(self, tmp_path):
        client = NPMClient(cache_dir=tmp_path)
        
        # Cache doesn't exist
        loaded = client._load_cache("nonexistent")
        assert loaded is None
