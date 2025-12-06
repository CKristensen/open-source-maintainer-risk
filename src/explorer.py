import sqlite3
import webbrowser
from textual.app import App, ComposeResult
from textual.widgets import DataTable, Header, Footer, Input, Static
from textual.binding import Binding
from textual.containers import Vertical, Horizontal, Center
from textual.screen import ModalScreen
from textual import events


HELP_TEXT = """\
[bold cyan]Risk Explorer - Help[/]

[bold]Navigation:[/]
  [yellow]j / ↓[/]      Move down
  [yellow]k / ↑[/]      Move up
  [yellow]g[/]          Go to top
  [yellow]G[/]          Go to bottom
  [yellow]Enter[/]      Select row / show details

[bold]Search & Filter:[/]
  [yellow]/[/]          Focus search box
  [yellow]Escape[/]     Clear search, return to table

[bold]Sorting:[/]
  [yellow]s[/]          Sort by risk score
  [yellow]c[/]          Sort by contributor count
  [yellow]n[/]          Sort by repo name
  [yellow]w[/]          Sort by weekly downloads

[bold]Views:[/]
  [yellow]d[/]          Toggle detail panel
  [yellow]r[/]          Refresh data from database
  [yellow]o[/]          Open selected repo in browser

[bold]Other:[/]
  [yellow]?[/]          Show this help
  [yellow]q[/]          Quit

[bold cyan]Risk Metrics Explained:[/]
  [yellow]Source[/]        Where the repo was found (NPM, GH = GitHub search)
  [yellow]Downloads[/]     Weekly package downloads (if from registry)
  [yellow]Risk Score[/]    Combined score (higher = riskier)
  [yellow]Velocity[/]      Recent vs older commits (>1x = growing)
  [yellow]Gini[/]          Contribution inequality (0-1, higher = concentrated)
  [yellow]Top1%[/]         % of commits by top contributor
  [yellow]Contributors[/]  Total unique contributors
  [yellow]Commits(1Y)[/]   Total commits in last 52 weeks

[dim]Press any key to close[/]
"""


class HelpScreen(ModalScreen):
    """A modal screen showing help information."""
    
    CSS = """
    HelpScreen {
        align: center middle;
    }
    
    #help-dialog {
        width: 70;
        height: auto;
        max-height: 80%;
        padding: 1 2;
        background: $surface;
        border: thick $primary;
    }
    """
    
    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
        Binding("question_mark", "dismiss", "Close"),
    ]
    
    def compose(self) -> ComposeResult:
        yield Static(HELP_TEXT, id="help-dialog")
    
    def on_key(self, event: events.Key) -> None:
        """Close on any key press."""
        self.dismiss()


class RiskExplorer(App):
    """A k9s-style TUI for exploring the risk database."""
    
    CSS = """
    Screen {
        background: $surface;
    }
    
    #search-box {
        dock: top;
        height: 3;
        padding: 0 1;
        background: $boost;
    }
    
    #search-input {
        width: 100%;
    }
    
    #stats {
        dock: top;
        height: 3;
        padding: 0 1;
        background: $primary;
        color: $text;
    }
    
    DataTable {
        height: 1fr;
    }
    
    #detail-panel {
        dock: bottom;
        height: 8;
        padding: 1;
        background: $boost;
        border-top: solid $primary;
        display: none;
    }
    
    #detail-panel.visible {
        display: block;
    }
    
    .risk-critical {
        color: red;
    }
    
    .risk-high {
        color: orange;
    }
    
    .risk-medium {
        color: yellow;
    }
    
    .risk-low {
        color: green;
    }
    """
    
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("question_mark", "show_help", "Help"),
        Binding("/", "focus_search", "Search"),
        Binding("escape", "clear_search", "Clear"),
        Binding("r", "refresh", "Refresh"),
        Binding("d", "toggle_detail", "Details"),
        Binding("o", "open_repo", "Open in Browser"),
        Binding("c", "sort_contributors", "Sort Contributors"),
        Binding("s", "sort_score", "Sort Score"),
        Binding("n", "sort_name", "Sort Name"),
        Binding("w", "sort_downloads", "Sort Downloads"),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("g", "cursor_top", "Top", show=False),
        Binding("G", "cursor_bottom", "Bottom", show=False),
    ]
    
    def __init__(self, db_path: str = "risk_report.db"):
        super().__init__()
        self.db_path = db_path
        self.all_data = []
        self.filtered_data = []
        self.total_db_rows = 0
        self.sort_column = "total_risk_score"
        self.sort_reverse = True
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("", id="stats")
        yield Horizontal(
            Input(placeholder="Type to filter repos... (press / to focus, ? for help)", id="search-input"),
            id="search-box"
        )
        yield DataTable(id="table")
        yield Static("", id="detail-panel")
        yield Footer()
    
    def on_mount(self) -> None:
        self.title = "Risk Explorer"
        self.sub_title = f"Database: {self.db_path}"
        self.load_data()
        self.setup_table()
        self.refresh_table()
    
    def load_data(self) -> None:
        """Load data from SQLite database."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            
            # Get total row count in database
            count_cursor = conn.execute("SELECT COUNT(*) FROM risk_report")
            self.total_db_rows = count_cursor.fetchone()[0]
            
            cursor = conn.execute("""
                SELECT repo, language, total_risk_score, risk_level, velocity_ratio, 
                       gini_coefficient, top1_share, top3_share, contributor_count,
                       total_commits, recent_commits, updated_at,
                       weekly_downloads, registry, package_name
                FROM risk_report
                ORDER BY total_risk_score DESC
            """)
            self.all_data = [dict(row) for row in cursor.fetchall()]
            self.filtered_data = self.all_data.copy()
            conn.close()
        except sqlite3.OperationalError as e:
            self.all_data = []
            self.filtered_data = []
            self.total_db_rows = 0
            self.notify(f"Database error: {e}", severity="error")
    
    def setup_table(self) -> None:
        """Setup the data table columns."""
        table = self.query_one("#table", DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        
        table.add_column("Repository", key="repo", width=35)
        table.add_column("Lang", key="lang", width=12)
        table.add_column("Source", key="source", width=6)
        table.add_column("Downloads", key="downloads", width=10)
        table.add_column("Risk", key="score", width=6)
        table.add_column("Level", key="level", width=10)
        table.add_column("Velocity", key="velocity", width=10)
        table.add_column("Gini", key="gini", width=8)
        table.add_column("Top1%", key="top1", width=8)
        table.add_column("Contributors", key="contrib", width=10)
        table.add_column("Commits(1Y)", key="commits", width=11)
    
    def refresh_table(self) -> None:
        """Refresh the table with current filtered data."""
        table = self.query_one("#table", DataTable)
        table.clear()
        
        # Sort data
        self.filtered_data.sort(
            key=lambda x: x.get(self.sort_column, 0) or 0,
            reverse=self.sort_reverse
        )
        
        for row in self.filtered_data:
            level = row.get("risk_level", "?")
            level_styled = self._style_level(level)
            
            # Handle None values for contributor metrics
            gini = row.get('gini_coefficient')
            gini_str = f"{gini:.2f}" if gini is not None else "N/A"
            top1 = row.get('top1_share')
            top1_str = f"{top1:.0%}" if top1 is not None else "N/A"
            contrib = row.get('contributor_count')
            contrib_str = (str(contrib) if int(contrib) != 100 else ">100") if contrib is not None else "?"
            
            # Format downloads (weekly)
            downloads = row.get('weekly_downloads')
            if downloads is not None and downloads > 0:
                if downloads >= 1_000_000:
                    dl_str = f"{downloads / 1_000_000:.1f}M"
                elif downloads >= 1_000:
                    dl_str = f"{downloads / 1_000:.0f}K"
                else:
                    dl_str = str(downloads)
            else:
                dl_str = "-"
            
            # Source (registry)
            registry = row.get('registry')
            source_str = registry.upper() if registry else "GH"
            
            table.add_row(
                row.get("repo", "?"),
                row.get("language", "?"),
                source_str,
                dl_str,
                f"{row.get('total_risk_score', 0):.1f}",
                level_styled,
                f"{row.get('velocity_ratio', 0):.2f}x",
                gini_str,
                top1_str,
                contrib_str,
                str(row.get("total_commits", "?")),
            )
        
        # Update stats
        stats = self.query_one("#stats", Static)
        total = len(self.all_data)
        showing = len(self.filtered_data)
        critical = sum(1 for r in self.filtered_data if r.get("risk_level") == "CRITICAL")
        high = sum(1 for r in self.filtered_data if r.get("risk_level") == "HIGH")
        stats.update(
            f" DB Total: {self.total_db_rows} | Showing: {showing} | "
            f"[red]CRITICAL: {critical}[/red] | [orange1]HIGH: {high}[/orange1] | "
            f"Sort: {self.sort_column} ({'desc' if self.sort_reverse else 'asc'})"
        )
    
    def _style_level(self, level: str) -> str:
        """Apply color styling to risk level."""
        colors = {
            "CRITICAL": "[red bold]CRITICAL[/]",
            "HIGH": "[orange1]HIGH[/]",
            "MEDIUM": "[yellow]MEDIUM[/]",
            "LOW": "[green]LOW[/]",
        }
        return colors.get(level, level)
    
    def on_input_changed(self, event: Input.Changed) -> None:
        """Filter table when search input changes."""
        search = event.value.lower().strip()
        if search:
            self.filtered_data = [
                r for r in self.all_data
                if search in r.get("repo", "").lower()
                or search in r.get("risk_level", "").lower()
            ]
        else:
            self.filtered_data = self.all_data.copy()
        self.refresh_table()
    
    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Show details when a row is selected."""
        if event.row_key is not None:
            row_index = event.cursor_row
            if 0 <= row_index < len(self.filtered_data):
                self._show_detail(self.filtered_data[row_index])
    
    def _show_detail(self, row: dict) -> None:
        """Display detailed info for selected repo."""
        detail = self.query_one("#detail-panel", Static)
        detail.add_class("visible")
        
        # Handle None values for contributor metrics
        gini = row.get('gini_coefficient')
        gini_str = f"{gini:.3f}" if gini is not None else "N/A"
        contrib = row.get('contributor_count')
        contrib_str = str(contrib) if contrib is not None else "?"
        top1 = row.get('top1_share')
        top1_str = f"{top1:.1%}" if top1 is not None else "N/A"
        top3 = row.get('top3_share')
        top3_str = f"{top3:.1%}" if top3 is not None else "N/A"
        
        # Format downloads for detail
        downloads = row.get('weekly_downloads')
        if downloads is not None and downloads > 0:
            if downloads >= 1_000_000:
                dl_str = f"{downloads / 1_000_000:.1f}M/wk"
            elif downloads >= 1_000:
                dl_str = f"{downloads / 1_000:.0f}K/wk"
            else:
                dl_str = f"{downloads}/wk"
        else:
            dl_str = "N/A"
        
        registry = row.get('registry')
        pkg_name = row.get('package_name')
        source_str = f"{registry.upper()}: {pkg_name}" if registry and pkg_name else "GitHub only"
        
        detail.update(
            f"[bold cyan]{row.get('repo', '?')}[/] [dim]({source_str})[/]\n"
            f"Risk Score: {row.get('total_risk_score', 0):.1f} ({row.get('risk_level', '?')}) | "
            f"Velocity: {row.get('velocity_ratio', 0):.2f}x | "
            f"Gini: {gini_str} | "
            f"Downloads: {dl_str}\n"
            f"Contributors: {contrib_str} | "
            f"Total Commits: {row.get('total_commits', '?')} | "
            f"Recent Commits: {row.get('recent_commits', '?')}\n"
            f"Top 1 Share: {top1_str} | "
            f"Top 3 Share: {top3_str}\n"
            f"[dim]Updated: {row.get('updated_at', 'N/A')}[/]"
        )
    
    def action_focus_search(self) -> None:
        """Focus the search input."""
        self.query_one("#search-input", Input).focus()
    
    def action_clear_search(self) -> None:
        """Clear search and reset filter."""
        search_input = self.query_one("#search-input", Input)
        search_input.value = ""
        self.filtered_data = self.all_data.copy()
        self.refresh_table()
        self.query_one("#table", DataTable).focus()
    
    def action_refresh(self) -> None:
        """Reload data from database."""
        self.load_data()
        search_input = self.query_one("#search-input", Input)
        if search_input.value:
            search = search_input.value.lower().strip()
            self.filtered_data = [
                r for r in self.all_data
                if search in r.get("repo", "").lower()
            ]
        self.refresh_table()
        self.notify("Data refreshed!")
    
    def action_toggle_detail(self) -> None:
        """Toggle the detail panel."""
        detail = self.query_one("#detail-panel", Static)
        detail.toggle_class("visible")
    
    def action_open_repo(self) -> None:
        """Open the selected repository in the browser."""
        table = self.query_one("#table", DataTable)
        if table.cursor_row is not None and 0 <= table.cursor_row < len(self.filtered_data):
            row = self.filtered_data[table.cursor_row]
            repo_name = row.get("repo", "")
            if repo_name:
                url = f"https://github.com/{repo_name}"
                webbrowser.open(url)
                self.notify(f"Opening {repo_name} in browser...")
    
    def action_sort_contributors(self) -> None:
        """Sort by contributor count."""
        if self.sort_column == "contributor_count":
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = "contributor_count"
            self.sort_reverse = True
        self.refresh_table()
    
    def action_sort_score(self) -> None:
        """Sort by risk score."""
        if self.sort_column == "total_risk_score":
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = "total_risk_score"
            self.sort_reverse = True
        self.refresh_table()
    
    def action_sort_name(self) -> None:
        """Sort by repo name."""
        if self.sort_column == "repo":
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = "repo"
            self.sort_reverse = False
        self.refresh_table()
    
    def action_sort_downloads(self) -> None:
        """Sort by weekly downloads."""
        if self.sort_column == "weekly_downloads":
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = "weekly_downloads"
            self.sort_reverse = True
        self.refresh_table()
    
    def action_show_help(self) -> None:
        """Show the help screen."""
        self.push_screen(HelpScreen())
    
    def action_cursor_down(self) -> None:
        """Move cursor down (vim j)."""
        table = self.query_one("#table", DataTable)
        table.action_cursor_down()
    
    def action_cursor_up(self) -> None:
        """Move cursor up (vim k)."""
        table = self.query_one("#table", DataTable)
        table.action_cursor_up()
    
    def action_cursor_top(self) -> None:
        """Move cursor to top (vim g)."""
        table = self.query_one("#table", DataTable)
        table.move_cursor(row=0)
    
    def action_cursor_bottom(self) -> None:
        """Move cursor to bottom (vim G)."""
        table = self.query_one("#table", DataTable)
        table.move_cursor(row=table.row_count - 1)


def run_explorer(db_path: str = "risk_report.db"):
    """Run the explorer app."""
    app = RiskExplorer(db_path=db_path)
    app.run()
