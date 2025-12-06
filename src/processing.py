import polars as pl
from typing import List, Dict


def calculate_gini_coefficient(contributions: List[int]) -> float:
    """
    Calculate the Gini coefficient for contribution distribution.
    
    Gini = 0: Perfect equality (all contributors have equal commits)
    Gini = 1: Perfect inequality (one person has all commits)
    
    Higher Gini = Higher concentration = Higher bus factor risk
    """
    if not contributions or len(contributions) == 0:
        return 1.0  # No data = assume high risk
    
    if len(contributions) == 1:
        return 1.0  # Single contributor = maximum concentration
    
    # Sort contributions ascending
    sorted_contribs = sorted(contributions)
    n = len(sorted_contribs)
    total = sum(sorted_contribs)
    
    if total == 0:
        return 1.0  # No commits = high risk
    
    # Gini formula: G = (2 * Σ(i * x_i)) / (n * Σx_i) - (n + 1) / n
    cumulative_sum = sum((i + 1) * x for i, x in enumerate(sorted_contribs))
    gini = (2 * cumulative_sum) / (n * total) - (n + 1) / n
    
    return max(0.0, min(1.0, gini))  # Clamp to [0, 1]


def calculate_top_contributor_share(contributions: List[int]) -> Dict[str, float]:
    """
    Calculate what percentage of commits the top contributors own.
    Returns top1_share and top3_share.
    """
    if not contributions or len(contributions) == 0:
        return {"top1_share": 1.0, "top3_share": 1.0}
    
    total = sum(contributions)
    if total == 0:
        return {"top1_share": 1.0, "top3_share": 1.0}
    
    sorted_desc = sorted(contributions, reverse=True)
    top1 = sorted_desc[0] / total
    top3 = sum(sorted_desc[:3]) / total if len(sorted_desc) >= 3 else sum(sorted_desc) / total
    
    return {"top1_share": top1, "top3_share": top3}


def compute_risk_metrics(raw_results: List) -> pl.DataFrame:
    """
    Transforms raw API responses into a structured Risk Scorecard.
    Uses Gini Coefficient to measure contribution concentration (bus factor).
    
    Supports enriched data from package registries (NPM, PyPI, Maven, etc.)
    with optional fields: package_name, weekly_downloads, registry.
    """
    # 1. Filter valid data and compute Gini + top contributor shares
    valid_records = []
    for r in raw_results:
        if r["status"] == "success" and "data" in r:
            contributions = r.get("contributions", [])
            contributor_data_available = r.get("contributor_data_available", False)
            
            # Only calculate metrics if we have actual contributor data
            if contributor_data_available and contributions:
                gini = calculate_gini_coefficient(contributions)
                top_shares = calculate_top_contributor_share(contributions)
                contributor_count = r.get("contributor_count", len(contributions))
            else:
                # Use None/sentinel values to indicate missing data
                gini = None
                top_shares = {"top1_share": None, "top3_share": None}
                contributor_count = None
            
            record = {
                "repo": r["repo"],
                "language": r.get("language", "Unknown"),
                "all_commits": r["data"]["all"],
                "contributor_count": contributor_count,
                "contributor_data_available": contributor_data_available,
                "gini_coefficient": gini,
                "top1_share": top_shares["top1_share"],
                "top3_share": top_shares["top3_share"],
            }
            
            # Optional package registry fields (NPM, PyPI, Maven, etc.)
            if "package_name" in r:
                record["package_name"] = r["package_name"]
            if "weekly_downloads" in r:
                record["weekly_downloads"] = r["weekly_downloads"]
            if "registry" in r:
                record["registry"] = r["registry"]
            
            valid_records.append(record)
    
    if not valid_records:
        return pl.DataFrame()

    # 2. Initialize DataFrame
    df = pl.DataFrame(valid_records)

    # 3. Feature Engineering
    # Calculate totals and recent activity from the 52-week commit data
    df = df.with_columns([
        # Total commits over the year
        pl.col("all_commits").list.sum().alias("total_commits"),
        # Recent activity (last 13 weeks = ~3 months)
        pl.col("all_commits").list.tail(13).list.sum().alias("recent_commits"),
        # Older activity (first 13 weeks)
        pl.col("all_commits").list.head(13).list.sum().alias("older_commits"),
    ])

    # 4. Handle "Division by Zero" edge cases (Data Quality)
    epsilon = 0.001
    df = df.with_columns([
        # Velocity ratio: recent vs older activity (higher = growing)
        (pl.col("recent_commits") / (pl.col("older_commits") + epsilon)).alias("velocity_ratio"),
    ])

    # 5. Risk Scoring Model using Gini Coefficient
    # Logic: 
    # - If velocity drops (ratio < 0.5), Risk goes UP.
    # - If Gini is high (concentrated contributions), Risk goes UP.
    # - If top contributor owns >50%, or top 3 own >80%, Risk goes UP.
    # - If contributor data is unavailable, use neutral score (3.0) instead of extreme values
    df = df.with_columns([
        # Velocity risk: lower velocity = higher risk (scale 0-5)
        pl.when(pl.col("velocity_ratio") < 0.25).then(5.0)
          .when(pl.col("velocity_ratio") < 0.5).then(4.0)
          .when(pl.col("velocity_ratio") < 0.75).then(3.0)
          .when(pl.col("velocity_ratio") < 1.0).then(2.0)
          .otherwise(1.0)
          .alias("risk_velocity"),
        
        # Bus factor risk based on Gini coefficient (scale 0-5)
        # Gini > 0.8 = very concentrated, Gini < 0.4 = well distributed
        # Use neutral score (3.0) if data unavailable
        pl.when(pl.col("gini_coefficient").is_null()).then(3.0)
          .when(pl.col("gini_coefficient") > 0.85).then(5.0)
          .when(pl.col("gini_coefficient") > 0.75).then(4.0)
          .when(pl.col("gini_coefficient") > 0.6).then(3.0)
          .when(pl.col("gini_coefficient") > 0.4).then(2.0)
          .otherwise(1.0)
          .alias("risk_gini"),
        
        # Top contributor concentration risk (scale 0-5)
        # High risk if top 1 owns >50% OR top 3 own >80%
        # Use neutral score (3.0) if data unavailable
        pl.when(pl.col("top1_share").is_null() | pl.col("top3_share").is_null()).then(3.0)
          .when((pl.col("top1_share") > 0.5) | (pl.col("top3_share") > 0.8)).then(5.0)
          .when((pl.col("top1_share") > 0.4) | (pl.col("top3_share") > 0.7)).then(4.0)
          .when((pl.col("top1_share") > 0.3) | (pl.col("top3_share") > 0.6)).then(3.0)
          .when((pl.col("top1_share") > 0.2) | (pl.col("top3_share") > 0.5)).then(2.0)
          .otherwise(1.0)
          .alias("risk_concentration"),
    ])
    
    # Combine bus factor risks (average of gini and concentration)
    df = df.with_columns([
        ((pl.col("risk_gini") + pl.col("risk_concentration")) / 2.0).alias("risk_bus_factor"),
    ])
    
    df = df.with_columns([
        (pl.col("risk_velocity") + pl.col("risk_bus_factor")).alias("total_risk_score")
    ])

    # 6. Categorization for the Dashboard
    df = df.with_columns([
        pl.when(pl.col("total_risk_score") >= 8).then(pl.lit("CRITICAL"))
          .when(pl.col("total_risk_score") >= 6).then(pl.lit("HIGH"))
          .when(pl.col("total_risk_score") >= 4).then(pl.lit("MEDIUM"))
          .otherwise(pl.lit("LOW"))
          .alias("risk_level")
    ])

    # Sort by risk (highest first) to surface at-risk projects
    return df.sort("total_risk_score", descending=True)
