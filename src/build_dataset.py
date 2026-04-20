from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

JAN_LISTINGS_PATH = DATA_DIR / "listings_2026_01.csv"
FEB_LISTINGS_PATH = DATA_DIR / "listings_2026_02.csv"
JAN_REVIEWS_PATH = DATA_DIR / "reviews_2026_01.csv"
FEB_REVIEWS_PATH = DATA_DIR / "reviews_2026_02.csv"
OUTPUT_PATH = DATA_DIR / "full_model_dataset.csv"


STATIC_LISTING_COLUMNS = [
    "name",
    "host_id",
    "host_profile_id",
    "host_name",
    "neighbourhood_group",
    "neighbourhood",
    "latitude",
    "longitude",
    "room_type",
]

LAG_LISTING_COLUMNS = [
    "price",
    "minimum_nights",
    "number_of_reviews",
    "last_review",
    "reviews_per_month",
    "calculated_host_listings_count",
    "availability_365",
    "number_of_reviews_ltm",
    "license",
]


def agg_reviews(df, cutoff, month_label):
    df = df.copy()
    df = df[df["date"] <= cutoff].copy()

    out = (
        df.groupby("listing_id")
        .agg(
            total_reviews=("date", "count"),
            first_review_date=("date", "min"),
            last_review_date=("date", "max"),
        )
        .reset_index()
    )

    for days in [30, 90, 365]:
        recent_reviews = (
            df[(df["date"] >= cutoff - pd.Timedelta(days=days)) & (df["date"] <= cutoff)]
            .groupby("listing_id")
            .size()
            .rename(f"reviews_last_{days}d")
            .reset_index()
        )
        out = out.merge(recent_reviews, on="listing_id", how="left")

    review_count_cols = ["reviews_last_30d", "reviews_last_90d", "reviews_last_365d"]
    out[review_count_cols] = out[review_count_cols].fillna(0).astype(int)
    out["days_since_last_review"] = (cutoff - out["last_review_date"]).dt.days

    out = out.rename(columns={"listing_id": "id"})
    return out.add_suffix(f"_{month_label}").rename(columns={f"id_{month_label}": "id"})


def merge_listing_snapshots(jan_listings, feb_listings):
    merged = feb_listings.merge(
        jan_listings,
        on="id",
        how="inner",
        suffixes=("_feb", "_jan"),
    )

    output = pd.DataFrame({"id": merged["id"]})

    for col in STATIC_LISTING_COLUMNS:
        feb_col = f"{col}_feb"
        jan_col = f"{col}_jan"
        if feb_col in merged.columns and jan_col in merged.columns:
            output[col] = merged[feb_col].combine_first(merged[jan_col])
        elif feb_col in merged.columns:
            output[col] = merged[feb_col]
        elif jan_col in merged.columns:
            output[col] = merged[jan_col]
        elif col in merged.columns:
            output[col] = merged[col]

    for col in LAG_LISTING_COLUMNS:
        feb_col = f"{col}_feb"
        jan_col = f"{col}_jan"
        if feb_col in merged.columns:
            output[col] = merged[feb_col]
        if jan_col in merged.columns:
            output[f"{col}_jan"] = merged[jan_col]

    return output


def main():
    jan_listings = pd.read_csv(JAN_LISTINGS_PATH)
    feb_listings = pd.read_csv(FEB_LISTINGS_PATH)
    jan_reviews = pd.read_csv(JAN_REVIEWS_PATH, parse_dates=["date"])
    feb_reviews = pd.read_csv(FEB_REVIEWS_PATH, parse_dates=["date"])

    jan_reviews_agg = agg_reviews(
        jan_reviews,
        cutoff=pd.Timestamp("2026-01-31"),
        month_label="jan",
    )
    feb_reviews_agg = agg_reviews(
        feb_reviews,
        cutoff=pd.Timestamp("2026-02-28"),
        month_label="feb",
    )

    full_df = merge_listing_snapshots(jan_listings, feb_listings)
    full_df = full_df.merge(jan_reviews_agg, on="id", how="left")
    full_df = full_df.merge(feb_reviews_agg, on="id", how="left")

    count_cols = [
        col
        for col in full_df.columns
        if col.startswith("total_reviews_") or col.startswith("reviews_last_")
    ]
    full_df[count_cols] = full_df[count_cols].fillna(0).astype(int)

    full_df.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {full_df.shape[0]:,} rows x {full_df.shape[1]:,} columns to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
