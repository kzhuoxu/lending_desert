import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="Lending Desert Discovery", layout="wide")


@st.cache_resource
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id)


@st.cache_data(ttl=3600)
def load_msa_labels():
    """Returns (md_to_metro, metro_names, metro_to_mds).

    md_to_metro: {msa_md int → short_name str}
    metro_names: sorted list of unique metro display names
    metro_to_mds: {short_name → [msa_md, ...]}  (Boston/LA have 2 MD codes each)
    """
    client = get_bq_client()
    query = """
    SELECT msa_md, short_name
    FROM `{project}.lending_desert.msa_lookup`
    GROUP BY msa_md, short_name
    ORDER BY short_name
    """.format(project=client.project)
    df = client.query(query).to_dataframe()
    md_to_metro = dict(zip(df["msa_md"], df["short_name"]))
    metro_names = sorted(df["short_name"].unique())
    metro_to_mds = df.groupby("short_name")["msa_md"].apply(list).to_dict()
    return md_to_metro, metro_names, metro_to_mds


@st.cache_data(ttl=3600)
def load_data():
    client = get_bq_client()
    query = """
    SELECT * FROM `{project}.lending_desert.mrt_opportunity_score`
    """.format(project=client.project)
    return client.query(query).to_dataframe()


@st.cache_data(ttl=86400)
def load_tract_centroids():
    """Load Census TIGER/Line 2022 gazetteer for census tract lat/lon centroids."""
    import os

    path = os.path.join(os.path.dirname(__file__), "2022_Gaz_tracts_national.txt")
    gaz = pd.read_csv(path, sep="\t", dtype={"GEOID": str})
    # Census gazetteer pads the final column header with trailing whitespace.
    gaz.columns = gaz.columns.str.strip()
    gaz = gaz[["GEOID", "INTPTLAT", "INTPTLONG"]]
    return gaz.rename(columns={"GEOID": "census_tract", "INTPTLAT": "lat", "INTPTLONG": "lon"})


def main():
    st.title("Lending Desert Discovery Engine")
    st.markdown(
        "Identifying census tracts with high mortgage demand but high denial rates "
        "due to appraisal bias — where a new lender can have the most impact."
    )

    md_to_metro, metro_names, metro_to_mds = load_msa_labels()
    df = load_data()
    df["msa_label"] = df["msa_md"].map(md_to_metro)

    # --- Filters ---
    selected_msa = st.selectbox(
        "Select Metro Area",
        options=metro_names,
        index=0,
    )
    # Boston and LA each have 2 MD codes — use isin() to capture both divisions
    msa_mds = metro_to_mds[selected_msa]
    filtered = df[df["msa_md"].isin(msa_mds)]

    col1, col2, col3 = st.columns(3)
    col1.metric("Census Tracts", len(filtered))
    col2.metric("Avg Denial Rate", f"{filtered['denial_rate'].mean():.1%}")
    col3.metric("Avg Opportunity Score", f"{filtered['opportunity_score'].mean():.3f}")

    # --- Tile 1: Opportunity Score Map ---
    st.subheader("Opportunity Score by Census Tract")
    st.caption(
        "Higher scores indicate tracts where banks are failing residents due to "
        "appraisal bias — prime targets for a new loan product."
    )

    centroids = load_tract_centroids()
    map_df = filtered.merge(centroids, on="census_tract", how="left").dropna(subset=["lat", "lon"])
    # Plotly cannot handle pandas nullable Int64; cast to float for the size encoding.
    map_df["total_applications"] = map_df["total_applications"].astype(float)

    if not map_df.empty:
        fig_map = px.scatter_mapbox(
            map_df,
            lat="lat",
            lon="lon",
            color="opportunity_score",
            size="total_applications",
            size_max=20,
            hover_name="tract_name",
            hover_data={
                "census_tract": True,
                "denial_rate": ":.1%",
                "minority_pct": ":.1%",
                "opportunity_score": True,
                "lat": False,
                "lon": False,
            },
            color_continuous_scale="Reds",
            zoom=8,
            mapbox_style="carto-positron",
            title=f"Opportunity Score by Tract — {selected_msa}",
        )
        fig_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0}, height=500)
        st.plotly_chart(fig_map, use_container_width=True)

    st.dataframe(
        filtered.nlargest(50, "opportunity_score")[
            ["census_tract", "tract_name", "opportunity_score", "denial_rate",
             "collateral_denial_pct", "minority_pct", "median_household_income",
             "total_applications"]
        ].reset_index(drop=True),
        use_container_width=True,
    )

    # --- Tile 2: Denial Reasons by Race Group ---
    st.subheader("Why They Say No — Denial Reasons by Tract Demographics")
    st.caption(
        "Comparing denial reasons in minority-majority tracts vs white-majority tracts. "
        "'Collateral' denials driven by appraisal bias are the key signal."
    )

    minority_tracts = filtered[filtered["minority_pct"] > 0.5]
    white_tracts = filtered[filtered["minority_pct"] <= 0.5]

    denial_reasons = ["Collateral", "Debt-to-Income", "Credit History", "Employment", "Insufficient Cash"]

    minority_counts = [
        minority_tracts["collateral_denials"].sum(),
        minority_tracts["dti_denials"].sum(),
        minority_tracts["credit_history_denials"].sum(),
        minority_tracts["employment_denials"].sum(),
        minority_tracts["insufficient_cash_denials"].sum(),
    ]
    white_counts = [
        white_tracts["collateral_denials"].sum(),
        white_tracts["dti_denials"].sum(),
        white_tracts["credit_history_denials"].sum(),
        white_tracts["employment_denials"].sum(),
        white_tracts["insufficient_cash_denials"].sum(),
    ]

    minority_total = sum(minority_counts) or 1
    white_total = sum(white_counts) or 1

    chart_df = pd.DataFrame({
        "Denial Reason": denial_reasons * 2,
        "Percentage": [c / minority_total * 100 for c in minority_counts]
                     + [c / white_total * 100 for c in white_counts],
        "Tract Type": ["Minority-Majority"] * 5 + ["White-Majority"] * 5,
    })

    fig_bar = px.bar(
        chart_df,
        x="Denial Reason",
        y="Percentage",
        color="Tract Type",
        barmode="group",
        title=f"Denial Reason Distribution — {selected_msa}",
        labels={"Percentage": "% of Denials"},
        color_discrete_map={"Minority-Majority": "#e74c3c", "White-Majority": "#3498db"},
    )
    st.plotly_chart(fig_bar, use_container_width=True)


if __name__ == "__main__":
    main()
