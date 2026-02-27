import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="Lending Desert Discovery", layout="wide")

MSA_LABELS = {
    14460: "Boston",
    12060: "Atlanta",
    13820: "Birmingham, AL",
    31080: "Los Angeles",
    40900: "Sacramento",
}


@st.cache_resource
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id)


@st.cache_data(ttl=3600)
def load_data():
    client = get_bq_client()
    query = """
    SELECT * FROM `{project}.lending_desert.mrt_opportunity_score`
    """.format(project=client.project)
    return client.query(query).to_dataframe()


def main():
    st.title("Lending Desert Discovery Engine")
    st.markdown(
        "Identifying census tracts with high mortgage demand but high denial rates "
        "due to appraisal bias — where a new lender can have the most impact."
    )

    df = load_data()
    df["msa_label"] = df["msa_md"].map(MSA_LABELS)

    # --- Filters ---
    selected_msa = st.selectbox(
        "Select Metro Area",
        options=list(MSA_LABELS.values()),
        index=0,
    )
    msa_code = [k for k, v in MSA_LABELS.items() if v == selected_msa][0]
    filtered = df[df["msa_md"] == msa_code]

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

    # NOTE: Map requires lat/lon tract centroids — join with Census TIGER/Line gazetteer:
    #   https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2022_Gazetteer/2022_Gaz_tracts_national.txt
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

    # Split tracts into minority-majority (>50%) vs white-majority
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

    # Normalize to percentages
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
