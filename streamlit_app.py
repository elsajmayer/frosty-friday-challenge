import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as F
from datetime import datetime
import pandas as pd

st.set_page_config(
    page_icon="🧊",
    layout="wide",
)

session = get_active_session()

medals = ["🥇", "🥈", "🥉"]
encouraging_emoji = "👍"

def format_time(seconds):
    return f"` {datetime.utcfromtimestamp(int(seconds)).strftime('%M:%S')} `"

# Collect and process the results
results = (
    session.table("FF_RESULTS")
        .select(F.col("NAME"), F.col("TIME"))
        .sort(F.col("TIME").asc())
).collect()

processed_results = []
for idx, result in enumerate(results):
    result_dict = result.as_dict()
    result_dict["NAME"] = result_dict["NAME"].upper()  # Convert name to uppercase
    if idx < 3:
        result_dict["Position"] = medals[idx]
    else:
        result_dict["Position"] = encouraging_emoji
    result_dict["FORMATTED_TIME"] = format_time(result_dict["TIME"])
    processed_results.append(result_dict)

df = pd.DataFrame(processed_results)

# Collect all row components
row_components = []
for idx, row in df.iterrows():
    row_components.append((row['Position'], row['NAME'], row['FORMATTED_TIME']))



# Render the title and headers
st.title("Snow Summit 24 :snowflake:")
st.title("FROSTY_FRIDAY() Leaderboard :polar_bear:")
c = st.container()
c.divider()
header_cols = c.columns([0.1, 0.45, 0.45])
header_cols[1].markdown("## **🔖 :grey[NAME]**")
header_cols[2].markdown("## **⏳ :grey[TIME]**")
c.divider()


# Render all rows at once
for position, name, formatted_time in row_components:
    cols = st.columns([0.1, 0.45, 0.45])
    cols[0].markdown(f"## {position}")
    cols[1].markdown(f"### :blue[{name}]")
    cols[2].markdown(f"### {formatted_time}")

st.divider()
