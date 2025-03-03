import streamlit as st
import pandas as pd
import plotly.express as px
import zipfile
import os
import io
import threading
import time
from scraper import scrape_hs_code

st.set_page_config(page_title="Trade Stats Scraper", page_icon=None)

def to_fiscal_year(year):
    return f"{year}-{str(year + 1)[-2:]}"

def from_fiscal_year(fiscal_year):
    return int(fiscal_year.split("-")[0])

fiscal_years = [to_fiscal_year(y) for y in range(1997, 2025)]
sourceurl = "https://tradestat.commerce.gov.in/"

@st.cache_data
def load_hscode_data():
    try:
        df = pd.read_csv("hscodes.csv", dtype={"HSN_CD": str})
        return df
    except Exception as e:
        st.error(f"Error loading HS code data: {e}")
        return pd.DataFrame()

hscodes_df = load_hscode_data()
my_url = "https://sreedev98.github.io/"
st.title("India Trade Stats Scraper")
st.markdown("_By [Sreedev Krishnakumar](%s)_" % my_url)
st.markdown("This was built to make it easier to scrape HS code wise data of India's trade with other countries over a "
            "larger period of time (which is a cumbersome process in the [Trade Stats portal](%s)). Simply choose the HS "
            "code(s) you want the data for, the years, and the type of trade (import/export) and the program will generate "
            "CSVs that you can download for each HS code with country-wise data, as well as an additional CSV with combined "
            "totals of the selected HS codes, in case there are multiple selected." % sourceurl)

hs_code_selection = st.multiselect("Select HS Code(s):", options=hscodes_df["HSN_CD"].astype(str).tolist(), default=[])
hs_numbers = ','.join(hs_code_selection)
hsurl = "https://www.mca.gov.in/XBRL/pdf/ITC_HS_codes.pdf"
st.markdown(f"(You can refer to the list of HS codes [here]({hsurl}).)")

start_fiscal_year = st.selectbox("Start Year", fiscal_years, index=fiscal_years.index(to_fiscal_year(2015)))
end_fiscal_year = st.selectbox("End Year", fiscal_years, index=fiscal_years.index(to_fiscal_year(2024)))

st.text("(The data for fiscal year 2024-25 is as of November 2024.)")

start_year = from_fiscal_year(start_fiscal_year)
end_year = from_fiscal_year(end_fiscal_year)

trade_type = st.selectbox("Trade Type:", ["Import", "Export"])

# Function to merge HS code data
def merge_hs_code_data(hs_code_selection, trade_type):
    merged_data = {}
    country_data = {}

    for hs_code in hs_code_selection:
        file_path = f"{hs_code}_{trade_type}.csv"
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, index_col=0)
            st.write(f"Loaded data for {hs_code}:", df.head())  # Debugging output
            for year in df.columns:
                if "Total" in df.index:
                    merged_data[year] = merged_data.get(year, 0) + df.at["Total", year]
                for country, value in df[year].items():
                    if country != "Total":
                        if year not in country_data:
                            country_data[year] = {}
                        country_data[year][country] = country_data[year].get(country, 0) + value

    combined_df = pd.DataFrame({"Fiscal Year": list(merged_data.keys()), "Total Trade Value": list(merged_data.values())})
    combined_df.sort_values("Fiscal Year", inplace=True)
    st.write("Combined data:", combined_df.head())  # Debugging output

    csv_path = "/tmp/combined_countrywise_trade.csv"
    if country_data:
        countrywise_df = pd.DataFrame.from_dict(country_data, orient="index").fillna(0)
        countrywise_df.index.name = "Fiscal Year"
        countrywise_df.to_csv(csv_path)
        st.write(f"Country-wise data saved at {csv_path}")  # Debugging output

    return combined_df, csv_path if country_data else None

# Function to schedule file deletion
def schedule_file_deletion(file_paths, delay=10):
    def delete_files():
        time.sleep(delay)
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Deleted file: {file_path}")

    threading.Thread(target=delete_files, daemon=True).start()

st.text("Processing may take a while depending on the number of HS codes and years selected.")

if st.button("Fetch Trade Data"):
    if hs_code_selection:
        all_data_paths = []
        progress_bar = st.progress(0)

        for i, hs_code in enumerate(hs_code_selection):
            data_path = scrape_hs_code(hs_code, start_year, end_year, trade_type)
            if data_path:
                all_data_paths.append(data_path)
            progress_bar.progress(int(((i + 1) / len(hs_code_selection)) * 100))

        combined_df, countrywise_csv_path = merge_hs_code_data(hs_code_selection, trade_type)

        if combined_df is not None:
            st.subheader(f"Trend in {trade_type}s for HS Codes {hs_numbers} during the period {start_fiscal_year} to {end_fiscal_year}")
            fig = px.line(combined_df, x="Fiscal Year", y="Total Trade Value", markers=True)
            fig.update_layout(xaxis_title="Fiscal Year", yaxis_title="Total Trade Value (US$ Million)")
            st.plotly_chart(fig, use_container_width=True)
            
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w") as zip_file:
                for path in all_data_paths:
                    zip_file.write(path, os.path.basename(path))
                if countrywise_csv_path:
                    zip_file.write(countrywise_csv_path, os.path.basename(countrywise_csv_path))
            zip_buffer.seek(0)

            st.download_button(label="Download country-wise data for each of the selected HS code", data=zip_buffer,
                               file_name=f"trade_data_{trade_type}.zip", mime="application/zip")

            schedule_file_deletion(all_data_paths + ([countrywise_csv_path] if countrywise_csv_path else []))
        
        st.success("Scraping complete!")
    else:
        st.error("Please select at least one HS Code before fetching data.")

st.markdown("DISCLAIMER: The data given here is sourced from the data published by the Department of Commerce, Ministry "
            "of Commerce and Industry of India through the [Trade Stats portal](%s). This data is to be used for "
            "reference purposes only. You should refer to official publications by DGCI&S, Kolkata for any further "
            "reference." % sourceurl)
