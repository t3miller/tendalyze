import os
import tempfile

import streamlit as st

from etl.ingest_hudl_csv import load_hudl_csv


def main():
    st.title("Tendalyze")

    st.write("Upload a Hudl-style play-by-play CSV and Tendalyze will load it "
             "into the database for use in your Tableau dashboards.")

    db_url = os.getenv("DATABASE_URL")
    if db_url:
        st.success("DATABASE_URL is set. Backend can talk to Neon. ✅")
    else:
        st.error("DATABASE_URL is NOT set on this machine. ❌")
        st.stop()

    st.markdown("---")

    uploaded_file = st.file_uploader(
        "Choose a Hudl CSV file",
        type=["csv"],
        help="Export a play-by-play CSV from Hudl and upload it here."
    )

    if uploaded_file is not None:
        st.write(f"File selected: **{uploaded_file.name}**")

        if st.button("Ingest this game into Tendalyze"):
            # Save uploaded file to a temporary location
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name

            try:
                load_hudl_csv(tmp_path)
                st.success("Ingestion complete! Plays have been loaded into Neon. ✅")
            except Exception as e:
                st.error(f"Error while ingesting this file: {e}")


if __name__ == "__main__":
    main()
