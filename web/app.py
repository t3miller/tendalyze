import os
import streamlit as st


def main():
    st.title("Tendalyze")

    st.write("This is your Tendalyze control panel.")

    db_url = os.getenv("DATABASE_URL")
    if db_url:
        st.success("DATABASE_URL is set. Backend can talk to Neon. ✅")
    else:
        st.error("DATABASE_URL is NOT set on this machine. ❌")


if __name__ == "__main__":
    main()
