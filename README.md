# Israel - Palestine conflict reddit analysis

This project analyzes the network structure and sentiment dynamics of conflict discourse across 8 key subreddits (including r/worldnews, r/Israel, and r/Palestine) from August 2023 to March 2024. By mapping user interactions and media consumption, we found that influence is not defined by strategy, but by stamina. We present an analysis of a diverse online frontline driven by a transnational army of hyper-active users who represent the narrative through sheer attrition.

<img width="1524" height="696" alt="israel palestine analysis" src="https://github.com/user-attachments/assets/0bd30f6f-6513-4f5a-ba69-a59f2ebe38e4" />

## Setup

1.  **Install Dependencies**
    Requires Python 3.10+.

    ```bash
    pip install -r requirements.txt
    ```

2.  **Get the Data**
    Download the filtered datasets from Google Drive:
    [**Download Link**](https://drive.google.com/drive/u/0/folders/1hOgGjc1E9yUFeBumeq6Rk2gXB3-e_b_p)

    Place the downloaded files into a folder named `reddit_processed/`.

## How to Run

Execute the Jupyter notebooks in the `notebooks/` folder in this exact order:

1.  `1_joining_of_comments.ipynb` (Merges data)
2.  `1_submissions cleaning.ipynb` (Cleans data)
3.  `eda.ipynb` (Main analysis and visualization)

## Structure

  * `notebooks/` - The core analysis files.
  * `reddit_processed/` - Where your downloaded input data goes.
  * `data_extraction/` - Scripts to process raw AcademicTorrents data (optional, for manual filtering).
  * `paths/` - Utility package for file management.
  * `requirements.txt/`: Lists all required Python packages for dependency management.

**Authors:** Team \#3 (Sukhodolskyi Dmytro & Ocheretianyi Hlieb) — *NaUKMA*
