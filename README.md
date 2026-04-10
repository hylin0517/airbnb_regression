# Airbnb Price Prediction

A portfolio-style data science project for predicting Airbnb listing prices from historical Inside Airbnb listings data.

This repository is intentionally notebook-focused. Data loading, audit work, exploration, modeling, interpretation, and final deliverables are all handled directly in Jupyter notebooks.

## Project Structure

```text
.
├── README.md
├── environment.yml
├── data/
│   └── listings.csv
├── notebooks/
│   ├── 01_data_audit.ipynb
│   └── 02_eda.ipynb
```

## Setup

Create the environment and launch Jupyter:

```bash
conda env create -f environment.yml
conda activate airbnb-reg
jupyter lab
```

## Workflow

- The dataset lives in `data/listings.csv`.
- Notebook paths are kept simple and explicit for easier use in VS Code Jupyter.
- All analysis work is meant to stay in notebooks instead of being split across a Python package.
