import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import subprocess
import sys

# ---------- CONFIG ----------
base_dir = Path(__file__).parent        # folder where the script is
input_folder = base_dir / "raw_excels"  # where branch Excel files are saved
output_folder = base_dir / "output"     # where master, reports, and plots go

master_filename = output_folder / "master_database.xlsx"
error_report_filename = output_folder / "error_report.csv"
top_reasons_png = output_folder / "top3_reasons.png"
delivered_vs_returned_png = output_folder / "delivered_vs_returned_by_product.png"

# Auto-create folders if missing
input_folder.mkdir(parents=True, exist_ok=True)   # Make sure input folder exists
output_folder.mkdir(parents=True, exist_ok=True)  # Make sure output folder exists

# ---------- OPEN FILE HELPER ----------
def open_file(path):
    """Open a file with the default app depending on the OS"""
    try:
        if sys.platform.startswith("darwin"):   # macOS
            subprocess.call(["open", path])
        elif os.name == "nt":                   # Windows
            os.startfile(path)
        elif os.name == "posix":                # Linux
            subprocess.call(["xdg-open", path])
    except Exception as e:
        print(f"Could not open {path}: {e}")

# ---------- SCHEMA ----------
columns = {
    "Plant": str,
    "Plant Code": "Int64",                
    "Date Delivered": "datetime64[D]",
    "Date Returned": "datetime64[D]",
    "Customer": str,
    "Customer Category": str,
    "Product": str,
    "Product Code": str,
    "Total Delivered (kgs)": "float64",
    "Total Returned (kgs)": "float64",
    "Reason of Return": str,
    "Return Category": str,
    "Accountability": str,
    "Validation": str,    # expect 'Valid' or 'Invalid'
    "Remarks": str,
}

required_columns = list(columns.keys())

# Allowed values
allowed_validation_vals = {"Valid", "Invalid"}
allowed_accountability_vals = {"Sales", "Processing", "Logistics", "Other"}
allowed_return_category_vals = {"Damaged", "Expired", "Wrong Item", "Other"}    
allowed_plant_vals = {"Plant1", "Plant2", "Plant3", "Plant4"}
allowed_customer_category_vals = {"Hotels", "Supermarkets", "Distributor","Direct", "Others"}

# ---------- HELPERS ----------
def find_input_files(folder):
    exts = ("*.xlsx", "*.xls", "*.csv")
    files = []
    for ext in exts:
        files.extend(glob.glob(os.path.join(folder, ext)))
    return sorted(files)  

def safe_read(path):
    """Try reading excel or csv, return DataFrame or None+error"""
    try:
        if path.lower().endswith(".csv"):
            df = pd.read_csv(path, dtype=str)
        else:
            df = pd.read_excel(path, sheet_name=0, dtype=str)  
        return df, None
    except Exception as e:
        return None, str(e)

def coerce_types(df):
    """Coerce dataframe columns to the desired dtypes in 'columns' dict.
       Returns (df_coerced, issues_list)"""
    issues = []
    df2 = df.copy()
    for col, dtype in columns.items():
        if col not in df2.columns:
            df2[col] = pd.NA
            issues.append(("missing_column", col, None))
            continue
        series = df2[col]
        if dtype in ("datetime64[ns]", "datetime64[D]"):
            coerced = pd.to_datetime(series, errors="coerce").dt.date
            bad_mask = coerced.isna() & series.notna()
            for idx, val in zip(series.index[bad_mask], series[bad_mask]):
                issues.append(("bad_datetime", col, idx, val))
            df2[col] = coerced
        elif dtype in ("float64", "float"):
            coerced = pd.to_numeric(series, errors="coerce")
            bad_mask = coerced.isna() & series.notna()
            for idx, val in zip(series.index[bad_mask], series[bad_mask]):
                issues.append(("bad_float", col, idx, val))
            df2[col] = coerced.astype("float64")
        elif dtype in ("Int64", "int64", "Int32"):
            coerced = pd.to_numeric(series, errors="coerce", downcast="integer")
            bad_mask = coerced.isna() & series.notna()
            for idx, val in zip(series.index[bad_mask], series[bad_mask]):
                issues.append(("bad_int", col, idx, val))
            df2[col] = coerced.astype("Int64")
        else:
            df2[col] = series.astype("string")
    return df2, issues

def validate_row_level(df, source_file):
    """Return list of issues for this df. Each item is dict: file,row,col,issue,value"""
    issues = []
    for idx, row in df.iterrows():
        for col in required_columns:
            if col not in df.columns:
                issues.append({"Plant": None, "row": idx+2, "column": col,
                               "issue": "column_missing", "value": None, "file": source_file})
            else:
                val = row[col]
                if pd.isna(val) and col not in ("Remarks",):
                    issues.append({"Plant": row.get("Plant", None), "row": idx+2,
                                   "column": col, "issue": "missing_value", "value": None, "file": source_file})
        if "Validation" in df.columns:
            v = row.get("Validation")
            if pd.notna(v) and str(v).strip() not in allowed_validation_vals:
                issues.append({"Plant": row.get("Plant", None), "row": idx+2,
                               "column": "Validation", "issue": "invalid_value", "value": v, "file": source_file})
        if "Reason of Return" in df.columns:
            r = row.get("Reason of Return")
            if pd.isna(r) or str(r).strip() == "":
                issues.append({"Plant": row.get("Plant", None), "row": idx+2,
                               "column": "Reason of Return", "issue": "missing_reason", "value": r, "file": source_file })
    return issues

# ---------- MAIN ----------
def main():
    files = find_input_files(input_folder)
    if not files:
        print(f"No input files found in {input_folder}. Put branch files there and re-run.")
        return

    all_frames, error_rows = [], []

    for f in files:
        df_raw, err = safe_read(f)
        if df_raw is None:
            error_rows.append({"Plant": None, "row": None, "column": None,
                               "issue": "read_error", "value": err, "file": f})
            continue
        df_raw.columns = [str(c).strip() for c in df_raw.columns]
        coerced_df, type_issues = coerce_types(df_raw)
        for it in type_issues:
            if it[0] == "missing_column":
                error_rows.append({"Plant": None, "row": None, "column": it[1],
                                   "issue": "missing_column", "value": None, "file": f})
            else:
                kind, col, idx, val = it
                plant_val = None
                if idx is not None and col in coerced_df.columns and idx in coerced_df.index:
                    plant_val = coerced_df.at[idx, "Plant"] if "Plant" in coerced_df.columns else None
                error_rows.append({"Plant": plant_val, "row": idx+2 if idx is not None else None,
                                   "column": col, "issue": kind, "value": val, "file": f})
        row_issues = validate_row_level(coerced_df, f)
        error_rows.extend(row_issues)
        coerced_df["__source_file"] = os.path.basename(f)
        all_frames.append(coerced_df)

    master = pd.concat(all_frames, ignore_index=True, sort=False) if all_frames else pd.DataFrame(columns=required_columns)

    # Save master
    master_out_path = os.path.join(output_folder, master_filename)
    master.to_excel(master_out_path, index=False)
    print("Saved master database to", master_out_path)

    # Save errors
    error_df = pd.DataFrame(error_rows)
    if error_df.empty:
        print("No validation errors found.")
    else:
        err_path = os.path.join(output_folder, error_report_filename)
        error_df.to_csv(err_path, index=False)
        print("Saved error report to", err_path)

    # Auto-open results
    if os.path.exists(master_out_path):
        open_file(master_out_path)
    if not error_df.empty and os.path.exists(err_path):
        open_file(err_path)

    # Charts
    if "Reason of Return" in master.columns:
        reasons = master["Reason of Return"].dropna().astype(str).str.strip()
        top3 = reasons.value_counts().head(3)
        print("Top 3 reasons:\n", top3.to_string())
        plt.figure(figsize=(6,4))
        top3.plot(kind="bar")
        plt.title("Top 3 Reasons for Returns")
        plt.ylabel("Count")
        plt.tight_layout()
        plt.savefig(os.path.join(output_folder, top_reasons_png))
        plt.close()
        print("Saved top reasons chart.")
    if "Product" in master.columns and "Total Delivered (kgs)" in master.columns:
        agg = master.groupby("Product", dropna=True)[["Total Delivered (kgs)","Total Returned (kgs)"]].sum(min_count=1).fillna(0)
        if not agg.empty:
            top_products = agg.sort_values("Total Delivered (kgs)", ascending=False).head(10)
            ax = top_products.plot(kind="bar", figsize=(10,5))
            ax.set_ylabel("Kilograms")
            plt.tight_layout()
            plt.savefig(os.path.join(output_folder, delivered_vs_returned_png))
            plt.close()
            print("Saved delivered vs returned chart.")

    print("Done.")

if __name__ == "__main__":
    main()