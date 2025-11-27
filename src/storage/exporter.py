import pandas as pd


def export(df, path: str):
    try:
        if path.endswith(".csv"):
            df.to_csv(path, index=False)
            print(f"Data exported to {path}")
        elif path.endswith((".xlsx", ".xls")):
            df.to_excel(path, index=False)
            print(f"Data exported to {path}")
        else:
            raise ValueError("Unsupported format. Use .csv or .xlsx/.xls")
    except Exception as e:
        print(f"Error exporting: {e}")

