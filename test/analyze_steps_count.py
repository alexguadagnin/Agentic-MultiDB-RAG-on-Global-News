import argparse
import pandas as pd


def main():
    parser = argparse.ArgumentParser(
        description="Calcola media e mediana di steps_count per category e dataset_size."
    )
    parser.add_argument("csv_path", help="Percorso del file CSV")
    parser.add_argument(
        "--exclude-errors",
        action="store_true",
        help="Esclude le righe con is_error=True",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="(Opzionale) Percorso CSV di output con i risultati aggregati",
    )
    args = parser.parse_args()

    df = pd.read_csv(args.csv_path)

    # Ensure correct dtypes
    df["steps_count"] = pd.to_numeric(df["steps_count"], errors="coerce")
    if "is_error" in df.columns:
        # se is_error è già boolean ok; se è stringa tipo "False"/"True" lo normalizziamo
        if df["is_error"].dtype == object:
            df["is_error"] = df["is_error"].astype(str).str.strip().str.lower().map({"true": True, "false": False})

    if args.exclude_errors and "is_error" in df.columns:
        df = df[df["is_error"] == False]  # noqa: E712

    # Drop rows without steps_count
    df = df.dropna(subset=["steps_count"])

    agg = (
        df.groupby(["category", "dataset_size"], as_index=False)
          .agg(
              n=("steps_count", "size"),
              mean_steps=("steps_count", "mean"),
              median_steps=("steps_count", "median"),
          )
          .sort_values(["category", "dataset_size"])
    )

    # arrotondamenti (facoltativi)
    agg["mean_steps"] = agg["mean_steps"].round(3)
    agg["median_steps"] = agg["median_steps"].round(3)

    print(agg.to_string(index=False))

    if args.out:
        agg.to_csv(args.out, index=False)
        print(f"\nSalvato output aggregato in: {args.out}")


if __name__ == "__main__":
    main()
