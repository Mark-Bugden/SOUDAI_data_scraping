from config import INTERIM_DIR, PROCESSED_DIR
from preprocessing.utils.cleaning import clean_date_data, clean_remaining_data
from preprocessing.utils.io import load_csv, save_csv
from preprocessing.utils.missing import handle_missing_values
from preprocessing.utils.outliers import remove_outliers
from preprocessing.utils.target import create_target_variable


def main():
    path_to_csv = INTERIM_DIR / "infosoud_checkpoint.csv"
    output_path = PROCESSED_DIR / "processed_decisions.csv"

    print(f"Loading CSV from: {path_to_csv}")
    df = load_csv(path_to_csv)

    print("Handling missing values...")
    df = handle_missing_values(df)

    print("Cleaning data...")
    df = clean_date_data(df)
    df = clean_remaining_data(df)

    print("Creating target variable...")
    df = create_target_variable(df)

    print("Removing outliers...")
    df = remove_outliers(df, years=5)

    print(f"Saving cleaned CSV to: {output_path}")
    save_csv(df, output_path)

    print("Done.")


if __name__ == "__main__":
    main()
