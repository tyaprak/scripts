import os
import re
import psycopg2
import pandas as pd
import chardet
import logging


def setup_logging():
    logging.basicConfig(filename="processing.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def process_file(file_path):
    logging.info(f"Processing file: {file_path}")

    # Detect the encoding of the file
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    encoding = result['encoding']
    logging.info(f"Detected encoding: {encoding}")

    # Load the file into a DataFrame
    df = pd.read_csv(file_path, delimiter=':', encoding=encoding, header=None, names=['email', 'pwd'])
    df['email'] = df['email'].str.lower()

    # Filter out invalid email lines and save them to a file
    invalid_lines = df[~df['email'].str.match(r"[^@]+@[^@]+\.[^@]+")]
    if not invalid_lines.empty:
        with open("rotten_data.txt", "a") as f:
            f.write(f"\nInvalid lines in file: {file_path}\n")
            f.write(invalid_lines.to_string(header=False, index=False))
            f.write("\n")
        df = df.drop(invalid_lines.index)

    # Insert the valid data into the database
    conn = psycopg2.connect(database="mydatabase", user="myuser", password="mypassword", host="localhost", port="5432")
    cur = conn.cursor()
    max_count = 10000  # set a maximum count for bulk insert

    for i in range(0, len(df), max_count):
        rows = df.iloc[i:i + max_count]
        data = [tuple(x) for x in rows.to_numpy()]
        query = "INSERT INTO mytable (email, pwd) VALUES %s"
        cur.execute(query, (data,))
        conn.commit()

    cur.close()
    conn.close()
    logging.info(f"Finished processing file: {file_path}")


def main():
    # Set up logging
    setup_logging()

    # Set the directory containing the txt files
    dir_path = "path/to/txt/files"

    # Process each file in the directory
    for file_name in os.listdir(dir_path):
        if file_name.endswith(".txt"):
            file_path = os.path.join(dir_path, file_name)
            process_file(file_path)


if __name__ == "__main__":
    main()
