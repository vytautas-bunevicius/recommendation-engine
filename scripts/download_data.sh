#!/bin/bash

echo "Downloading IMDb datasets..."

# Create the data directory if it doesn't exist
mkdir -p data

# Download datasets
datasets=(
  "title.basics.tsv.gz"
  "title.ratings.tsv.gz"
  "name.basics.tsv.gz"
  "title.crew.tsv.gz"
)

for dataset in "${datasets[@]}"; do
  echo "Downloading $dataset..."
  curl -o "data/$dataset" "https://datasets.imdbws.com/$dataset"
done

echo "Data download complete."

# Unzipping the files
echo "Unzipping IMDb datasets..."

for dataset in "${datasets[@]}"; do
  echo "Unzipping $dataset..."
  gunzip -f "data/${dataset}"
done

echo "Unzipping complete."

# Convert TSV to CSV
echo "Converting TSV files to CSV..."

for tsv_file in data/*.tsv; do
  csv_file="${tsv_file%.tsv}.csv"
  echo "Converting $tsv_file to $csv_file..."
  sed 's/\t/,/g' "$tsv_file" > "$csv_file"
  rm "$tsv_file"  # Remove the original TSV file
done

echo "Conversion complete. CSV files are ready for use."
