#!/bin/bash

echo "Downloading IMDb datasets..."

# Create the data directory if it doesn't exist
mkdir -p data

# Download title.basics.tsv.gz
curl -o data/title.basics.tsv.gz https://datasets.imdbws.com/title.basics.tsv.gz

# Download title.ratings.tsv.gz
curl -o data/title.ratings.tsv.gz https://datasets.imdbws.com/title.ratings.tsv.gz

echo "Data download complete."

# Unzipping the files
echo "Unzipping IMDb datasets..."

gunzip -f data/title.basics.tsv.gz
gunzip -f data/title.ratings.tsv.gz

echo "Unzipping complete. Data is ready for use."
