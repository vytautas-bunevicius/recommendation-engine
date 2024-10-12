# Netflix-like Recommender System: Setup and Usage Guide

## Prerequisites

1. Python 3.7+
2. PostgreSQL database
3. pip (Python package manager)

## Setup

1. Clone the repository to your local machine.

2. Create a virtual environment and activate it:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

4. Set up the PostgreSQL database:
   - Create a new database named `netflix_recommender`
   - Update the database connection details in `src/database/connection.py` if necessary

5. Create the database schema:
   - Run the SQL commands in `src/database/schema.sql` in your PostgreSQL database

## Running the Project

### Step 1: Download and Process IMDb Data

1. Run the download script:
   ```
   bash scripts/download_data.sh
   ```
   This will download the IMDb datasets and convert them from TSV to CSV format.

2. Process the IMDb data:
   ```
   python scripts/process_imdb_data.py
   ```
   This script will read the CSV files and populate the database tables.

### Step 2: Generate Synthetic User Data

Run the user data generation script:
```
python scripts/generate_user_data.py
```
This will create synthetic users and viewing history in the database.

### Step 3: Start the API Server

Run the Flask application:
```
python src/api/main.py
```
The API server will start, typically on `http://localhost:5000`.

## How It Works

1. **Data Ingestion**:
   - IMDb data is downloaded and processed to populate the `movies`, `persons`, and `movie_crew` tables.
   - Synthetic user data and viewing history are generated to simulate a real-world scenario.

2. **API Endpoints**:
   - `/movies`: Get a list of movies or search for movies
   - `/movies/<movie_id>`: Get details of a specific movie
   - `/users/<user_id>`: Get user information
   - `/users/<user_id>/viewing_history`: Get a user's viewing history
   - `/users/<user_id>/recommendations`: Get personalized movie recommendations for a user
   - `/movies/<movie_id>/similar`: Get movies similar to a given movie

3. **Recommendation System**:
   - The content-based recommender uses TF-IDF vectorization on movie titles and genres.
   - Cosine similarity is calculated between movies to find similar content.
   - For user recommendations, the system considers the user's viewing history and finds similar movies.
   - For movie similarity, it compares a given movie with all other movies in the database.

## Testing the API

You can use tools like cURL or Postman to test the API endpoints. Here are some example requests:

1. Get a list of movies:
   ```
   GET http://localhost:5000/movies?limit=10&offset=0
   ```

2. Search for movies:
   ```
   GET http://localhost:5000/movies/search?query=inception
   ```

3. Get user recommendations (replace `<user_id>` with an actual UUID):
   ```
   GET http://localhost:5000/users/<user_id>/recommendations
   ```

4. Get similar movies (replace `<movie_id>` with an actual movie ID):
   ```
   GET http://localhost:5000/movies/<movie_id>/similar
   ```

## Next Steps

1. Implement user authentication and authorization.
2. Add a simple frontend to interact with the API.
3. Implement caching to improve performance.
4. Add more advanced recommendation algorithms (e.g., collaborative filtering).
5. Set up unit tests and integration tests.
