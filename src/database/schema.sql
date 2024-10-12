-- Schema for the recommendation_engine database.

-- Table to store movie information
CREATE TABLE movies (
    id VARCHAR(10) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    type VARCHAR(50),
    start_year INTEGER,
    end_year INTEGER,
    runtime INTEGER,
    genres VARCHAR(255),
    avg_rating FLOAT,
    num_votes INTEGER
);

-- Table to store person information
CREATE TABLE persons (
    id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    birth_year INTEGER,
    death_year INTEGER,
    primary_profession VARCHAR(255)
);

-- Table to store relationships between movies and crew members
CREATE TABLE movie_crew (
    movie_id VARCHAR(10) REFERENCES movies(id),
    person_id VARCHAR(10) REFERENCES persons(id),
    role VARCHAR(50),
    PRIMARY KEY (movie_id, person_id, role)
);

-- Table to store user information
CREATE TABLE users (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    birth_year INTEGER
);

-- Table to store viewing history
CREATE TABLE viewing_history (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    movie_id VARCHAR(10) REFERENCES movies(id),
    watch_date DATE NOT NULL,
    watch_duration INTEGER NOT NULL
);

-- Indexes to improve query performance
CREATE INDEX idx_movies_title ON movies(title);
CREATE INDEX idx_movies_type ON movies(type);
CREATE INDEX idx_movies_start_year ON movies(start_year);
CREATE INDEX idx_movies_genres ON movies(genres);
CREATE INDEX idx_viewing_history_user_id ON viewing_history(user_id);
CREATE INDEX idx_viewing_history_movie_id ON viewing_history(movie_id);
CREATE INDEX idx_viewing_history_watch_date ON viewing_history(watch_date);
