-- Table to store movie information
CREATE TABLE movies (
    id VARCHAR(10) PRIMARY KEY,
    title VARCHAR(512) NOT NULL,
    original_title VARCHAR(512),
    type VARCHAR(50),
    is_adult BOOLEAN,
    start_year INTEGER,
    end_year INTEGER,
    runtime INTEGER,
    genres VARCHAR(255)
);
-- Table to store person information
CREATE TABLE persons (
    id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    birth_year INTEGER,
    death_year INTEGER,
    professions VARCHAR(255)
);
-- Table to store relationships between movies and crew members
CREATE TABLE movie_crew (
    movie_id VARCHAR(10) REFERENCES movies(id),
    person_id VARCHAR(10) REFERENCES persons(id),
    role VARCHAR(50),
    PRIMARY KEY (movie_id, person_id, role)
);
-- Table to store director information
CREATE TABLE directors (
    movie_id VARCHAR(10) REFERENCES movies(id),
    person_id VARCHAR(10) REFERENCES persons(id),
    PRIMARY KEY (movie_id, person_id)
);
-- Table to store writer information
CREATE TABLE writers (
    movie_id VARCHAR(10) REFERENCES movies(id),
    person_id VARCHAR(10) REFERENCES persons(id),
    PRIMARY KEY (movie_id, person_id)
);
-- Table to store known for titles
CREATE TABLE known_for_titles (
    person_id VARCHAR(10) REFERENCES persons(id),
    movie_id VARCHAR(10) REFERENCES movies(id),
    PRIMARY KEY (person_id, movie_id)
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
-- Table to store movie ratings
CREATE TABLE movie_ratings (
    movie_id VARCHAR(10) PRIMARY KEY REFERENCES movies(id),
    avg_rating FLOAT,
    num_votes INTEGER
);
-- Indexes to improve query performance
CREATE INDEX idx_movies_title ON movies(title);
CREATE INDEX idx_movies_original_title ON movies(original_title);
CREATE INDEX idx_movies_type ON movies(type);
CREATE INDEX idx_movies_start_year ON movies(start_year);
CREATE INDEX idx_movies_genres ON movies(genres);
CREATE INDEX idx_viewing_history_user_id ON viewing_history(user_id);
CREATE INDEX idx_viewing_history_movie_id ON viewing_history(movie_id);
CREATE INDEX idx_viewing_history_watch_date ON viewing_history(watch_date);
CREATE INDEX idx_movie_ratings_avg_rating ON movie_ratings(avg_rating);
CREATE INDEX idx_persons_name ON persons(name);
CREATE INDEX idx_directors_movie_id ON directors(movie_id);
CREATE INDEX idx_writers_movie_id ON writers(movie_id);
CREATE INDEX idx_known_for_titles_person_id ON known_for_titles(person_id);
CREATE INDEX idx_movies_title_fts ON movies USING gin(to_tsvector('english', title));
