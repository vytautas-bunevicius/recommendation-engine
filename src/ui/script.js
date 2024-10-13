const API_BASE_URL = 'http://127.0.0.1:5000';

async function fetchAPI(endpoint, method = 'GET', body = null) {
    const options = {
        method,
        headers: {
            'Content-Type': 'application/json',
        },
    };

    if (body) {
        options.body = JSON.stringify(body);
    }

    const response = await fetch(`${API_BASE_URL}${endpoint}`, options);
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }
    return response.json();
}

function showLoading(elementId) {
    const element = document.getElementById(elementId);
    element.innerHTML = '<div class="loading-spinner"></div>';
}

function hideLoading(elementId) {
    const element = document.getElementById(elementId);
    element.innerHTML = '';
}

async function loadUsers() {
    try {
        const users = await fetchAPI('/users');
        console.log('Users:', users);
        const userSelect = document.getElementById('user-select');
        users.forEach(user => {
            const option = document.createElement('option');
            option.value = user.id;
            option.textContent = user.name;
            userSelect.appendChild(option);
        });
    } catch (error) {
        console.error('Error loading users:', error);
    }
}

async function getRecommendations() {
    const userId = document.getElementById('user-select').value;
    if (!userId) {
        alert('Please select a user');
        return;
    }

    showLoading('recommendation-list');
    try {
        const recommendations = await fetchAPI(`/users/${userId}/recommendations`);
        console.log('Recommendations:', recommendations);
        displayMovies(recommendations, 'recommendation-list');
    } catch (error) {
        console.error('Error getting recommendations:', error);
        alert('Error getting recommendations. Please check the console for details.');
    } finally {
        hideLoading('recommendation-list');
    }
}

async function searchMovies() {
    const query = document.getElementById('search-query').value;
    if (!query) {
        alert('Please enter a search query');
        return;
    }

    showLoading('search-results');
    try {
        const movies = await fetchAPI(`/movies/search?query=${encodeURIComponent(query)}`);
        console.log('Search results:', movies);
        displayMovies(movies, 'search-results');
    } catch (error) {
        console.error('Error searching movies:', error);
        alert('Error searching movies. Please check the console for details.');
    } finally {
        hideLoading('search-results');
    }
}

async function getViewingHistory() {
    const userId = document.getElementById('user-select').value;
    if (!userId) {
        alert('Please select a user');
        return;
    }

    showLoading('history-list');
    try {
        const history = await fetchAPI(`/users/${userId}/viewing_history`);
        console.log('Viewing history:', history);
        displayMovies(history, 'history-list');
    } catch (error) {
        console.error('Error getting viewing history:', error);
        alert('Error getting viewing history. Please check the console for details.');
    } finally {
        hideLoading('history-list');
    }
}

function displayMovies(movies, containerId) {
    const container = document.getElementById(containerId);
    console.log('Displaying movies:', movies);
    if (!Array.isArray(movies) || movies.length === 0) {
        container.innerHTML = '<p>No movies found.</p>';
        return;
    }

    container.innerHTML = movies.map(movie => {
        // Check if it's a viewing history item
        if (movie.movie_id) {
            return `
                <div class="movie-item">
                    <h3>${movie.title || 'No Title'}</h3>
                    <p>Watched on: ${new Date(movie.watch_date).toLocaleDateString()}</p>
                    <p>Duration: ${movie.watch_duration} minutes</p>
                </div>
            `;
        } else {
            // For recommendations and search results
            return `
                <div class="movie-item">
                    <h3>${movie.title || 'No Title'}</h3>
                    <p>Year: ${movie.start_year || 'N/A'}</p>
                    <p>Rating: ${movie.avg_rating ? movie.avg_rating.toFixed(1) : 'N/A'}</p>
                </div>
            `;
        }
    }).join('');
}

document.addEventListener('DOMContentLoaded', () => {
    loadUsers();
    document.getElementById('get-recommendations').addEventListener('click', getRecommendations);
    document.getElementById('search-button').addEventListener('click', searchMovies);
    document.getElementById('get-history').addEventListener('click', getViewingHistory);
});
