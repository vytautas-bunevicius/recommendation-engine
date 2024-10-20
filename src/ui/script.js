const API_BASE_URL = 'http://127.0.0.1:8000';

function showSpinner(elementId) {
    const container = document.getElementById(elementId);
    const spinnerContainer = document.createElement('div');
    spinnerContainer.className = 'spinner-container';
    spinnerContainer.innerHTML = '<div class="spinner"></div>';
    container.appendChild(spinnerContainer);
}

function hideSpinner(elementId) {
    const container = document.getElementById(elementId);
    const spinnerContainer = container.querySelector('.spinner-container');
    if (spinnerContainer) {
        container.removeChild(spinnerContainer);
    }
}

function showUserSelectSpinner() {
    const userSelectWrapper = document.getElementById('user-select-wrapper');
    const spinner = document.getElementById('user-select-spinner');
    userSelectWrapper.classList.add('loading');
    spinner.classList.remove('hidden');
    document.getElementById('user-select').disabled = true;
}

function hideUserSelectSpinner() {
    const userSelectWrapper = document.getElementById('user-select-wrapper');
    const spinner = document.getElementById('user-select-spinner');
    userSelectWrapper.classList.remove('loading');
    spinner.classList.add('hidden');
    document.getElementById('user-select').disabled = false;
}

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

    try {
        const response = await fetch(`${API_BASE_URL}${endpoint}`, options);
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error(`Error fetching ${endpoint}:`, error);
        throw error;
    }
}

async function loadUsers() {
    showUserSelectSpinner();

    try {
        const users = await fetchAPI('/users');
        const userSelect = document.getElementById('user-select');
        userSelect.innerHTML = '<option value="">Select a user</option>';
        users.forEach(user => {
            const option = document.createElement('option');
            option.value = user.id;
            option.textContent = user.name;
            userSelect.appendChild(option);
        });
    } catch (error) {
        console.error('Error loading users:', error);
        alert('Failed to load users. Please try again later.');
    } finally {
        hideUserSelectSpinner();
    }
}

async function getRecommendations() {
    const userId = document.getElementById('user-select').value;
    if (!userId) return;

    showSpinner('recommendation-list');
    try {
        const recommendations = await fetchAPI(`/users/${userId}/recommendations`);
        displayMovies(recommendations, 'recommendation-list');
    } catch (error) {
        console.error('Error getting recommendations:', error);
        document.getElementById('recommendation-list').innerHTML = '<p class="placeholder-text">Error getting recommendations. Please try again later.</p>';
    } finally {
        hideSpinner('recommendation-list');
    }
}

async function getViewingHistory() {
    const userId = document.getElementById('user-select').value;
    if (!userId) return;

    showSpinner('history-list');
    try {
        const history = await fetchAPI(`/users/${userId}/viewing_history`);
        displayMovies(history, 'history-list', true);
    } catch (error) {
        console.error('Error getting viewing history:', error);
        document.getElementById('history-list').innerHTML = '<p class="placeholder-text">Error getting viewing history. Please try again later.</p>';
    } finally {
        hideSpinner('history-list');
    }
}

async function searchMovies() {
    const query = document.getElementById('search-query').value.trim();
    if (!query) {
        alert('Please enter a search query');
        return;
    }

    showSpinner('search-results');
    try {
        const movies = await fetchAPI(`/movies/search?query=${encodeURIComponent(query)}`);
        displayMovies(movies, 'search-results');
    } catch (error) {
        console.error('Error searching movies:', error);
        document.getElementById('search-results').innerHTML = '<p class="placeholder-text">Error searching movies. Please try again later.</p>';
    } finally {
        hideSpinner('search-results');
    }
}

function displayMovies(movies, containerId, isHistory = false) {
    const container = document.getElementById(containerId);
    if (!Array.isArray(movies) || movies.length === 0) {
        container.innerHTML = '<p class="placeholder-text">No movies found.</p>';
        return;
    }

    container.innerHTML = movies.map(movie => `
        <div class="movie-item">
            <h3>${movie.title}</h3>
            ${movie.start_year ? `<p>Year: ${movie.start_year}</p>` : ''}
            ${movie.avg_rating ? `<p class="rating">Rating: ${movie.avg_rating.toFixed(1)}</p>` : ''}
            ${movie.genres ? `<p>Genres: ${movie.genres}</p>` : ''}
            ${isHistory ? `
                <p>Watched on: ${new Date(movie.watch_date).toLocaleDateString()}</p>
                <p>Duration: ${movie.watch_duration} minutes</p>
            ` : ''}
        </div>
    `).join('');
}

document.addEventListener('DOMContentLoaded', () => {
    loadUsers();
    document.getElementById('user-select').addEventListener('change', () => {
        const userId = document.getElementById('user-select').value;
        if (userId) {
            getRecommendations();
            getViewingHistory();
        } else {
            document.getElementById('recommendation-list').innerHTML = '<p class="placeholder-text">Select a user to see recommendations</p>';
            document.getElementById('history-list').innerHTML = '<p class="placeholder-text">Select a user to see viewing history</p>';
        }
    });
    document.getElementById('search-button').addEventListener('click', searchMovies);
    document.getElementById('search-query').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            searchMovies();
        }
    });
});
