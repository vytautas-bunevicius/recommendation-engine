/**
 * Module providing user interaction functions for movie recommendations.
 *
 * This module includes functions to:
 * - Load users into a dropdown for selection.
 * - Show or hide spinners during data loading.
 * - Fetch data from a backend API for recommendations, viewing history, or movie search.
 * - Display movies based on the fetched data.
 *
 * The module interacts with a REST API to retrieve movie-related information and updates the DOM accordingly.
 */

const API_BASE_URL = 'http://127.0.0.1:8000';

/**
 * Show a spinner within a specified container element.
 * @param {string} elementId - The ID of the container element.
 */
function showSpinner(elementId) {
  const container = document.getElementById(elementId);
  const spinnerContainer = document.createElement('div');
  spinnerContainer.className = 'spinner-container';
  spinnerContainer.innerHTML = '<div class="spinner"></div>';
  container.appendChild(spinnerContainer);
}

/**
 * Hide the spinner within a specified container element.
 * @param {string} elementId - The ID of the container element.
 */
function hideSpinner(elementId) {
  const container = document.getElementById(elementId);
  const spinnerContainer = container.querySelector('.spinner-container');
  if (spinnerContainer) {
    container.removeChild(spinnerContainer);
  }
}

/**
 * Show the spinner for the user select dropdown.
 */
function showUserSelectSpinner() {
  const userSelectWrapper = document.getElementById('user-select-wrapper');
  const spinner = document.getElementById('user-select-spinner');
  userSelectWrapper.classList.add('loading');
  spinner.classList.remove('hidden');
  document.getElementById('user-select').disabled = true;
}

/**
 * Hide the spinner for the user select dropdown.
 */
function hideUserSelectSpinner() {
  const userSelectWrapper = document.getElementById('user-select-wrapper');
  const spinner = document.getElementById('user-select-spinner');
  userSelectWrapper.classList.remove('loading');
  spinner.classList.add('hidden');
  document.getElementById('user-select').disabled = false;
}

/**
 * Fetch data from the API.
 * @param {string} endpoint - The API endpoint to fetch.
 * @param {string} [method='GET'] - The HTTP method to use.
 * @param {Object|null} [body=null] - The request body for POST requests.
 * @returns {Promise<Object>} The response data.
 */
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
      throw new Error(
        errorData.detail || `HTTP error! status: ${response.status}`
      );
    }
    return await response.json();
  } catch (error) {
    console.error(`Error fetching ${endpoint}:`, error);
    throw error;
  }
}

/**
 * Load users and populate the user select dropdown.
 */
async function loadUsers() {
  showUserSelectSpinner();

  try {
    const users = await fetchAPI('/users');
    const userSelect = document.getElementById('user-select');
    userSelect.innerHTML = '<option value="">Select a user</option>';
    users.forEach((user) => {
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

/**
 * Get movie recommendations for the selected user.
 */
async function getRecommendations() {
  const userId = document.getElementById('user-select').value;
  if (!userId) return;

  showSpinner('recommendation-list');
  try {
    const recommendations = await fetchAPI(`/users/${userId}/recommendations`);
    displayMovies(recommendations, 'recommendation-list');
  } catch (error) {
    console.error('Error getting recommendations:', error);
    document.getElementById('recommendation-list').innerHTML =
      '<p class="placeholder-text">Error getting recommendations. Please try again later.</p>';
  } finally {
    hideSpinner('recommendation-list');
  }
}

/**
 * Get the viewing history for the selected user.
 */
async function getViewingHistory() {
  const userId = document.getElementById('user-select').value;
  if (!userId) return;

  showSpinner('history-list');
  try {
    const history = await fetchAPI(`/users/${userId}/viewing_history`);
    displayMovies(history, 'history-list', true);
  } catch (error) {
    console.error('Error getting viewing history:', error);
    document.getElementById('history-list').innerHTML =
      '<p class="placeholder-text">Error getting viewing history. Please try again later.</p>';
  } finally {
    hideSpinner('history-list');
  }
}

/**
 * Search for movies based on a query.
 */
async function searchMovies() {
  const query = document.getElementById('search-query').value.trim();
  if (!query) {
    alert('Please enter a search query');
    return;
  }

  showSpinner('search-results');
  try {
    const movies = await fetchAPI(
      `/movies/search?query=${encodeURIComponent(query)}`
    );
    displayMovies(movies, 'search-results');
  } catch (error) {
    console.error('Error searching movies:', error);
    document.getElementById('search-results').innerHTML =
      '<p class="placeholder-text">Error searching movies. Please try again later.</p>';
  } finally {
    hideSpinner('search-results');
  }
}

/**
 * Display a list of movies in a specified container.
 * @param {Array<Object>} movies - The list of movies to display.
 * @param {string} containerId - The ID of the container element.
 * @param {boolean} [isHistory=false] - Whether the movies are from viewing history.
 */
function displayMovies(movies, containerId, isHistory = false) {
  const container = document.getElementById(containerId);
  if (!Array.isArray(movies) || movies.length === 0) {
    container.innerHTML = '<p class="placeholder-text">No movies found.</p>';
    return;
  }

  container.innerHTML = movies
    .map(
      (movie) => `
        <div class="movie-item">
            <h3>${movie.title}</h3>
            ${movie.start_year ? `<p>Year: ${movie.start_year}</p>` : ''}
            ${
              movie.avg_rating
                ? `<p class="rating">Rating: ${movie.avg_rating.toFixed(1)}</p>`
                : ''
            }
            ${movie.genres ? `<p>Genres: ${movie.genres}</p>` : ''}
            ${
              isHistory
                ? `
                <p>Watched on: ${new Date(
                  movie.watch_date
                ).toLocaleDateString()}</p>
                <p>Duration: ${movie.watch_duration} minutes</p>
            `
                : ''
            }
        </div>
    `
    )
    .join('');
}

/**
 * Event listener for DOM content loaded.
 */
document.addEventListener('DOMContentLoaded', () => {
  loadUsers();
  document.getElementById('user-select').addEventListener('change', () => {
    const userId = document.getElementById('user-select').value;
    if (userId) {
      getRecommendations();
      getViewingHistory();
    } else {
      document.getElementById('recommendation-list').innerHTML =
        '<p class="placeholder-text">Select a user to see recommendations</p>';
      document.getElementById('history-list').innerHTML =
        '<p class="placeholder-text">Select a user to see viewing history</p>';
    }
  });
  document
    .getElementById('search-button')
    .addEventListener('click', searchMovies);
  document.getElementById('search-query').addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
      searchMovies();
    }
  });
});
