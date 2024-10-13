"""Main module for the Flask application providing API endpoints and serving the web UI.

This module creates and configures the Flask application, registers blueprints
for movies, users, and recommendations, and sets up routes to serve the static
files for the web UI. It also configures CORS for all routes.
"""

from flask import Flask, send_from_directory
from flask_cors import CORS

from src.api.movies import movies_bp
from src.api.recommendations import recommendations_bp
from src.api.users import users_bp


def create_app() -> Flask:
    """Creates and configures the Flask application.

    This function initializes the Flask app, enables CORS, registers blueprints
    for various API endpoints, and sets up routes to serve the static files for
    the web UI.

    Returns:
        The configured Flask application.
    """
    app = Flask(__name__, static_folder='../ui')

    # Enable CORS for all routes
    CORS(app, resources={r"/*": {"origins": ["http://localhost:5000", "http://127.0.0.1:5000"]}})

    # Register blueprints for API endpoints
    app.register_blueprint(movies_bp)
    app.register_blueprint(users_bp)
    app.register_blueprint(recommendations_bp)

    @app.route('/')
    def index():
        """Serves the main HTML file for the web UI."""
        return send_from_directory(app.static_folder, 'index.html')

    @app.route('/<path:path>')
    def serve_static(path: str):
        """Serves static files for the web UI."""
        return send_from_directory(app.static_folder, path)

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
