"""
Main module for the Flask application providing API endpoints.
Registers blueprints for movies, users, and recommendations.
"""

from flask import Flask

from src.api.movies import movies_bp
from src.api.recommendations import recommendations_bp
from src.api.users import users_bp


def create_app() -> Flask:
    """Creates and configures the Flask application.

    Returns:
        The configured Flask application.
    """
    app = Flask(__name__)
    app.register_blueprint(movies_bp)
    app.register_blueprint(users_bp)
    app.register_blueprint(recommendations_bp)
    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
