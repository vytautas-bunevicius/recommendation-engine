from flask import Flask
from api.movies import movies_bp
from api.users import users_bp
from api.recommendations import recommendations_bp

app = Flask(__name__)

app.register_blueprint(movies_bp)
app.register_blueprint(users_bp)
app.register_blueprint(recommendations_bp)

if __name__ == "__main__":
    app.run(debug=True)
