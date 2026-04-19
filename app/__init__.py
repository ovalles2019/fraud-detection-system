from flask import Flask

from app.api import bp as api_bp


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.from_mapping(
        JSON_SORT_KEYS=False,
    )
    app.register_blueprint(api_bp)
    return app
