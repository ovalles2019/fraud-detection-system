"""Run the API: `python run.py` from the project root (after installing requirements)."""

from app import create_app

app = create_app()

if __name__ == "__main__":
    # Flask dev server — use gunicorn/waitress in production.
    app.run(host="127.0.0.1", port=5000, debug=True)
