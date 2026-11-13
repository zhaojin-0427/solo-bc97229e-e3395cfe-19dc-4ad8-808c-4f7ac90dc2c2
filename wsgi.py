"""gunicorn 入口（单 worker，多线程）：

    gunicorn --workers=1 --threads=4 --bind=0.0.0.0:8000 wsgi:app
"""
from app.server import create_app

app = create_app()
