Steps:
python -m venv .env
source .env/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8080
python api/main.py
