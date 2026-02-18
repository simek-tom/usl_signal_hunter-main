from flask import Flask

app = Flask(__name__)

app.secret_key = "dev-secret-change-me"   # # set from env later

from app import routes