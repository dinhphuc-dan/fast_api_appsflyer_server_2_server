import uvicorn
from pyngrok import ngrok
from dotenv import load_dotenv
from pathlib import Path
import argparse
import os

# basic setup for argument throug command line for local testing and development
_parser = argparse.ArgumentParser()
_parser.add_argument("--production")
_args = _parser.parse_args()


if __name__ == "__main__":
    if _args.production == "true":
        pass 
    else:
        load_dotenv(dotenv_path=Path(".env.dev"), override=True, verbose=True)
        ngrok.set_auth_token(os.getenv("NGROK_AUTHTOKEN"))
        url = ngrok.connect(8080).public_url
        print(url, flush=True)
        uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)