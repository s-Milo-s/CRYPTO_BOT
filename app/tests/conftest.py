from dotenv import load_dotenv
import pathlib

# Automatically load .env from project root
load_dotenv(dotenv_path=pathlib.Path(__file__).parent.parent / ".env")