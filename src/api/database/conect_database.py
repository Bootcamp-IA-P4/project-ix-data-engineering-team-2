import os
from supabase import create_client, Client
from dotenv import load_dotenv

class Supabase_conections:
    def __init__(self):
        load_dotenv()
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        self.client = None
        self.connect()

    def connect(self):
        try:
            self.client = create_client(self.url, self.key)
            print("Connected to Supabase successfully.")
        except Exception as e:
            print(f"Failed to connect to Supabase: {e}")
            self.client = None

conect = Supabase_conections()            