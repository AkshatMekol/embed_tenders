import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")

DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMBEDDING_MODEL = "text-embedding-3-large"  

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
TENDERS_COLLECTION = os.getenv("TENDERS_COLLECTION")
VECTOR_COLLECTION = os.getenv("VECTOR_COLLECTION")
DOCS_STATUS_COLLECTION = os.getenv("DOCS_STATUS_COLLECTION")

BATCH_SIZE = 2048
MAX_PROCESSES_GROQ = 5
MAX_PROCESSES_DEEPSEEK = 10

GROQ_OCR_PROMPT = """
                  Extract all text from this scanned page exactly as it appears on the page.
                  - Do NOT summarize, interpret, or add any commentary.
                  - Output only the text exactly as on the page, no less no more.
                  - If no text is found, return an empty string "".
                  """

DEEPSEEK_TRANSLATE_PROMPT = """
                            You are a translator. Translate the following text to English exactly.
                            - If the text is already in English, leave it unchanged.
                            - Do not summarize, comment, or alter the content in any way.
                            - Preserve all formatting, spacing, and newlines.
                            - If the text is blank, return blank.
                            """

CLASSIFY_PROMPT = """
                  You are a strict classifier for tender documents.
                  
                  Your task is to identify ONLY the pages that must be filled out by the contractor and sent back to the client.
                  These pages contain blanks, empty fields, places to write, tables to fill, or areas for signatures/seals.
                  
                  Ignore any page that is purely:
                  - Instructions, clauses, or general text
                  - Tender descriptions
                  - Annexures with information already filled
                  - Tables that only display data without requiring input
                  
                  Respond with ONE WORD ONLY: FORM or OTHER.
                  
                  Page content:
                  {content}
                  """
