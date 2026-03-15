from apps.api.database import engine
from apps.api.models import Base

print("Creating semantic_chunks table...")
Base.metadata.create_all(bind=engine)
print("Done.")
