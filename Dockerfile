FROM python:3.11-slim

WORKDIR /app

# No system packages needed for the default install:
#   - pdfplumber uses pdfminer.six (pure Python) — no libpoppler
#   - PyMuPDF ships prebuilt manylinux wheels — no system libs required
#   - pytesseract needs tesseract-ocr system binary if OCR is enabled
#     (uncomment below if USE_OCR=true)
#
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     tesseract-ocr \
#     && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p uploads uploads/.tmp

EXPOSE 8000

# Healthcheck: Python stdlib urllib — no curl needed in the image.
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request, sys; \
        r = urllib.request.urlopen('http://localhost:8000/health', timeout=4); \
        sys.exit(0 if r.status == 200 else 1)"

CMD ["uvicorn", "apps.api.main:app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "1", "--timeout-keep-alive", "30"]
