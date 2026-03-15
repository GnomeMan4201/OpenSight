def chunk_text(text, size=800, overlap=120):
    chunks = []
    start = 0
    idx = 0

    while start < len(text):
        end = start + size
        chunk = text[start:end]
        chunks.append({
            "index": idx,
            "text": chunk
        })
        start = end - overlap
        idx += 1

    return chunks
