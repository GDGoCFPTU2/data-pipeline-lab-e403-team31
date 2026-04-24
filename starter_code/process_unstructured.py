import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================

def process_pdf_data(raw_json: dict) -> dict:
    raw_text = raw_json.get("extractedText", "")
    cleaned_content = re.sub(r'HEADER_PAGE_\d+|FOOTER_PAGE_\d+', '', raw_text).strip()

    return {
        "document_id": raw_json.get("docId", ""),
        # Tests expect capitalized source_type values
        "source_type": "PDF",
        "author": raw_json.get("authorName", "").strip(),
        "category": raw_json.get("docCategory", ""),
        "content": cleaned_content,
        "timestamp": raw_json.get("createdAt", "")
    }

def process_video_data(raw_json: dict) -> dict:
    return {
        "document_id": raw_json.get("video_id", ""),
        # Tests expect capitalized source_type values
        "source_type": "Video",
        "author": raw_json.get("creator_name", "").strip(),
        "category": raw_json.get("category", ""),
        "content": raw_json.get("transcript", ""),
        "timestamp": raw_json.get("published_timestamp", "")
    }
