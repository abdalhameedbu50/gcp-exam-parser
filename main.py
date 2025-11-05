import os
import re
import json
from flask import Flask, request
from google.cloud import storage

app = Flask(__name__)
storage_client = storage.Client()
BUCKET = os.environ.get("BUCKET")

def clean_text(text: str) -> str:
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def parse_blocks(raw: str):
    content = re.sub(r'#{2,}.*?topic.*?question.*?discussion', '##QSPLIT', raw, flags=re.IGNORECASE | re.DOTALL)
    sections = [s for s in content.split('##QSPLIT') if s.strip()]
    out = []
    q = 1

    for sec in sections:
        qm = re.search(r'\[.*?Questions.*?\]\s*(.*?)(?=\n[A-Z]\.|Suggested Answer:|\*\*Answer:)', sec, flags=re.IGNORECASE | re.DOTALL)
        if not qm:
            qm = re.search(r'Question.*?\n(.*?)(?=\n[A-Z]\.|Suggested Answer:|\*\*Answer:)', sec, flags=re.IGNORECASE | re.DOTALL)
        if not qm:
            continue
        question = clean_text(qm.group(1))
        if len(question) < 15:
            continue

        option_pat = r'([A-Z])\.\s*(.*?)(?=\n[A-Z]\.\s|\nSuggested Answer:|\n\*\*Answer:|$)'
        option_pairs = re.findall(option_pat, sec, re.DOTALL)
        options = {a: clean_text(b) for a, b in option_pairs if clean_text(b)}
        if len(options) < 2:
            continue

        am = re.search(r'Suggested Answer:\s*([A-Z ,]+)', sec)
        if not am:
            am = re.search(r'\*\*Answer:\s*([A-Z ,]+)\*\*', sec)
        correct = "N/A"
        if am:
            parts = [p.strip() for p in re.split(r'[,\s]+', am.group(1)) if p.strip()]
            correct = ",".join(parts)

        out.append({"id": q, "question": question, "options": options, "correct": correct})
        q += 1
    return out

def extract_event_info(request_json):
    """Extract bucket and object name from Eventarc Cloud Audit Log event"""
    print(f"Full event payload: {json.dumps(request_json, indent=2)}")
    
    # Eventarc Cloud Storage Audit Log format
    if "protoPayload" in request_json:
        try:
            # Get bucket from resource labels
            bucket = request_json.get("resource", {}).get("labels", {}).get("bucket_name")
            
            # Get object name from protoPayload
            resource_name = request_json.get("protoPayload", {}).get("resourceName", "")
            # Format: projects/_/buckets/BUCKET_NAME/objects/OBJECT_NAME
            if "/objects/" in resource_name:
                name = resource_name.split("/objects/", 1)[1]
            else:
                name = None
            
            print(f"Extracted from Audit Log: bucket={bucket}, name={name}")
            return bucket, name
        except Exception as e:
            print(f"Error parsing Audit Log format: {e}")
    
    # Pub/Sub wrapped format (fallback)
    msg = request_json.get("message", {})
    attrs = msg.get("attributes", {})
    bucket = attrs.get("bucketId")
    name = attrs.get("objectId")
    
    if bucket or name:
        print(f"Extracted from Pub/Sub format: bucket={bucket}, name={name}")
    
    return bucket, name

@app.route("/", methods=["POST"])
def handle():
    req_json = request.get_json(silent=True) or {}
    print("=" * 80)
    print("Received event:", json.dumps(req_json, indent=2))
    print("=" * 80)

    bucket, name = extract_event_info(req_json)
    print(f"Parsed bucket={bucket}, name={name}")

    # Validate environment
    if not BUCKET:
        print("ERROR: BUCKET environment variable not set")
        return ("BUCKET environment variable not set", 500)

    if not bucket or not name:
        print("ERROR: No bucket/object in event")
        return ("No bucket/object in event", 204)

    # Use the configured BUCKET env var instead of event bucket for security
    target_bucket = BUCKET or bucket
    print(f"Using bucket: {target_bucket}")

    if not name.startswith("input/"):
        print(f"Ignored file (not in input/): {name}")
        return ("Ignored (not input folder)", 204)

    blob = storage_client.bucket(target_bucket).blob(name)

    try:
        print(f"Downloading blob: {name}")
        raw = blob.download_as_text()
    except Exception as e:
        print(f"Error downloading as text, trying bytes: {e}")
        try:
            raw = blob.download_as_bytes().decode("utf-8", errors="ignore")
        except Exception as e2:
            print(f"Error downloading blob: {e2}")
            return (f"Error downloading: {e2}", 500)

    print(f"Downloaded {len(raw)} characters")
    parsed = parse_blocks(raw)
    print(f"Parsed {len(parsed)} questions")
    
    base = name.replace("input/", "clean/")

    if not parsed:
        error_blob = storage_client.bucket(target_bucket).blob(base + ".err.txt")
        error_blob.upload_from_string("No questions parsed")
        print(f"No questions found, saved error file: {base}.err.txt")
        return ("Done - no questions", 200)

    # Save JSON
    json_key = base.rsplit(".", 1)[0] + ".json"
    json_blob = storage_client.bucket(target_bucket).blob(json_key)
    json_blob.upload_from_string(
        json.dumps(parsed, indent=2), content_type="application/json"
    )
    print(f"Saved JSON: {json_key}")

    # Save txt
    lines = []
    for it in parsed:
        lines.append(f"Question {it['id']}:\n{it['question']}\n\nOptions:\n")
        for k in sorted(it["options"].keys()):
            lines.append(f"{k}. {it['options'][k]}\n")
        lines.append("=" * 61)
        lines.append("=" * 61)
        lines.append(f"Suggested Answer: {it['correct']}\n\n")

    txt_key = base.rsplit(".", 1)[0] + "_clean.txt"
    txt_blob = storage_client.bucket(target_bucket).blob(txt_key)
    txt_blob.upload_from_string("\n".join(lines))
    print(f"Saved TXT: {txt_key}")

    print(f"✅ Done processing {name}")
    return ("OK", 200)

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return {"status": "healthy", "bucket": BUCKET}, 200

if __name__ == "__main__":
    # For local testing
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
