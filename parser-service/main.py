import os
import re
import json
import traceback
import time
from flask import Flask, request, jsonify
from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions
from google.auth import default
from google.auth.transport.requests import Request

app = Flask(__name__)

# Initialize storage client globally with error handling
BUCKET = os.environ.get("BUCKET")

def get_storage_client():
    """Initialize storage client with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Get credentials explicitly with timeout
            credentials, project = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
            
            # Refresh credentials to ensure they're valid
            if not credentials.valid:
                print(f"Refreshing credentials (attempt {attempt + 1}/{max_retries})")
                auth_req = Request()
                credentials.refresh(auth_req)
            
            client = storage.Client(credentials=credentials, project=project)
            print(f"✅ Storage client initialized successfully (project: {project})")
            return client
        except Exception as e:
            print(f"⚠️ Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"❌ Failed to initialize storage client after {max_retries} attempts")
                raise
    return None

# Initialize client
storage_client = None
try:
    storage_client = get_storage_client()
    print(f"📦 Target bucket: {BUCKET}")
except Exception as e:
    print(f"❌ Failed to initialize storage client: {e}")
    print(traceback.format_exc())

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
            
            print(f"✅ Extracted from Audit Log: bucket={bucket}, name={name}")
            return bucket, name
        except Exception as e:
            print(f"⚠️ Error parsing Audit Log format: {e}")
    
    # Pub/Sub wrapped format (fallback)
    msg = request_json.get("message", {})
    attrs = msg.get("attributes", {})
    bucket = attrs.get("bucketId")
    name = attrs.get("objectId")
    
    if bucket or name:
        print(f"✅ Extracted from Pub/Sub format: bucket={bucket}, name={name}")
    
    return bucket, name

@app.route("/", methods=["POST"])
def handle():
    try:
        req_json = request.get_json(silent=True) or {}
        print("=" * 80)
        print("📥 Received event")
        print("=" * 80)
        
        # Validate storage client
        if storage_client is None:
            print("❌ Storage client not initialized")
            return jsonify({"error": "Storage client not initialized"}), 500
        
        # Validate environment
        if not BUCKET:
            print("❌ BUCKET environment variable not set")
            return jsonify({"error": "BUCKET environment variable not set"}), 500

        bucket, name = extract_event_info(req_json)
        print(f"📄 File: {name}")
        print(f"🪣 Bucket: {bucket}")

        if not bucket or not name:
            print("⚠️ No bucket/object in event")
            return jsonify({"status": "ignored", "reason": "No bucket/object in event"}), 200

        # Use the configured BUCKET env var for security
        target_bucket = BUCKET
        print(f"🎯 Target bucket: {target_bucket}")

        if not name.startswith("input/"):
            print(f"⏭️ Skipped (not in input/): {name}")
            return jsonify({"status": "ignored", "reason": "Not in input folder"}), 200

        # Download file
        print(f"⬇️ Downloading: {name}")
        bucket_obj = storage_client.bucket(target_bucket)
        blob = bucket_obj.blob(name)
        
        # Check if blob exists
        if not blob.exists():
            print(f"❌ Blob does not exist: {name}")
            return jsonify({"error": "Blob not found"}), 404

        try:
            raw = blob.download_as_text(encoding='utf-8')
            print(f"✅ Downloaded as text: {len(raw)} characters")
        except Exception as e:
            print(f"⚠️ Failed to download as text: {e}")
            raw = blob.download_as_bytes().decode("utf-8", errors="ignore")
            print(f"✅ Downloaded as bytes: {len(raw)} characters")

        # Parse content
        print("🔍 Parsing content...")
        parsed = parse_blocks(raw)
        print(f"✅ Parsed {len(parsed)} questions")
        
        base = name.replace("input/", "clean/")

        if not parsed:
            error_blob = bucket_obj.blob(base + ".err.txt")
            error_blob.upload_from_string("No questions parsed")
            print(f"⚠️ No questions found, saved error file: {base}.err.txt")
            return jsonify({"status": "completed", "questions": 0, "error": "No questions parsed"}), 200

        # Save JSON
        json_key = base.rsplit(".", 1)[0] + ".json"
        json_blob = bucket_obj.blob(json_key)
        json_content = json.dumps(parsed, indent=2)
        json_blob.upload_from_string(json_content, content_type="application/json")
        print(f"💾 Saved JSON: {json_key}")

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
        txt_blob = bucket_obj.blob(txt_key)
        txt_blob.upload_from_string("\n".join(lines))
        print(f"💾 Saved TXT: {txt_key}")

        print(f"✅ Successfully processed {name}")
        return jsonify({
            "status": "success",
            "input_file": name,
            "output_json": json_key,
            "output_txt": txt_key,
            "questions_parsed": len(parsed)
        }), 200

    except gcp_exceptions.Forbidden as e:
        print(f"❌ Permission denied: {e}")
        print(traceback.format_exc())
        return jsonify({"error": "Permission denied", "details": str(e)}), 403
    
    except gcp_exceptions.NotFound as e:
        print(f"❌ Resource not found: {e}")
        print(traceback.format_exc())
        return jsonify({"error": "Resource not found", "details": str(e)}), 404
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        print(traceback.format_exc())
        return jsonify({"error": "Internal server error", "details": str(e)}), 500

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    status = {
        "status": "healthy",
        "bucket": BUCKET,
        "storage_client": "initialized" if storage_client else "not initialized"
    }
    
    # Test storage access
    if storage_client and BUCKET:
        try:
            bucket_obj = storage_client.bucket(BUCKET)
            bucket_obj.exists()
            status["storage_access"] = "ok"
        except Exception as e:
            status["storage_access"] = f"error: {str(e)}"
            status["status"] = "unhealthy"
    
    return jsonify(status), 200 if status["status"] == "healthy" else 503

@app.route("/test-permissions", methods=["GET"])
def test_permissions():
    """Test endpoint to verify permissions"""
    if not storage_client:
        return jsonify({"error": "Storage client not initialized"}), 500
    
    if not BUCKET:
        return jsonify({"error": "BUCKET env var not set"}), 500
    
    try:
        bucket_obj = storage_client.bucket(BUCKET)
        
        # Test bucket access
        exists = bucket_obj.exists()
        
        # Try to list blobs
        blobs = list(bucket_obj.list_blobs(max_results=5))
        
        return jsonify({
            "bucket": BUCKET,
            "exists": exists,
            "can_list": True,
            "sample_files": [blob.name for blob in blobs]
        }), 200
        
    except gcp_exceptions.Forbidden as e:
        return jsonify({
            "error": "Permission denied",
            "bucket": BUCKET,
            "details": str(e)
        }), 403
    except Exception as e:
        return jsonify({
            "error": str(e),
            "bucket": BUCKET
        }), 500

if __name__ == "__main__":
    # For local testing
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
