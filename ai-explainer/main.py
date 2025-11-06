import os
import json
import traceback
from flask import Flask, request, jsonify
from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions

app = Flask(__name__)
BUCKET = os.environ.get("BUCKET")

# Initialize Cloud Storage client
try:
    storage_client = storage.Client()
    print(f"✅ Storage client initialized. Target bucket: {BUCKET}")
except Exception as e:
    storage_client = None
    print(f"❌ Failed to initialize storage client: {e}")
    print(traceback.format_exc())


def extract_event_info(request_json):
    """Extract bucket and file name from Eventarc or Pub/Sub format."""

    # Case 1: Eventarc (Cloud Audit Logs)
    if "protoPayload" in request_json:
        try:
            bucket = request_json.get("resource", {}).get("labels", {}).get("bucket_name")
            resource_name = request_json.get("protoPayload", {}).get("resourceName", "")
            name = None
            if "/objects/" in resource_name:
                name = resource_name.split("/objects/", 1)[1]
            print(f"✅ Extracted (Audit Log): bucket={bucket}, name={name}")
            return bucket, name
        except Exception as e:
            print(f"⚠️ Failed parsing Audit Log event: {e}")

    # Case 2: Pub/Sub message (older format)
    msg = request_json.get("message", {})
    attrs = msg.get("attributes", {})
    bucket = attrs.get("bucketId")
    name = attrs.get("objectId")

    if bucket or name:
        print(f"✅ Extracted (Pub/Sub): bucket={bucket}, name={name}")

    return bucket, name


@app.route("/", methods=["POST"])
def handle():
    try:
        req_json = request.get_json(silent=True) or {}
        print("=" * 80)
        print("📥 Received Event:")
        print(json.dumps(req_json)[:1000])
        print("=" * 80)

        # Check for valid storage client
        if storage_client is None:
            print("❌ Storage client not initialized")
            return jsonify({"error": "Storage client not initialized"}), 500

        bucket, name = extract_event_info(req_json)
        if not bucket or not name:
            print("⚠️ No bucket/object info in event")
            return jsonify({"status": "ignored", "reason": "No bucket/object in event"}), 200

        print(f"🪣 Bucket: {bucket}")
        print(f"📄 Object: {name}")

        # Only respond to 'clean/' folder files
        if not name.startswith("clean/"):
            print(f"⏭️ Skipping non-clean file: {name}")
            return jsonify({"status": "ignored", "reason": "Not a clean file"}), 200

        # Download file content
        blob = storage_client.bucket(bucket).blob(name)
        raw = blob.download_as_text(encoding="utf-8")
        print(f"✅ File downloaded ({len(raw)} chars)")

        # --- AI Explainer logic placeholder ---
        summary = f"AI Explanation for {name}:\n\n{raw[:400]}"
        output_name = name.replace("clean/", "explained/") + ".txt"

        # Upload explanation file
        output_blob = storage_client.bucket(bucket).blob(output_name)
        output_blob.upload_from_string(summary)
        print(f"💾 Saved AI explanation to {output_name}")

        return jsonify({
            "status": "success",
            "input_file": name,
            "output_file": output_name
        }), 200

    except gcp_exceptions.GoogleAPICallError as e:
        print(f"❌ GCP API error: {e}")
        return jsonify({"error": "GCP API error", "details": str(e)}), 500
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        print(traceback.format_exc())
        return jsonify({"error": "Internal server error", "details": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "healthy" if storage_client else "unhealthy",
        "bucket": BUCKET
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
