import os
import json
import traceback
import re
from flask import Flask, request, jsonify
from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions
import time

app = Flask(__name__)
BUCKET = os.environ.get("BUCKET")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5"))  # Process 5 questions at a time
MAX_QUESTIONS = int(os.environ.get("MAX_QUESTIONS", "100"))  # Limit per file

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

def parse_questions_streaming(blob, max_questions=None):
    """
    Stream and parse questions from a blob without loading entire file into memory.
    Yields questions one at a time.
    """
    delimiter = "=" * 61
    buffer = ""
    question_count = 0
    
    # Stream the blob in chunks
    print(f"📖 Starting to stream file...")
    chunk_size = 1024 * 1024  # 1MB chunks
    
    try:
        # Download in chunks
        for chunk in blob.download_as_bytes(start=0):
            buffer += chunk.decode('utf-8', errors='ignore')
            
            # Process complete sections in buffer
            while delimiter in buffer:
                section, buffer = buffer.split(delimiter, 1)
                section = section.strip()
                
                if section and len(section) > 50:  # Ignore very short sections
                    # Extract suggested answer
                    suggested_answer = None
                    answer_match = re.search(r'Suggested Answer:\s*([A-Z])', section, re.IGNORECASE)
                    if answer_match:
                        suggested_answer = answer_match.group(1)
                    
                    question_count += 1
                    yield {
                        "number": question_count,
                        "question": section,
                        "suggested_answer": suggested_answer
                    }
                    
                    # Stop if we've reached the max
                    if max_questions and question_count >= max_questions:
                        print(f"⚠️ Reached max questions limit ({max_questions})")
                        return
        
        # Process any remaining content in buffer
        if buffer.strip() and len(buffer.strip()) > 50:
            section = buffer.strip()
            suggested_answer = None
            answer_match = re.search(r'Suggested Answer:\s*([A-Z])', section, re.IGNORECASE)
            if answer_match:
                suggested_answer = answer_match.group(1)
            
            question_count += 1
            yield {
                "number": question_count,
                "question": section,
                "suggested_answer": suggested_answer
            }
    
    except Exception as e:
        print(f"❌ Error during streaming: {e}")
        raise
    
    print(f"✅ Finished streaming. Total questions found: {question_count}")

def generate_explanation_batch(questions_batch):
    """
    Generate explanations for a batch of questions.
    TODO: Replace with actual AI API call (Vertex AI, OpenAI, Claude)
    """
    explanations = []
    
    for q_data in questions_batch:
        question_num = q_data["number"]
        question = q_data["question"]
        suggested_answer = q_data["suggested_answer"]
        
        # Truncate question for display (first 300 chars)
        question_preview = question[:300] + "..." if len(question) > 300 else question
        
        # TODO: Replace with actual AI API call
        explanation = f"""{'=' * 80}
Question {question_num}
{'=' * 80}

{question_preview}

Suggested Answer: {suggested_answer if suggested_answer else "Not provided"}

EXPLANATION:
[This would be replaced with actual AI-generated explanation]
The correct answer is {suggested_answer} because...

Key concepts to understand:
- Concept 1: ...
- Concept 2: ...
- Concept 3: ...

Why other options are incorrect:
- Other options...

{'=' * 80}

"""
        explanations.append(explanation)
    
    return explanations

def write_output_incrementally(bucket, output_name, content, mode='append'):
    """
    Write to output blob incrementally to avoid memory issues.
    """
    output_blob = storage_client.bucket(bucket).blob(output_name)
    
    if mode == 'create':
        # Create new file with initial content
        output_blob.upload_from_string(content, content_type="text/plain")
    else:
        # Append to existing file
        try:
            existing = output_blob.download_as_text()
            new_content = existing + content
            output_blob.upload_from_string(new_content, content_type="text/plain")
        except Exception as e:
            # If file doesn't exist, create it
            output_blob.upload_from_string(content, content_type="text/plain")

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
        
        # Get blob reference (don't download yet)
        blob = storage_client.bucket(bucket).blob(name)
        
        # Check if blob exists
        if not blob.exists():
            print(f"❌ Blob does not exist: {name}")
            return jsonify({"error": "File not found"}), 404
        
        print(f"✅ Blob found, size: {blob.size} bytes")
        
        # Create output filename
        output_name = name.replace("clean/", "explained/")
        if not output_name.endswith(".txt"):
            output_name += ".txt"
        
        # Initialize output file with header
        header = f"""AI-Generated Question Explanations
Generated: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}
Source: {name}

{'=' * 80}

"""
        write_output_incrementally(bucket, output_name, header, mode='create')
        print(f"📝 Created output file: {output_name}")
        
        # Process questions in batches
        total_processed = 0
        batch = []
        
        for question_data in parse_questions_streaming(blob, max_questions=MAX_QUESTIONS):
            batch.append(question_data)
            
            # Process batch when it reaches BATCH_SIZE
            if len(batch) >= BATCH_SIZE:
                print(f"🤖 Processing batch of {len(batch)} questions (total: {total_processed + len(batch)})")
                
                # Generate explanations for batch
                explanations = generate_explanation_batch(batch)
                
                # Write batch to output file
                batch_content = "\n".join(explanations)
                write_output_incrementally(bucket, output_name, batch_content, mode='append')
                
                total_processed += len(batch)
                batch = []
                
                print(f"✅ Batch written to output ({total_processed} questions processed)")
        
        # Process remaining questions in final batch
        if batch:
            print(f"🤖 Processing final batch of {len(batch)} questions")
            explanations = generate_explanation_batch(batch)
            batch_content = "\n".join(explanations)
            write_output_incrementally(bucket, output_name, batch_content, mode='append')
            total_processed += len(batch)
        
        print(f"💾 Completed! Total questions processed: {total_processed}")
        print(f"📤 Output saved to: {output_name}")
        
        return jsonify({
            "status": "success",
            "input_file": name,
            "output_file": output_name,
            "questions_processed": total_processed,
            "batch_size": BATCH_SIZE
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
        "bucket": BUCKET,
        "batch_size": BATCH_SIZE,
        "max_questions": MAX_QUESTIONS
    }), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
