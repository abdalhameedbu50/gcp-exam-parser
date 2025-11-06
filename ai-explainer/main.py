import os
import json
import traceback
import re
from flask import Flask, request, jsonify
from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions
from google.cloud.exceptions import NotFound
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig
import time

app = Flask(__name__)
BUCKET = os.environ.get("BUCKET")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5"))  # Process 5 questions at a time
MAX_QUESTIONS = int(os.environ.get("MAX_QUESTIONS", "2000"))  # Limit per file
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
LOCATION = os.environ.get("VERTEX_AI_LOCATION", "us-central1")

# Initialize Cloud Storage client
try:
    storage_client = storage.Client()
    print(f"✅ Storage client initialized. Target bucket: {BUCKET}")
except Exception as e:
    storage_client = None
    print(f"❌ Failed to initialize storage client: {e}")
    print(traceback.format_exc())

# Initialize Vertex AI
try:
    if PROJECT_ID:
        vertexai.init(project=PROJECT_ID, location=LOCATION)
        print(f"✅ Vertex AI initialized. Project: {PROJECT_ID}, Location: {LOCATION}")
    else:
        print("⚠️ PROJECT_ID not set, Vertex AI not initialized")
except Exception as e:
    print(f"❌ Failed to initialize Vertex AI: {e}")
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

def detect_file_format(filename):
    """Detect if file is JSON or text format."""
    return filename.lower().endswith('.json')

def parse_json_questions(content):
    """
    Parse JSON format questions.
    Expects structure like: {"questions": [...]} or just [...]
    """
    try:
        data = json.loads(content)
        
        # Handle different JSON structures
        if isinstance(data, dict):
            # Look for common keys
            if "questions" in data:
                questions_list = data["questions"]
            elif "items" in data:
                questions_list = data["items"]
            elif "data" in data:
                questions_list = data["data"]
            else:
                # Try to find first list in the dict
                questions_list = None
                for value in data.values():
                    if isinstance(value, list):
                        questions_list = value
                        break
                if not questions_list:
                    print("⚠️ Could not find questions list in JSON")
                    return []
        elif isinstance(data, list):
            questions_list = data
        else:
            print("⚠️ Unexpected JSON structure")
            return []
        
        print(f"📋 Found {len(questions_list)} questions in JSON")
        
        # Parse each question
        parsed_questions = []
        for i, q in enumerate(questions_list, 1):
            if isinstance(q, dict):
                # Extract question text (try common field names)
                question_text = (
                    q.get("question") or 
                    q.get("text") or 
                    q.get("description") or
                    q.get("prompt") or
                    str(q)
                )
                
                # Extract suggested answer
                suggested_answer = (
                    q.get("suggested_answer") or
                    q.get("correct_answer") or
                    q.get("answer") or
                    q.get("correctAnswer") or
                    None
                )
                
                # Extract options if available
                options = (
                    q.get("options") or
                    q.get("choices") or
                    q.get("answers") or
                    []
                )
                
                parsed_questions.append({
                    "number": i,
                    "question": question_text,
                    "suggested_answer": suggested_answer,
                    "options": options,
                    "raw_data": q
                })
            else:
                # If question is just a string
                parsed_questions.append({
                    "number": i,
                    "question": str(q),
                    "suggested_answer": None,
                    "options": [],
                    "raw_data": q
                })
        
        return parsed_questions
    
    except json.JSONDecodeError as e:
        print(f"❌ JSON parse error: {e}")
        return []
    except Exception as e:
        print(f"❌ Error parsing JSON questions: {e}")
        print(traceback.format_exc())
        return []

def parse_text_questions_streaming(blob, max_questions=None):
    """
    Stream and parse questions from text file with delimiter.
    Yields questions one at a time.
    """
    delimiter = "=" * 61
    buffer = ""
    question_count = 0
    
    print(f"📖 Starting to stream text file...")
    chunk_size = 1024 * 1024  # 1MB chunks
    
    try:
        # Get blob size
        blob.reload()  # Refresh metadata
        total_size = blob.size
        print(f"📏 File size: {total_size} bytes")
        
        # Stream download
        start = 0
        while start < total_size:
            end = min(start + chunk_size, total_size)
            chunk_bytes = blob.download_as_bytes(start=start, end=end)
            buffer += chunk_bytes.decode('utf-8', errors='ignore')
            start = end
            
            # Process complete sections in buffer
            while delimiter in buffer:
                section, buffer = buffer.split(delimiter, 1)
                section = section.strip()
                
                if section and len(section) > 50:  # Ignore very short sections
                    # Parse the section to extract components
                    parsed = parse_question_section(section)
                    
                    question_count += 1
                    parsed["number"] = question_count
                    
                    yield parsed
                    
                    # Stop if we've reached the max
                    if max_questions and question_count >= max_questions:
                        print(f"⚠️ Reached max questions limit ({max_questions})")
                        return
        
        # Process any remaining content in buffer
        if buffer.strip() and len(buffer.strip()) > 50:
            section = buffer.strip()
            parsed = parse_question_section(section)
            question_count += 1
            parsed["number"] = question_count
            yield parsed
    
    except Exception as e:
        print(f"❌ Error during streaming: {e}")
        print(traceback.format_exc())
        raise
    
    print(f"✅ Finished streaming. Total questions found: {question_count}")

def parse_question_section(section):
    """
    Parse a single question section to extract:
    - Question text
    - Options (A, B, C, D)
    - Suggested Answer
    """
    original_section = section
    
    # Extract suggested answer (look for pattern like "Suggested Answer: C")
    suggested_answer = None
    answer_match = re.search(r'Suggested Answer:\s*([A-Z])', section, re.IGNORECASE)
    if answer_match:
        suggested_answer = answer_match.group(1)
        # Remove the suggested answer line from section for cleaner parsing
        section = section[:answer_match.start()].strip()
    
    # Extract question number and text
    question_text = section
    question_number_match = re.match(r'Question\s+(\d+):\s*', section)
    if question_number_match:
        # Remove "Question X:" prefix
        question_text = section[question_number_match.end():]
    
    # Extract options (A. B. C. D.)
    options = {}
    
    # Split by lines and look for option patterns
    lines = question_text.split('\n')
    current_option = None
    current_text = []
    question_lines = []
    in_options = False
    
    for line in lines:
        # Check if line starts with an option letter (A. B. C. D.)
        option_match = re.match(r'^([A-D])[.)\s]+(.*)

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
        options = q_data.get("options", {})
        
        # Format question text (don't truncate)
        question_text = question if isinstance(question, str) else str(question)
        
        # Format options
        options_text = ""
        if options:
            options_text = "\n\nOptions:"
            for key in sorted(options.keys()):
                value = options[key]
                # Truncate very long options
                if len(value) > 200:
                    value = value[:200] + "..."
                options_text += f"\n{key}. {value}"
        
        # Build the explanation
        explanation = f"""{'=' * 80}
Question {question_num}
{'=' * 80}

{question_text}{options_text}

Suggested Answer: {suggested_answer if suggested_answer else "Not found"}

{'=' * 80}
EXPLANATION:
{'=' * 80}

[This will be replaced with actual AI-generated explanation]

Analysis:
The correct answer is {suggested_answer if suggested_answer else "[answer]"}.

Key Reasoning:
• Point 1: [Detailed explanation of why this answer is correct]
• Point 2: [Additional context and best practices]
• Point 3: [Common scenarios where this applies]

Why This Matters:
[Explanation of the underlying concept and its importance]

Common Mistakes:
• Mistake 1: [What people often get wrong]
• Mistake 2: [Misconceptions to avoid]

Related Concepts:
• Concept 1: [Related topic or service]
• Concept 2: [Additional knowledge area]
• Concept 3: [Best practices]

Why Other Options Are Incorrect:"""
        
        # Add analysis for other options if available
        if options and suggested_answer:
            for key in sorted(options.keys()):
                if key != suggested_answer:
                    explanation += f"\n• Option {key}: [Why this is incorrect]"
        
        explanation += f"\n\n{'=' * 80}\n\n"
        
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
        
        # Skip if this is already an explained file or temporary file
        if "explained/" in name or name.endswith("_tmp") or name.endswith(".tmp"):
            print(f"⏭️ Skipping output/temp file: {name}")
            return jsonify({"status": "ignored", "reason": "Output or temp file"}), 200
        
        # Get blob reference
        blob = storage_client.bucket(bucket).blob(name)
        
        # Check if blob exists
        if not blob.exists():
            print(f"❌ Blob does not exist: {name}")
            return jsonify({"status": "ignored", "reason": "File not found"}), 404
        
        # Refresh blob metadata to get size
        blob.reload()
        
        print(f"✅ Blob found, size: {blob.size} bytes")
        
        # Detect file format
        is_json = detect_file_format(name)
        print(f"📋 File format: {'JSON' if is_json else 'TEXT'}")
        
        # Create output filename
        output_name = name.replace("clean/", "explained/")
        # Remove .json extension if present, add .txt
        if output_name.endswith(".json"):
            output_name = output_name[:-5] + ".txt"
        elif not output_name.endswith(".txt"):
            output_name += ".txt"
        
        # Check if output already exists to avoid reprocessing
        output_blob = storage_client.bucket(bucket).blob(output_name)
        if output_blob.exists():
            print(f"⏭️ Output file already exists: {output_name}")
            return jsonify({
                "status": "skipped",
                "reason": "Output file already exists",
                "output_file": output_name
            }), 200
        
        # Initialize output file with header
        header = f"""AI-Generated Question Explanations
Generated: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}
Source: {name}
Format: {'JSON' if is_json else 'TEXT'}

{'=' * 80}

"""
        write_output_incrementally(bucket, output_name, header, mode='create')
        print(f"📝 Created output file: {output_name}")
        
        # Process based on file format
        total_processed = 0
        batch = []
        
        if is_json:
            # For JSON, download entire file (usually not too large)
            print("📥 Downloading JSON file...")
            content = blob.download_as_text()
            questions = parse_json_questions(content)
            
            # Limit questions
            if len(questions) > MAX_QUESTIONS:
                print(f"⚠️ Limiting to {MAX_QUESTIONS} questions (found {len(questions)})")
                questions = questions[:MAX_QUESTIONS]
            
            # Process in batches
            for i in range(0, len(questions), BATCH_SIZE):
                batch = questions[i:i+BATCH_SIZE]
                print(f"🤖 Processing batch {i//BATCH_SIZE + 1} ({len(batch)} questions)")
                
                # Generate explanations for batch
                explanations = generate_explanation_batch(batch)
                
                # Write batch to output file
                batch_content = "\n".join(explanations)
                write_output_incrementally(bucket, output_name, batch_content, mode='append')
                
                total_processed += len(batch)
                print(f"✅ Batch written ({total_processed}/{len(questions)} total)")
        
        else:
            # For text files, use streaming
            for question_data in parse_text_questions_streaming(blob, max_questions=MAX_QUESTIONS):
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
            "batch_size": BATCH_SIZE,
            "file_format": "json" if is_json else "text"
        }), 200
        
    except gcp_exceptions.NotFound as e:
        print(f"⚠️ File not found: {e}")
        return jsonify({"status": "ignored", "reason": "File not found"}), 404
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
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080))), line.strip())
        
        if option_match:
            # Save previous option if exists
            if current_option:
                options[current_option] = ' '.join(current_text).strip()
            
            # Start new option
            current_option = option_match.group(1)
            current_text = [option_match.group(2)]
            in_options = True
        elif in_options and current_option and line.strip():
            # Continue previous option (multi-line option)
            current_text.append(line.strip())
        elif not in_options:
            # Still in question text
            question_lines.append(line)
    
    # Save last option
    if current_option:
        options[current_option] = ' '.join(current_text).strip()
    
    # Reconstruct question text
    if question_lines:
        question_text = '\n'.join(question_lines).strip()
    elif not options:
        # No options found, keep original question text
        question_text = question_text.strip()
    
    return {
        "question": question_text,
        "suggested_answer": suggested_answer,
        "options": options,
        "raw_data": original_section
    }

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
        options = q_data.get("options", {})
        
        # Format question text (don't truncate)
        question_text = question if isinstance(question, str) else str(question)
        
        # Format options
        options_text = ""
        if options:
            options_text = "\n\nOptions:"
            for key in sorted(options.keys()):
                value = options[key]
                # Truncate very long options
                if len(value) > 200:
                    value = value[:200] + "..."
                options_text += f"\n{key}. {value}"
        
        # Build the explanation
        explanation = f"""{'=' * 80}
Question {question_num}
{'=' * 80}

{question_text}{options_text}

Suggested Answer: {suggested_answer if suggested_answer else "Not found"}

{'=' * 80}
EXPLANATION:
{'=' * 80}

[This will be replaced with actual AI-generated explanation]

Analysis:
The correct answer is {suggested_answer if suggested_answer else "[answer]"}.

Key Reasoning:
• Point 1: [Detailed explanation of why this answer is correct]
• Point 2: [Additional context and best practices]
• Point 3: [Common scenarios where this applies]

Why This Matters:
[Explanation of the underlying concept and its importance]

Common Mistakes:
• Mistake 1: [What people often get wrong]
• Mistake 2: [Misconceptions to avoid]

Related Concepts:
• Concept 1: [Related topic or service]
• Concept 2: [Additional knowledge area]
• Concept 3: [Best practices]

Why Other Options Are Incorrect:"""
        
        # Add analysis for other options if available
        if options and suggested_answer:
            for key in sorted(options.keys()):
                if key != suggested_answer:
                    explanation += f"\n• Option {key}: [Why this is incorrect]"
        
        explanation += f"\n\n{'=' * 80}\n\n"
        
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
        
        # Get blob reference
        blob = storage_client.bucket(bucket).blob(name)
        
        # Refresh blob metadata to get size
        blob.reload()
        
        print(f"✅ Blob found, size: {blob.size} bytes")
        
        # Detect file format
        is_json = detect_file_format(name)
        print(f"📋 File format: {'JSON' if is_json else 'TEXT'}")
        
        # Create output filename
        output_name = name.replace("clean/", "explained/")
        # Remove .json extension if present, add .txt
        if output_name.endswith(".json"):
            output_name = output_name[:-5] + ".txt"
        elif not output_name.endswith(".txt"):
            output_name += ".txt"
        
        # Initialize output file with header
        header = f"""AI-Generated Question Explanations
Generated: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}
Source: {name}
Format: {'JSON' if is_json else 'TEXT'}

{'=' * 80}

"""
        write_output_incrementally(bucket, output_name, header, mode='create')
        print(f"📝 Created output file: {output_name}")
        
        # Process based on file format
        total_processed = 0
        batch = []
        
        if is_json:
            # For JSON, download entire file (usually not too large)
            print("📥 Downloading JSON file...")
            content = blob.download_as_text()
            questions = parse_json_questions(content)
            
            # Limit questions
            if len(questions) > MAX_QUESTIONS:
                print(f"⚠️ Limiting to {MAX_QUESTIONS} questions (found {len(questions)})")
                questions = questions[:MAX_QUESTIONS]
            
            # Process in batches
            for i in range(0, len(questions), BATCH_SIZE):
                batch = questions[i:i+BATCH_SIZE]
                print(f"🤖 Processing batch {i//BATCH_SIZE + 1} ({len(batch)} questions)")
                
                # Generate explanations for batch
                explanations = generate_explanation_batch(batch)
                
                # Write batch to output file
                batch_content = "\n".join(explanations)
                write_output_incrementally(bucket, output_name, batch_content, mode='append')
                
                total_processed += len(batch)
                print(f"✅ Batch written ({total_processed}/{len(questions)} total)")
        
        else:
            # For text files, use streaming
            for question_data in parse_text_questions_streaming(blob, max_questions=MAX_QUESTIONS):
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
            "batch_size": BATCH_SIZE,
            "file_format": "json" if is_json else "text"
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
