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
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "3"))
MAX_QUESTIONS = int(os.environ.get("MAX_QUESTIONS", "1000"))
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
    """Parse JSON format questions."""
    try:
        data = json.loads(content)
        
        if isinstance(data, dict):
            if "questions" in data:
                questions_list = data["questions"]
            elif "items" in data:
                questions_list = data["items"]
            elif "data" in data:
                questions_list = data["data"]
            else:
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
        
        parsed_questions = []
        for i, q in enumerate(questions_list, 1):
            if isinstance(q, dict):
                question_text = (
                    q.get("question") or 
                    q.get("text") or 
                    q.get("description") or
                    q.get("prompt") or
                    str(q)
                )
                
                suggested_answer = (
                    q.get("suggested_answer") or
                    q.get("correct_answer") or
                    q.get("answer") or
                    q.get("correctAnswer") or
                    None
                )
                
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

def parse_question_section(section):
    """Parse a single question section."""
    original_section = section
    
    suggested_answer = None
    answer_match = re.search(r'Suggested Answer:\s*([A-Z])', section, re.IGNORECASE)
    if answer_match:
        suggested_answer = answer_match.group(1)
        section = section[:answer_match.start()].strip()
    
    question_text = section
    question_number_match = re.match(r'Question\s+(\d+):\s*', section)
    if question_number_match:
        question_text = section[question_number_match.end():]
    
    options = {}
    lines = question_text.split('\n')
    current_option = None
    current_text = []
    question_lines = []
    in_options = False
    
    for line in lines:
        option_match = re.match(r'^([A-D])[.)\s]+(.*)$', line.strip())
        
        if option_match:
            if current_option:
                options[current_option] = ' '.join(current_text).strip()
            
            current_option = option_match.group(1)
            current_text = [option_match.group(2)]
            in_options = True
        elif in_options and current_option and line.strip():
            current_text.append(line.strip())
        elif not in_options:
            question_lines.append(line)
    
    if current_option:
        options[current_option] = ' '.join(current_text).strip()
    
    if question_lines:
        question_text = '\n'.join(question_lines).strip()
    elif not options:
        question_text = question_text.strip()
    
    return {
        "question": question_text,
        "suggested_answer": suggested_answer,
        "options": options,
        "raw_data": original_section
    }

def parse_text_questions_streaming(blob, max_questions=None):
    """Stream and parse questions from text file."""
    delimiter = "=" * 61
    buffer = ""
    question_count = 0
    
    print(f"📖 Starting to stream text file...")
    chunk_size = 1024 * 1024
    
    try:
        blob.reload()
        total_size = blob.size
        print(f"📏 File size: {total_size} bytes")
        
        start = 0
        while start < total_size:
            end = min(start + chunk_size, total_size)
            chunk_bytes = blob.download_as_bytes(start=start, end=end)
            buffer += chunk_bytes.decode('utf-8', errors='ignore')
            start = end
            
            while delimiter in buffer:
                section, buffer = buffer.split(delimiter, 1)
                section = section.strip()
                
                if section and len(section) > 50:
                    parsed = parse_question_section(section)
                    question_count += 1
                    parsed["number"] = question_count
                    yield parsed
                    
                    if max_questions and question_count >= max_questions:
                        print(f"⚠️ Reached max questions limit ({max_questions})")
                        return
        
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

def generate_explanation_with_gemini(question_text, options, suggested_answer):
    """Use Gemini to generate explanation."""
    try:
        model = GenerativeModel("gemini-1.5-flash")
        
        options_text = ""
        if options:
            options_text = "\n\nOptions:"
            for key in sorted(options.keys()):
                options_text += f"\n{key}. {options[key]}"
        
        prompt = f"""You are an expert technical instructor explaining exam questions. 

Question:
{question_text}{options_text}

Correct Answer: {suggested_answer if suggested_answer else "Not specified"}

Please provide a detailed explanation that includes:

1. **Why the correct answer is right**: Explain in detail why answer {suggested_answer} is the best choice, covering the technical concepts and best practices involved.

2. **Key Concepts**: List and explain the important concepts needed to understand this question.

3. **Why other options are wrong**: For each incorrect option, explain specifically why it's not the best choice.

4. **Real-world application**: Briefly describe when and how this knowledge would be applied in practice.

5. **Common mistakes**: What mistakes do people commonly make with this concept?

Format your response in a clear, educational manner suitable for someone studying for this certification exam. Be thorough but concise."""

        generation_config = GenerationConfig(
            temperature=0.4,
            top_p=0.8,
            top_k=40,
            max_output_tokens=2048,
        )
        
        response = model.generate_content(
            prompt,
            generation_config=generation_config
        )
        
        if response and response.text:
            return response.text
        else:
            print("⚠️ Empty response from Gemini")
            return "[AI explanation not available]"
            
    except Exception as e:
        print(f"❌ Error generating explanation with Gemini: {e}")
        print(traceback.format_exc())
        return f"[Error generating explanation: {str(e)}]"

def generate_explanation_batch(questions_batch):
    """Generate explanations for a batch of questions."""
    explanations = []
    
    for q_data in questions_batch:
        question_num = q_data["number"]
        question = q_data["question"]
        suggested_answer = q_data["suggested_answer"]
        options = q_data.get("options", {})
        
        question_text = question if isinstance(question, str) else str(question)
        
        options_display = ""
        if options:
            options_display = "\n\nOptions:"
            for key in sorted(options.keys()):
                value = options[key]
                options_display += f"\n{key}. {value}"
        
        print(f"🤖 Generating AI explanation for Question {question_num}...")
        
        ai_explanation = generate_explanation_with_gemini(question_text, options, suggested_answer)
        
        explanation = f"""{'=' * 80}
Question {question_num}
{'=' * 80}

{question_text}{options_display}

Suggested Answer: {suggested_answer if suggested_answer else "Not found"}

{'=' * 80}
AI-GENERATED EXPLANATION:
{'=' * 80}

{ai_explanation}

{'=' * 80}

"""
        explanations.append(explanation)
        time.sleep(0.5)
    
    return explanations

def write_output_incrementally(bucket, output_name, content, mode='append'):
    """Write to output blob incrementally."""
    output_blob = storage_client.bucket(bucket).blob(output_name)
    
    if mode == 'create':
        output_blob.upload_from_string(content, content_type="text/plain")
    else:
        try:
            existing = output_blob.download_as_text()
            new_content = existing + content
            output_blob.upload_from_string(new_content, content_type="text/plain")
        except Exception:
            output_blob.upload_from_string(content, content_type="text/plain")

@app.route("/", methods=["POST"])
def handle():
    try:
        req_json = request.get_json(silent=True) or {}
        print("=" * 80)
        print("📥 Received Event:")
        print(json.dumps(req_json)[:1000])
        print("=" * 80)
        
        if storage_client is None:
            print("❌ Storage client not initialized")
            return jsonify({"error": "Storage client not initialized"}), 500
        
        bucket, name = extract_event_info(req_json)
        
        if not bucket or not name:
            print("⚠️ No bucket/object info in event")
            return jsonify({"status": "ignored", "reason": "No bucket/object in event"}), 200
        
        print(f"🪣 Bucket: {bucket}")
        print(f"📄 Object: {name}")
        
        if not name.startswith("clean/"):
            print(f"⏭️ Skipping non-clean file: {name}")
            return jsonify({"status": "ignored", "reason": "Not a clean file"}), 200
        
        if "explained/" in name or name.endswith("_tmp") or name.endswith(".tmp"):
            print(f"⏭️ Skipping output/temp file: {name}")
            return jsonify({"status": "ignored", "reason": "Output or temp file"}), 200
        
        blob = storage_client.bucket(bucket).blob(name)
        
        if not blob.exists():
            print(f"❌ Blob does not exist: {name}")
            return jsonify({"status": "ignored", "reason": "File not found"}), 404
        
        blob.reload()
        print(f"✅ Blob found, size: {blob.size} bytes")
        
        is_json = detect_file_format(name)
        print(f"📋 File format: {'JSON' if is_json else 'TEXT'}")
        
        output_name = name.replace("clean/", "explained/")
        if output_name.endswith(".json"):
            output_name = output_name[:-5] + ".txt"
        elif not output_name.endswith(".txt"):
            output_name += ".txt"
        
        output_blob = storage_client.bucket(bucket).blob(output_name)
        if output_blob.exists():
            print(f"⏭️ Output file already exists: {output_name}")
            return jsonify({
                "status": "skipped",
                "reason": "Output file already exists",
                "output_file": output_name
            }), 200
        
        header = f"""AI-Generated Question Explanations
Generated: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}
Source: {name}
Format: {'JSON' if is_json else 'TEXT'}

{'=' * 80}

"""
        write_output_incrementally(bucket, output_name, header, mode='create')
        print(f"📝 Created output file: {output_name}")
        
        total_processed = 0
        batch = []
        
        if is_json:
            print("📥 Downloading JSON file...")
            content = blob.download_as_text()
            questions = parse_json_questions(content)
            
            if len(questions) > MAX_QUESTIONS:
                print(f"⚠️ Limiting to {MAX_QUESTIONS} questions (found {len(questions)})")
                questions = questions[:MAX_QUESTIONS]
            
            for i in range(0, len(questions), BATCH_SIZE):
                batch = questions[i:i+BATCH_SIZE]
                print(f"🤖 Processing batch {i//BATCH_SIZE + 1} ({len(batch)} questions)")
                
                explanations = generate_explanation_batch(batch)
                batch_content = "\n".join(explanations)
                write_output_incrementally(bucket, output_name, batch_content, mode='append')
                
                total_processed += len(batch)
                print(f"✅ Batch written ({total_processed}/{len(questions)} total)")
        
        else:
            for question_data in parse_text_questions_streaming(blob, max_questions=MAX_QUESTIONS):
                batch.append(question_data)
                
                if len(batch) >= BATCH_SIZE:
                    print(f"🤖 Processing batch of {len(batch)} questions (total: {total_processed + len(batch)})")
                    
                    explanations = generate_explanation_batch(batch)
                    batch_content = "\n".join(explanations)
                    write_output_incrementally(bucket, output_name, batch_content, mode='append')
                    
                    total_processed += len(batch)
                    batch = []
                    
                    print(f"✅ Batch written to output ({total_processed} questions processed)")
            
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
        
    except NotFound as e:
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
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
