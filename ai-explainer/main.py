import os, json
from flask import Flask, request
from google.cloud import storage, aiplatform

app = Flask(__name__)
storage_client = storage.Client()

PROJECT = os.environ.get("PROJECT_ID")
REGION = os.environ.get("REGION", "us-central1")
BUCKET = os.environ.get("BUCKET")

aiplatform.init(project=PROJECT, location=REGION)

def generate_explanation(question, options, answer):
    prompt = f"""
Question: {question}
Options:
{json.dumps(options, indent=2)}
Correct Answer: {answer}

Explain why this answer is correct in clear exam-style explanation.
"""
    model = aiplatform.ChatModel.from_pretrained("gemini-1.0-pro")
    chat = model.start_chat()
    response = chat.send_message(prompt)
    return response.text

@app.route("/", methods=["POST"])
def handle():
    data = request.get_json(silent=True) or {}
    bucket = data["message"]["attributes"]["bucketId"]
    name = data["message"]["attributes"]["objectId"]

    if not name.startswith("clean/") or not name.endswith(".json"):
        return "ignored", 204

    raw = storage_client.bucket(bucket).blob(name).download_as_text()
    questions = json.loads(raw)

    output = []
    for q in questions:
        explanation = generate_explanation(q["question"], q["options"], q["correct"])
        output.append({
            "id": q["id"],
            "question": q["question"],
            "correct": q["correct"],
            "explanation": explanation
        })

    out_path = name.replace("clean/", "explanations/").replace(".json", "_explained.json")
    storage_client.bucket(bucket).blob(out_path).upload_from_string(
        json.dumps(output, indent=2), content_type="application/json"
    )

    return "OK", 200

