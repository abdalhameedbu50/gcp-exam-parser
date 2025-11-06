from flask import Flask, request, jsonify
from transformers import pipeline

app = Flask(__name__)

# Load model once (small and fast)
generator = pipeline("text2text-generation", model="google/flan-t5-base")

@app.route("/generate", methods=["POST"])
def generate():
    data = request.get_json()
    prompt = data.get("prompt", "")
    max_tokens = data.get("max_tokens", 256)

    result = generator(prompt, max_length=max_tokens, do_sample=True, temperature=0.7)
    return jsonify({"output": result[0]["generated_text"]})

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

