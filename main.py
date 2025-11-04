import os
import re
import json
from flask import Flask, request
from google.cloud import storage

app = Flask(__name__)
storage_client = storage.Client()
BUCKET = os.environ["BUCKET"]

def clean_text(text: str) -> str:
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def parse_blocks(raw: str):
    content = re.sub(r'#{2,}.*?topic.*?question.*?discussion','##QSPLIT',raw,flags=re.IGNORECASE|re.DOTALL)
    sections = [s for s in content.split('##QSPLIT') if s.strip()]
    out=[]; q=1

    for sec in sections:
        qm=re.search(r'\[.*?Questions.*?\]\s*(.*?)(?=\n[A-Z]\.|Suggested Answer:|\*\*Answer:)',sec,flags=re.IGNORECASE|re.DOTALL)
        if not qm:
            qm=re.search(r'Question.*?\n(.*?)(?=\n[A-Z]\.|Suggested Answer:|\*\*Answer:)',sec,flags=re.IGNORECASE|re.DOTALL)
        if not qm: continue
        question=clean_text(qm.group(1))
        if len(question)<15: continue

        option_pat=r'([A-Z])\.\s*(.*?)(?=\n[A-Z]\.\s|\nSuggested Answer:|\n\*\*Answer:|$)'
        option_pairs=re.findall(option_pat,sec,re.DOTALL)
        options={a:clean_text(b) for a,b in option_pairs if clean_text(b)}
        if len(options)<2: continue

        am=re.search(r'Suggested Answer:\s*([A-Z ,]+)',sec)
        if not am: am=re.search(r'\*\*Answer:\s*([A-Z ,]+)\*\*',sec)
        correct="N/A"
        if am:
            parts=[p.strip() for p in re.split(r'[,\s]+',am.group(1)) if p.strip()]
            correct=",".join(parts)

        out.append({"id":q,"question":question,"options":options,"correct":correct})
        q+=1
    return out

@app.route("/", methods=["POST"])
def handle():
    data = request.get_json(silent=True) or {}
    msg = data.get("message", {})
    attrs = msg.get("attributes", {})
    bucket = attrs.get("bucketId")
    name = attrs.get("objectId")
    if not name or not bucket: return "no object",204
    if not name.startswith("input/"): return "ignored",204

    raw = storage_client.bucket(bucket).blob(name).download_as_text(errors="ignore")
    parsed = parse_blocks(raw)
    base = name.replace("input/","clean/")

    if not parsed:
        storage_client.bucket(bucket).blob(base+".err.txt").upload_from_string("No questions parsed")
        return "done",200

    json_key = base.rsplit(".",1)[0]+".json"
    storage_client.bucket(bucket).blob(json_key).upload_from_string(
        json.dumps(parsed,indent=2),content_type="application/json")

    lines=[]
    for it in parsed:
        lines.append(f"Question {it['id']}:\n{it['question']}\n\nOptions:\n")
        for k in sorted(it["options"].keys()):
            lines.append(f"{k}. {it['options'][k]}\n")
        lines.append("="*61); lines.append("="*61)
        lines.append(f"Suggested Answer: {it['correct']}\n")
    txt_key = base.rsplit(".",1)[0]+"_clean.txt"
    storage_client.bucket(bucket).blob(txt_key).upload_from_string("\n".join(lines))
    return "ok",200

