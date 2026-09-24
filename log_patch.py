import ast, io
p = "/opt/x402/app/app.py"
s = io.open(p, encoding="utf-8").read()
assert "REQLOG" not in s, "already applied"

s = s.replace('''@app.before_request
def _guard():''',
'''REQLOG = os.path.join(DATA_DIR, "requests.jsonl")

@app.before_request
def _stamp():
    g.t0 = time.time()

@app.after_request
def _reqlog(resp):
    """One JSON line per paid request: who paid, what they got, how long it took."""
    try:
        if not request.path.startswith(PAID_PREFIX):
            return resp
        rec = {"ts": now_iso(), "path": request.path, "status": resp.status_code,
               "ms": int((time.time() - getattr(g, "t0", time.time())) * 1000),
               "payer": (payer_address() or "")[:12] or None,
               "trial": bool(getattr(g, "trial", False)),
               "ip": (request.headers.get("X-Forwarded-For", request.remote_addr) or "").split(",")[0].strip(),
               "ua": (request.headers.get("User-Agent") or "")[:80],
               "bytes": resp.calculate_content_length() or 0}
        with open(REQLOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\\n")
    except Exception:
        pass
    return resp

@app.before_request
def _guard():''', 1)

ast.parse(s)
io.open(p, "w", encoding="utf-8").write(s)
print("request log added + syntax ok")
