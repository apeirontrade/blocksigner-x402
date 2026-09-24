"""Patch 7 for app.py: a weekly integrity note on /provenance/integrity.
Each run of the integrity job appends a dated snapshot to DATA_DIR/integrity_history.jsonl; the page shows
the newest note, the change since last week, and the trend table. Usage: python3 weekly_patch.py [app.py]
"""
import sys, ast
p = sys.argv[1] if len(sys.argv) > 1 else "/opt/x402/app/app.py"
s = open(p, encoding="utf-8").read()
if "integrity_history" in s:
    print("already applied"); sys.exit(0)

def rep(old, new, count=1):
    global s
    n = s.count(old); assert n == count, (old[:70], n)
    s = s.replace(old, new)

rep('''INTEGRITY_BASE = os.path.join(DATA_DIR, "integrity_base.json")''',
'''INTEGRITY_BASE = os.path.join(DATA_DIR, "integrity_base.json")
INTEGRITY_HIST = os.path.join(DATA_DIR, "integrity_history.jsonl")

def integrity_history(limit=8):
    """One snapshot per run, newest last: [{date, linked_pct, independent_pct, merchants, usdc}]."""
    rows = []
    try:
        with open(INTEGRITY_HIST, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try: rows.append(json.loads(line))
                    except Exception: pass
    except Exception:
        return []
    seen, out = set(), []
    for r in rows:                      # one row per date, the last run of that day wins
        out = [x for x in out if x.get("date") != r.get("date")] + [r]
    return out[-limit:]

def _weekly_note():
    h = integrity_history()
    if not h:
        return ""
    e = _html.escape
    cur = h[-1]
    prev = next((x for x in reversed(h[:-1]) if x.get("date") != cur.get("date")), None)
    delta = ""
    if prev:
        d = (cur.get("linked_pct") or 0) - (prev.get("linked_pct") or 0)
        word = "up" if d > 0.05 else "down" if d < -0.05 else "unchanged"
        delta = (f" That is {word}" + (f" {abs(d):.1f} points" if word != "unchanged" else "") +
                 f" from {prev['linked_pct']:.1f}% on {e(str(prev.get('date')))}.")
    rows = "".join(f"<tr><td>{e(str(x.get('date')))}</td><td class=\\"num\\">{x.get('merchants', 0)}</td>"
                   f"<td class=\\"num\\">${x.get('usdc', 0):,.2f}</td><td class=\\"num\\">{x.get('linked_pct', 0):.1f}%</td>"
                   f"<td class=\\"num\\">{x.get('independent_pct', 0):.2f}%</td></tr>" for x in h)
    return (f'<div class="card"><div class="lbl">This week\\'s number</div>'
            f'<p><b>{cur.get("linked_pct", 0):.1f}% of the last 30 days of challenge volume came from wallets linked to the merchant being paid.</b>{e(delta)}</p>'
            f'<table class="integ"><thead><tr><th>Run</th><th class="num">Merchants</th><th class="num">USDC traced</th>'
            f'<th class="num">Linked</th><th class="num">Independent</th></tr></thead><tbody>{rows}</tbody></table>'
            f'<p class="mut" style="margin-top:12px">Recomputed automatically whenever the wash report rebuilds. '
            f'Machine-readable history: <a href="{e(PUBLIC_BASE)}/provenance/history.json">history.json</a>.</p></div>')

@app.route("/provenance/history.json")
def provenance_history():
    return jsonify({"runs": integrity_history(limit=60), "source": PUBLIC_BASE + "/provenance/integrity"})''')

rep('''<div class="card"><div class="lbl">Volume by payer class''',
    '''{_weekly_note()}
<div class="card"><div class="lbl">Volume by payer class''')

ast.parse(s)
open(p, "w", encoding="utf-8").write(s)
print("weekly note added + syntax ok:", p)
