#!/usr/bin/env python3
"""Algorand x402 Challenge wash report - background job.

Rebuilds DATA_DIR/washreport.json from two public sources:
  * the GoPlausible facilitator's public challenge leaderboard (who is ranked, claimed totals)
  * the Algorand indexer (the actual USDC settlements into each merchant's payTo)

Scoring is a line-for-line port of the published additive wash-risk model
(agentkit packages/scoring/src/wash-risk.ts, methodology 0.1.0) so the numbers sold here
match the documented methodology. Everything is a statistical estimate from public data;
nothing here asserts intent. Run from cron; the web app only ever reads the output file.
"""
import json, os, sys, time, math, hashlib, statistics, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, Counter

FAC = os.getenv("FACILITATOR_URL", "https://facilitator.goplausible.xyz")
IDX = os.getenv("INDEXER_URL", "https://mainnet-idx.algonode.cloud")
USDC = 31566704
DATA_DIR = os.getenv("DATA_DIR", "/opt/x402/data")
OUT = os.path.join(DATA_DIR, "washreport.json")
MAX_MERCHANTS = int(os.getenv("WASH_MAX_MERCHANTS", "60"))
SAMPLE = int(os.getenv("WASH_SAMPLE", "1000"))          # most recent settlements per merchant
TOP_PAYERS = int(os.getenv("WASH_TOP_PAYERS", "10"))    # payers profiled per merchant
METHODOLOGY = "https://apeirontrade.github.io/provenance-site/methodology.html"
# Wallets this operator controls, declared so our own entry is graded against known ground truth.
SELF_DECLARED = {
    "K5HIZPOUUUBQ5WJ6I3DT6NGIQUMALYJYSVVBY7CXA3BYBWY6225DNNBDSA": "merchant payTo (this operator)",
    "LNLA2AAADJPTVCQK7XAL5N7HZWIUV7XXPNJRGHJK5PMPHUOOR4WPO6XSAI": "operator's scout-agent wallet",
    "2ZFD4V2OQPYJNRS25BBELH6FDH7FB7KLQ77IY5XITZKYW3AXX42K7QP45Y": "operator's development-machine test wallet",
}
for a in filter(None, os.getenv("WASH_SELF_WALLETS", "").split(",")):
    SELF_DECLARED.setdefault(a.strip(), "operator-declared test wallet")
OUR_PAYTO = os.getenv("AVM_ADDRESS", "K5HIZPOUUUBQ5WJ6I3DT6NGIQUMALYJYSVVBY7CXA3BYBWY6225DNNBDSA")


def gj(url, tries=4):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "provenance-washreport/0.1 (+https://blocksigner.org)"})
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 404: return None
            time.sleep(1.5 * (i + 1))
        except Exception as e:
            last = e; time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"fetch failed {url[:90]}: {last}")


def leaderboard():
    items, off, total = [], 0, None
    while True:
        d = gj(f"{FAC}/data/leaderboards?cat=merchants&range=all&env=mainnet&src=x402-global-challenge&limit=50&offset={off}&_={int(time.time())}")
        got = d.get("items", []); total = d.get("total", total)
        new = [i for i in got if i.get("address") and i["address"] not in {x["address"] for x in items}]
        items += new
        if not new or len(got) < 50 or len(items) >= (total or 0): break
        off += 50
    return items, total


def inflows(addr, limit=SAMPLE):
    """Most recent USDC transfers INTO addr. x402 settlements on Algorand appear both as grouped
    transfers with a facilitator fee payer and as plain transfers where the payer covers its own fee,
    so every inbound USDC transfer from another wallet is counted (see limitations)."""
    out, nxt, raw = [], None, 0
    while len(out) < limit:
        u = f"{IDX}/v2/transactions?address={addr}&address-role=receiver&asset-id={USDC}&tx-type=axfer&limit=1000" + (f"&next={nxt}" if nxt else "")
        d = gj(u)
        if not d: break
        for t in d.get("transactions", []):
            a = t.get("asset-transfer-transaction", {})
            if a.get("receiver") != addr or not a.get("amount"): continue
            raw += 1
            if t["sender"] == addr: continue
            out.append({"payer": t["sender"], "amt": a["amount"] / 1e6, "ts": t.get("round-time", 0), "round": t.get("confirmed-round", 0)})
        nxt = d.get("next-token")
        if not nxt or raw >= limit * 2: break
    return out[:limit], raw


def outflow_receivers(addr):
    d = gj(f"{IDX}/v2/transactions?address={addr}&address-role=sender&limit=500") or {}
    rec = set()
    for t in d.get("transactions", []):
        r = (t.get("payment-transaction") or {}).get("receiver") or (t.get("asset-transfer-transaction") or {}).get("receiver")
        if r and r != addr: rec.add(r)
    return rec


_payer_cache = {}
def payer_profile(p):
    if p in _payer_cache: return _payer_cache[p]
    prof = {"created_round": None, "auth": None, "funder": None, "current_round": None}
    try:
        a = gj(f"{IDX}/v2/accounts/{p}?exclude=all") or {}
        acct = a.get("account", {})
        prof["created_round"] = acct.get("created-at-round"); prof["auth"] = acct.get("auth-addr"); prof["current_round"] = a.get("current-round")
        f = gj(f"{IDX}/v2/transactions?address={p}&address-role=receiver&asset-id={USDC}&tx-type=axfer&limit=50") or {}
        by = Counter()
        for t in f.get("transactions", []):
            x = t.get("asset-transfer-transaction", {})
            if x.get("receiver") == p and t.get("sender") != p: by[t["sender"]] += x.get("amount", 0)
        if by: prof["funder"] = by.most_common(1)[0][0]
    except Exception as e:
        prof["error"] = str(e)[:80]
    _payer_cache[p] = prof
    return prof


def hhi(shares): return sum(s * s for s in shares)
def grade(score): return "A" if score < 10 else "B" if score < 20 else "C" if score < 42 else "D" if score < 70 else "F"


def assess(addr, pays, outflows):
    """Port of assessWashRisk(). Returns (score, level, indicators, facts)."""
    ind, raw = [], 0.0
    def add(name, contribution, weight, detail):
        nonlocal raw
        if contribution > 0.05: ind.append({"name": name, "contribution": round(contribution, 3), "weight": weight, "detail": detail})
        raw += contribution * weight
    rev = defaultdict(float)
    for p in pays: rev[p["payer"]] += p["amt"]
    payers = list(rev); n = len(payers); total = sum(rev.values())
    top = sorted(payers, key=lambda k: -rev[k])[:TOP_PAYERS]
    with ThreadPoolExecutor(max_workers=5) as ex: profs = dict(zip(top, ex.map(payer_profile, top)))

    sev = 1 if n <= 1 else 0.9 if n <= 2 else 0.65 if n <= 4 else 0.35 if n <= 9 else 0
    add("few_payers", sev, 45, f"{n} distinct payer{'s' if n != 1 else ''} in sample")

    # clusters: payers sharing a funder or an auth address collapse into one cluster (union by key)
    key = {}
    for p in payers:
        pr = profs.get(p) or {}
        key[p] = pr.get("auth") or (("F:" + pr["funder"]) if pr.get("funder") and pr["funder"] != addr else None) or p
    crev = defaultdict(float)
    for p in payers: crev[key[p]] += rev[p]
    if total > 0:
        sh = sorted((v / total for v in crev.values()), reverse=True)
        add("concentration", max(0.0, (hhi(sh) - 0.1) / 0.9), 20, f"top payer cluster holds {round(sh[0] * 100)}% of sampled revenue (HHI {hhi(sh):.2f})")

    # self-dealing: revenue from payers the merchant itself pays (or the merchant paying itself)
    loop = [p for p in payers if p in outflows or p == addr or (profs.get(p) or {}).get("funder") == addr]
    cyc = sum(rev[p] for p in loop) / total if total else 0
    add("self_dealing", cyc, 25, f"{round(cyc * 100)}% of sampled revenue comes from wallets the merchant funds or pays")

    ages = []
    first = {}
    for p in pays:
        if p["payer"] not in first or p["round"] < first[p["payer"]]: first[p["payer"]] = p["round"]
    for p in top:
        cr = (profs.get(p) or {}).get("created_round")
        if cr and first.get(p): ages.append(max(0, first[p] - cr) * 2.85 / 86400)      # rounds -> days (approx 2.85 s/round)
    med_age = statistics.median(ages) if ages else None
    if med_age is not None:
        s = 1 if med_age < 1 else 0.7 if med_age < 3 else 0.4 if med_age < 7 else 0.15 if med_age < 30 else 0
        add("fresh_wallets", s, 15, f"median top-payer wallet age at first payment {med_age:.1f}d")

    if len(pays) >= 5:
        ts = sorted(p["ts"] for p in pays); d = [b - a for a, b in zip(ts, ts[1:])]
        mean = sum(d) / len(d); cv = (statistics.pstdev(d) / mean) if mean > 0 else 0
        ident = max(Counter(round(x) for x in d).values()) / len(d)
        s = max(1 - cv / 0.3 if cv < 0.3 else 0, ident if ident > 0.5 else 0)
        add("metronomic", s, 15, f"timing CV {cv:.2f}, {round(ident * 100)}% identical-interval gaps")

    auths = Counter(pr["auth"] for pr in profs.values() if pr.get("auth"))
    shared = sum(c for c in auths.values() if c > 1)
    if auths: add("rekey_sybil", shared / n if n else 0, 20, f"{round((shared / n if n else 0) * 100)}% of payers share a controlling auth address")

    fund = Counter(pr["funder"] for pr in profs.values() if pr.get("funder"))
    if fund:
        f, c = fund.most_common(1)[0]; frac = c / max(1, len(profs))
        if frac > 0.3: add("single_funder", frac, 15, f"{round(frac * 100)}% of profiled payers funded by one wallet ({f[:6]}…{f[-4:]})")

    score = min(100, round(raw)); level = "critical" if score >= 70 else "high" if score >= 45 else "medium" if score >= 20 else "low"
    ind.sort(key=lambda i: -i["contribution"] * i["weight"])
    facts = {"payers_in_sample": n, "payer_clusters": len(crev), "top_payers": [{"payer": p, "share_pct": round(rev[p] / total * 100, 1) if total else 0,
             "funder": (profs.get(p) or {}).get("funder"), "paid_by_merchant": p in outflows, "self_declared": SELF_DECLARED.get(p)} for p in top[:5]]}
    return score, level, ind, facts, profs, rev


def main():
    t0 = time.time(); items, total = leaderboard()
    items = items[:MAX_MERCHANTS]
    if not any(i["address"] == OUR_PAYTO for i in items):
        items.append({"rank": None, "address": OUR_PAYTO, "sub": "blocksigner.org", "volume": None, "settles": None, "id": None})
    rows, payer_merchants, funder_payers, errors = [], defaultdict(set), defaultdict(set), []
    for n, it in enumerate(items, 1):
        addr = it["address"]
        try:
            pays, raw_seen = inflows(addr); outs = outflow_receivers(addr)
            if not pays:
                rows.append({"rank": it.get("rank"), "domain": it.get("sub"), "payTo": addr, "claimed_volume_usdc": it.get("volume"), "claimed_settles": it.get("settles"),
                             "grade": "n/v", "level": "not verifiable", "score": None, "sample_settlements": 0,
                             "note": "no inbound USDC transfers to this payTo were visible on-chain; not graded"})
                continue
            score, level, ind, facts, profs, rev = assess(addr, pays, outs)
            for p in rev: payer_merchants[p].add(addr)
            for p, pr in profs.items():
                if pr.get("funder"): funder_payers[pr["funder"]].add(p)
            vol = it.get("volume"); sample_vol = sum(x["amt"] for x in pays)
            rows.append({"rank": it.get("rank"), "domain": it.get("sub"), "payTo": addr, "claimed_volume_usdc": round(vol, 4) if vol is not None else None,
                         "claimed_settles": it.get("settles"), "sample_settlements": len(pays), "sample_volume_usdc": round(sample_vol, 4),
                         "sample_covers_all": bool(it.get("settles") and len(pays) >= it["settles"] * 0.95),
                         "window": {"from": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(min(x["ts"] for x in pays))), "to": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(max(x["ts"] for x in pays)))},
                         "score": score, "level": level, "grade": grade(score),
                         "organic_adjusted_volume_usdc": round(vol * (1 - score / 100), 4) if vol is not None else None,
                         "indicators": ind[:4], **facts, "is_this_operator": addr == OUR_PAYTO})
            print(f"[{n}/{len(items)}] {str(it.get('sub'))[:34]:34} score {score:3} {grade(score)}  payers {facts['payers_in_sample']:4}  sample {len(pays)}", flush=True)
        except Exception as e:
            errors.append({"payTo": addr, "error": str(e)[:140]}); print("ERR", addr[:8], e, flush=True)

    graded = [r for r in rows if r.get("score") is not None and r.get("claimed_volume_usdc")]
    tot = sum(r["claimed_volume_usdc"] for r in graded); org = sum(r["organic_adjusted_volume_usdc"] for r in graded)
    roaming = sorted(((p, ms) for p, ms in payer_merchants.items() if len(ms) >= 3), key=lambda x: -len(x[1]))
    rings = sorted(((f, ps) for f, ps in funder_payers.items() if len(ps) >= 2), key=lambda x: -len(x[1]))
    merch = {r["payTo"]: r.get("domain") for r in rows}
    ours = next((r for r in rows if r["payTo"] == OUR_PAYTO), None)
    report = {
        "service": "Provenance - Algorand x402 Challenge wash report",
        "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "build_seconds": round(time.time() - t0),
        "chain": "Algorand mainnet", "asset": "USDC (ASA 31566704)",
        "scope": f"{len(rows)} of {total} merchants on the GoPlausible x402-global-challenge leaderboard, highest claimed volume first",
        "headline": {"merchants_scored": len(graded), "claimed_volume_usdc": round(tot, 2), "organic_adjusted_volume_usdc": round(org, 2),
                     "estimated_non_organic_pct": round((1 - org / tot) * 100, 1) if tot else None,
                     "grades": dict(Counter(r["grade"] for r in rows)),
                     "reading": "volume-weighted share of claimed challenge volume that the wash-risk model does not attribute to independent multi-party demand"},
        "merchants": sorted(rows, key=lambda r: (r.get("rank") is None, r.get("rank") or 0)),
        "this_operator": {"payTo": OUR_PAYTO, "grade": ours and ours.get("grade"), "score": ours and ours.get("score"),
                          "statement": "We grade our own entry with the same code. Almost all of our settlements to date are our own end-to-end tests from wallets we control, "
                                       "which we declare here; the model is expected to flag that, and this entry doubles as a known-answer check on the scoring."},
        "clusters": {"roaming_payers": [{"payer": p, "merchants_paid": len(ms), "merchants": sorted(filter(None, (merch.get(m) for m in ms)))[:12], "self_declared": SELF_DECLARED.get(p)} for p, ms in roaming[:40]],
                     "shared_funders": [{"funder": f, "funds_payers": len(ps), "funder_is_merchant": merch.get(f), "payers": sorted(ps)[:12]} for f, ps in rings[:40]]},
        "method": {"version": "0.1.0", "url": METHODOLOGY, "weights": {"few_payers": 45, "self_dealing": 25, "concentration": 20, "rekey_sybil": 20, "fresh_wallets": 15, "metronomic": 15, "single_funder": 15},
                   "levels": {"low": "<20", "medium": "20-44", "high": "45-69", "critical": ">=70"}, "grades": {"A": "<10", "B": "10-19", "C": "20-41", "D": "42-69", "F": ">=70"},
                   "sampling": f"up to the {SAMPLE} most recent inbound USDC transfers per payTo; top {TOP_PAYERS} payers by revenue profiled for wallet age, funder and auth address"},
        "limitations": ["Statistical estimates from public data; not a finding about any operator's intent.",
                        "Weights are set by judgment, not fitted; there is no labelled ground-truth set beyond this operator's self-declared wallets, so no false-positive rate is claimed.",
                        "High-volume merchants are scored on a recent sample, not their full history.",
                        "Every inbound USDC transfer to a payTo is counted; transfers that were not x402 payments (top-ups, refunds) cannot always be separated.",
                        "A single large legitimate customer, or a scheduled job that is real demand, can look like concentration or metronomic timing.",
                        "Funder attribution uses each payer's largest recent USDC inflow; exchange or bridge hot wallets can appear as a shared funder."],
        "errors": errors,
    }
    os.makedirs(DATA_DIR, exist_ok=True); tmp = OUT + ".tmp"
    with open(tmp, "w") as f: json.dump(report, f, separators=(",", ":"))
    os.replace(tmp, OUT)
    print(f"wrote {OUT}: {len(rows)} merchants, non-organic {report['headline']['estimated_non_organic_pct']}%, {round(time.time() - t0)}s", flush=True)


if __name__ == "__main__":
    main()
