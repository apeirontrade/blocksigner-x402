"""Daily payer-integrity report for the Algorand x402 Challenge.
Runs wash_audit (funder trace) on every merchant in the latest wash report, aggregates the payer classes and
writes DATA_DIR/integrity.json, which /provenance/integrity serves. Per-merchant audits are cached 6 h by app.wash_audit.
"""
import json, os, sys, time
sys.path.insert(0, "/opt/x402/app")
os.chdir("/opt/x402/app")
import app as A

def main():
    rep = A.wash_report() or {}
    merchants = [r for r in rep.get("merchants", []) if r.get("payTo")]
    tot = {}; wallets = {}; settles = {}; payers = set(); n_ok = 0; t0 = time.time()
    for r in merchants:
        try:
            a = A.wash_audit(r["payTo"])
        except Exception as e:
            print("skip", r["payTo"][:8], str(e)[:80]); continue
        n_ok += 1
        for c in a.get("classes", []):
            tot[c["key"]] = tot.get(c["key"], 0.0) + c["usdc"]; wallets[c["key"]] = wallets.get(c["key"], 0) + c["wallets"]
        for p in a.get("payers", []):
            payers.add(p["payer"]); settles[p["class"]] = settles.get(p["class"], 0) + p["calls"]
        time.sleep(0.5)
    T = sum(tot.values()) or 1.0
    labels = {"merchant_funded": "Payer wallets funded directly by the merchant they pay",
              "single_merchant_linked": "Heavy single-merchant payers funded two hops from that merchant",
              "merchant_self": "Merchant payTo wallets paying themselves or another merchant",
              "light": "Light or unlinked payers", "shared_bot": "Independent wallets paying 3 or more merchants (autonomous sweepers)"}
    classes = [{"key": k, "label": v, "wallets": wallets.get(k, 0), "usdc": round(tot.get(k, 0.0), 2), "settlements": settles.get(k, 0),
                "share_pct": round(100 * tot.get(k, 0.0) / T, 2)} for k, v in labels.items()]
    linked = sum(c["share_pct"] for c in classes if c["key"] in ("merchant_funded", "single_merchant_linked", "merchant_self"))
    shared = next(c["share_pct"] for c in classes if c["key"] == "shared_bot")
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    out = {
        "title": "Algorand x402 Challenge: payer integrity report",
        "as_of": now,
        "window": {"from": time.strftime("%Y-%m-%d", time.gmtime(time.time() - 30 * 86400)), "to": time.strftime("%Y-%m-%d", time.gmtime()), "days": 30},
        "scope": {"merchants_analyzed": n_ok, "note": "Every merchant in the latest Provenance wash report (the challenge leaderboard), traced through the public Algorand indexer; up to 40 payers per merchant",
                  "distinct_payers": len(payers), "total_usdc": round(T, 2), "total_settlements": sum(settles.values())},
        "headline": f"{linked:.1f}% of 30-day volume comes from wallets linked to the merchant being paid; independent multi-merchant payers account for {shared:.2f}%.",
        "classes": classes,
        "method": {"summary": "Every payer wallet was traced to its funders (inbound ALGO/USDC senders) and to its own outbound USDC to any known x402 payTo. "
                              "A payer funded by the merchant it pays is classed merchant-funded; a heavy single-merchant payer whose funder was itself funded by that merchant is classed linked; "
                              "wallets that pay three or more unrelated merchants are classed as shared bots.", "url": A._wj.METHODOLOGY, "version": "0.2.1-daily"},
        "caveats": ["Statistical estimate from public on-chain data; it is not a finding about any operator's intent.",
                    "Merchant-funded payers can be legitimate: a merchant may fund its own customers' agents as a promotion or for testing.",
                    "Up to 40 payers per merchant and 1,000 recent transfers per address are traced; very large merchants are sampled.",
                    "The window counts USDC transfers into a payTo, not only facilitator-settled x402 payments."],
        "this_operator": {"payTo": A.AVM_ADDRESS, "grade": next((r.get("grade") for r in merchants if r.get("payTo") == A.AVM_ADDRESS), None),
                          "statement": "Almost all of Agent World's settlements are our own end-to-end tests from wallets we control, which we declare. We grade ourselves with the same code and publish it as a known-answer check."},
        "per_merchant_grades": A.PUBLIC_BASE + "/commission/washreport",
        "check_one": A.PUBLIC_BASE + "/commission/washcheck?payTo=<address>&trial=1",
        "audit_one": A.PUBLIC_BASE + "/commission/washaudit?payTo=<address>",
        "elapsed_seconds": round(time.time() - t0),
    }
    # append a dated snapshot so the page can show the week-on-week trend
    try:
        hist = os.path.join(os.path.dirname(A.INTEGRITY), "integrity_history.jsonl")
        with open(hist, "a", encoding="utf-8") as f:
            f.write(json.dumps({"date": time.strftime("%Y-%m-%d", time.gmtime()), "ts": now,
                                "linked_pct": round(linked, 2), "independent_pct": round(shared, 2),
                                "merchants": n_ok, "usdc": round(T, 2), "payers": len(payers)}) + "\n")
    except Exception as e:
        print("history append skipped:", e)
    tmp = A.INTEGRITY + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(out, f, indent=1)
    os.replace(tmp, A.INTEGRITY)
    print(out["headline"], "| merchants", n_ok, "| payers", len(payers), "| seconds", out["elapsed_seconds"])

if __name__ == "__main__":
    main()
