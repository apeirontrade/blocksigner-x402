"""Loads every paid route's browser pay page in real Chromium and runs the exact
encode step the page performs after a wallet signs. Reports which routes would fail."""
import json,sys,urllib.request
from playwright.sync_api import sync_playwright
BASE="https://blocksigner.org"
routes=list(json.load(urllib.request.urlopen(BASE+"/x402.json",timeout=20))["routes"])
JS_OLD="(Y)=>{try{btoa(JSON.stringify(Y));return 'ok'}catch(e){return 'FAIL: '+e.message}}"
JS_NEW="(Y)=>{try{const s=btoa(unescape(encodeURIComponent(JSON.stringify(Y))));const back=decodeURIComponent(escape(atob(s)));return back===JSON.stringify(Y)?'ok':'FAIL: roundtrip mismatch'}catch(e){return 'FAIL: '+e.message}}"
bad=0
with sync_playwright() as p:
    b=p.chromium.launch(); ctx=b.new_context(extra_http_headers={"Accept":"text/html"})
    pg=ctx.new_page()
    print(f"{'route':24} {'pay page':9} {'served fix':10} {'encode as served':34} encode w/ fix")
    for r in routes:
        try:
            resp=pg.goto(BASE+r,timeout=45000,wait_until="domcontentloaded")
            html=pg.content()
            cfg=pg.evaluate("()=>window.x402||null")
            if not cfg: print(f"{r:24} {resp.status:<9} no window.x402 found"); bad+=1; continue
            pr=cfg["paymentRequired"]
            Y={"x402Version":pr.get("x402Version",2),"accepted":pr["accepts"][0],"resource":pr.get("resource"),"extensions":pr.get("extensions"),"payload":{"paymentGroup":["AAAA"],"paymentIndex":0}}
            patched="encodeURIComponent(JSON.stringify(Y))" in html
            served=pg.evaluate(JS_NEW if patched else JS_OLD,Y)
            fixed=pg.evaluate(JS_NEW,Y)
            if served!="ok": bad+=1
            print(f"{r:24} {resp.status:<9} {str(patched):10} {served[:34]:34} {fixed}")
        except Exception as e:
            bad+=1; print(f"{r:24} ERROR {str(e)[:90]}")
    b.close()
print(f"\n{bad} of {len(routes)} routes would fail for a human paying in a browser")
sys.exit(1 if bad else 0)
