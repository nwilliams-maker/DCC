from __future__ import annotations
import json, os, re
import pandas as pd

sheet=(os.environ.get("IC_SHEET_URL") or "").strip()
if not sheet:
    raise RuntimeError("IC_SHEET_URL missing")
url=f"{sheet.split('/edit')[0]}/export?format=csv&gid=934075207"
df=pd.read_csv(url)
df.columns=[str(c).strip().lower() for c in df.columns]
orange={"AK","AZ","CA","HI","ID","NV","OR","WA"}
rows=[]
for _,row in df.iterrows():
    raw=row.get("json payload")
    if pd.isna(raw) or not str(raw).strip():
        continue
    try:
        p=json.loads(str(raw))
    except Exception:
        continue
    state=str(p.get("state") or "").strip().upper()[:2]
    is_orange=state in orange
    if not is_orange:
        for stop in [x.strip() for x in str(p.get("locs") or "").split("|") if x.strip()]:
            m=re.search(r",\s*([A-Za-z]{2})\s+\d{5}(?:-\d{4})?\s*$",stop)
            if m and m.group(1).upper() in orange:
                is_orange=True
                break
    if not is_orange:
        continue
    dt=pd.to_datetime(row.get("date created"),utc=True,errors="coerce")
    rows.append({
        "wo":str(p.get("wo") or "").strip(),
        "contractor":str(row.get("contractor") or "").strip(),
        "state":state,
        "date_created":dt.isoformat() if pd.notna(dt) else str(row.get("date created") or "")
    })
rows=[r for r in rows if r["wo"]]
rows.sort(key=lambda x:x["date_created"],reverse=True)
print("ORANGE_ACCEPTED_SHEET="+json.dumps(rows[:10],separators=(",",":")),flush=True)
