from fastapi import FastAPI, HTTPException, Header
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List, Dict
from pathlib import Path
from datetime import datetime
import requests, os, io, re
import pandas as pd
import pdfplumber
import base64

BASE_URL = os.getenv("PUBLIC_BASE_URL", "https://example.com")
API_KEY = os.getenv("API_KEY", "")
OUTDIR = Path(os.getenv("OUTDIR", "out"))
OUTDIR.mkdir(parents=True, exist_ok=True)

FIXED_ROWS = [
    "Revenue | Product","Revenue | Services","Revenue | Other Operating Revenue",
    "COGS | Materials","COGS | Direct Labor","COGS | Purchased Services","COGS | Other COGS",
    "Gross Profit (derived)",
    "OpEx | Sales & Marketing | People","OpEx | Sales & Marketing | Media/Ads","OpEx | Sales & Marketing | Agencies","OpEx | Sales & Marketing | Events","OpEx | Sales & Marketing | Software/Tools",
    "OpEx | General & Administrative | People","OpEx | General & Administrative | Facilities","OpEx | General & Administrative | Professional Services","OpEx | General & Administrative | IT/Software","OpEx | General & Administrative | Travel","OpEx | General & Administrative | Other G&A",
    "OpEx | Research & Development | People","OpEx | Research & Development | Contractors","OpEx | Research & Development | Cloud/Compute","OpEx | Research & Development | Labs/Prototyping","OpEx | Research & Development | Other R&D",
    "Depreciation","Amortization",
    "Other Operating | Grants/Subsidies","Other Operating | Gains/Losses on Disposals","Other Operating | Restructuring",
    "Operating Profit (EBIT) (derived)",
    "Below Operating | Net Interest","Below Operating | FX Gains/Losses","Below Operating | Associates/JVs",
    "Tax | Current","Tax | Deferred",
    "Net Income (derived)"
]

LABEL_MAP = [
    (r"\b(advertis|marketing|media|pub(licit[eé])?)\b", "OpEx | Sales & Marketing | Media/Ads"),
    (r"\b(g[& ]*a|frais\s*g[ée]n[ée]raux|administrative)\b", "OpEx | General & Administrative | People"),
    (r"\b(r&d|recherche|entwicklung|research)\b", "OpEx | Research & Development | People"),
    (r"\b(cost of sales|co[uû]t des ventes|cogs)\b", "COGS | Other COGS"),
    (r"\b(materials|mati[eè]res)\b", "COGS | Materials"),
    (r"\b(direct labor|main d'?oeuvre|personnel direct)\b", "COGS | Direct Labor"),
    (r"\b(services? achet[ée]s|purchased services)\b", "COGS | Purchased Services"),
    (r"\b(revenue|chiffre d'affaires|sales)\b", "Revenue | Product"),
    (r"\b(depreciation|amortissement technique)\b", "Depreciation"),
    (r"\b(amortization|amortissement incorporel)\b", "Amortization"),
    (r"\b(taxes?|imp[oô]ts?)\b", "Tax | Current")
]

class FileRef(BaseModel):
    name: str
    contentUrl: Optional[str] = None
    contentType: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    dataUri: Optional[str] = None

class BuildReq(BaseModel):
    files: List[FileRef]
    baseCurrency: Optional[str] = None

app = FastAPI()

def try_number(x: str):
    if x is None: return None
    s = str(x).strip().replace(" ", "").replace(",", "").replace("\u00A0","")
    neg = s.startswith("(") and s.endswith(")")
    s = s.replace("(", "").replace(")", "")
    if not re.match(r"^-?\d+(\.\d+)?$", s): return None
    v = float(s)
    return -v if neg else v

def normalize_label(label: str):
    if not label: return None
    low = label.lower()
    for pattern, fixed in LABEL_MAP:
        if re.search(pattern, low): return fixed
    return None

def extract_from_excel(content: bytes, fname: str, base_currency: Optional[str]):
    rows = []
    xls = pd.ExcelFile(io.BytesIO(content))
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet)
        cols = [str(c).strip().lower() for c in df.columns]
        def col_like(keys):
            for k in keys:
                for i,c in enumerate(cols):
                    if k in c: return df.columns[i]
            return None
        c_label = col_like(["label","item","intitul","description","account"])
        c_amt   = col_like(["amount","montant","value","valeur","total"])
        c_year  = col_like(["year","exercice","fy","fiscal"])
        c_ccy   = col_like(["curr","devise","currency"])
        for _, r in df.iterrows():
            label = str(r.get(c_label, "") or "")
            fixed = normalize_label(label)
            if not fixed: continue
            amt = try_number(r.get(c_amt, None))
            rows.append({
                "company_name": Path(fname).stem,
                "fiscal_year": str(r.get(c_year, "")) if c_year else "",
                "reporting_currency": str(r.get(c_ccy, "")) if c_ccy else "",
                "base_currency": (base_currency if base_currency and base_currency!="NONE" else (str(r.get(c_ccy, "")) if c_ccy else "")),
                "line_item": fixed,
                "amount": amt
            })
    return rows

def extract_from_pdf(content: bytes, fname: str, base_currency: Optional[str]):
    rows = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        text = "\n".join([(p.extract_text() or "") for p in pdf.pages])
        rep_ccy = "USD" if re.search(r"\bUSD|\$|US\$|dollars?\b", text, re.I) else ("EUR" if re.search(r"\bEUR|€\b", text, re.I) else ("GBP" if re.search(r"\bGBP|£\b", text, re.I) else ""))
        for line in text.splitlines():
            line = line.strip()
            if not line: continue
            m = re.search(r"(.*?)[\s:\-]+([\(\)\-\d\s,\.]+)$", line)
            if not m: continue
            label_raw, num_raw = m.group(1), m.group(2)
            fixed = normalize_label(label_raw)
            if not fixed: continue
            amt = try_number(num_raw)
            if amt is None: continue
            rows.append({
                "company_name": Path(fname).stem,
                "fiscal_year": "",
                "reporting_currency": rep_ccy,
                "base_currency": (base_currency if base_currency and base_currency!="NONE" else rep_ccy),
                "line_item": fixed,
                "amount": amt
            })
    return rows

def write_workbook(rows: List[dict], out_path: Path):
    by_co = {}
    for r in rows:
        by_co.setdefault(r["company_name"] or "Company", []).append(r)
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        if not by_co:
            pd.DataFrame([{ "company_name":"","fiscal_year":"","reporting_currency":"","base_currency":"",
                            "line_item":line,"amount":"","percent_of_revenue":"" } for line in FIXED_ROWS])\
              .to_excel(xw, sheet_name="Company_Template", index=False)
            return
        for company, items in by_co.items():
            total_rev = sum(float(i.get("amount") or 0) for i in items if str(i["line_item"]).startswith("Revenue"))
            lookup = {i["line_item"]: i for i in items}
            meta = items[0]
            out = []
            for line in FIXED_ROWS:
                amt = lookup.get(line, {}).get("amount", None)
                perc = (amt/total_rev) if (total_rev and isinstance(amt,(int,float))) else ""
                out.append({
                    "company_name": meta.get("company_name", company),
                    "fiscal_year": meta.get("fiscal_year",""),
                    "reporting_currency": meta.get("reporting_currency",""),
                    "base_currency": meta.get("base_currency") or meta.get("reporting_currency",""),
                    "line_item": line,
                    "amount": amt if isinstance(amt,(int,float)) else "",
                    "percent_of_revenue": perc
                })
            df = pd.DataFrame(out)
            safe = re.sub(r'[\\/?*\[\]:]', '_', company)[:31] or "Company"
            df.to_excel(xw, sheet_name=safe, index=False)

def _download_file(f: FileRef) -> bytes:
    if f.dataUri:
        b64 = f.dataUri.split(",", 1)[1] if "," in f.dataUri else f.dataUri
        return base64.b64decode(b64)
    if f.contentUrl:
        r = requests.get(f.contentUrl, headers=f.headers or {})
        r.raise_for_status()
        return r.content
    raise HTTPException(400, f"no content for {f.name}")

@app.post("/buildPerCompanyWorkbook")
def build(req: BuildReq, x_api_key: Optional[str] = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "unauthorized")
    if not req.files:
        raise HTTPException(400, "files[] required")

    all_rows: List[dict] = []
    for f in req.files:
        b = _download_file(f)
        name = (f.name or "").lower()
        if name.endswith((".xlsx", ".xls")) or (f.contentType or "").endswith("sheet"):
            all_rows.extend(extract_from_excel(b, f.name or "file.xlsx", req.baseCurrency))
        elif name.endswith(".pdf") or (f.contentType or "").endswith("pdf"):
            all_rows.extend(extract_from_pdf(b, f.name or "file.pdf", req.baseCurrency))
        else:
            continue

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_name = f"CostBench_PerCompany_{ts}.xlsx"
    out_path = OUTDIR / out_name
    write_workbook(all_rows, out_path)
    return {"fileUrl": f"{BASE_URL}/out/{out_name}", "fileName": "CostBench_PerCompany.xlsx"}

app.mount("/out", StaticFiles(directory=str(OUTDIR)), name="out")

@app.get("/health")
def health(): return {"ok": True}
