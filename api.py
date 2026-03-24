#!/usr/bin/env python3
"""
BPW Running Gear Knowledge Graph — Read-Only API
Schlanke REST-API für den Silver Lake Wissensgraph.

Endpoints:
  GET /                          → API-Info + Docs-Link
  GET /api/stats                 → Graph-Statistiken
  GET /api/bauteile?q=...        → Bauteile suchen (TN, Name, Typ)
  GET /api/cross-references/{tn} → Kompatible Teile für eine Teilenummer
  GET /api/baugruppen            → Alle Baugruppen (Hierarchie)
  GET /api/baugruppen/{id}       → Bauteile einer Baugruppe
  GET /api/zulieferer            → Zulieferer-Liste
  GET /api/mitbewerber           → Mitbewerber-Liste
  GET /api/normen                → Normen und Standards
  GET /api/trailer               → Trailer-Modelle
  GET /api/reklamationen         → Reklamationen / Feldeinsätze
  GET /api/dokumente/{tn}        → Dokumente zu einer Teilenummer
"""

import json, gzip, re, os
from pathlib import Path
from collections import defaultdict
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

# ── Graph laden ──────────────────────────────────────────────────────────────

GRAPH_GZ = Path(__file__).parent / "graph.json.gz"
GRAPH_JSON = os.environ.get("GRAPH_PATH", "")

if GRAPH_JSON and Path(GRAPH_JSON).exists():
    print(f"Lade Graph (JSON): {GRAPH_JSON}")
    with open(GRAPH_JSON, "r", encoding="utf-8") as f:
        G = json.load(f)
elif GRAPH_GZ.exists():
    print(f"Lade Graph (GZ): {GRAPH_GZ}")
    with gzip.open(GRAPH_GZ, "rt", encoding="utf-8") as f:
        G = json.load(f)
else:
    raise FileNotFoundError("Kein Graph gefunden! Setze GRAPH_PATH oder lege graph.json.gz ab.")

TOTAL_N = sum(len(v) for v in G["nodes"].values())
TOTAL_E = sum(len(v) for v in G["edges"].values())
print(f"Graph geladen: {TOTAL_N} Knoten, {TOTAL_E} Kanten, Version {G.get('version', '?')}")


# ── Indizes ──────────────────────────────────────────────────────────────────

def normalize(pn: str) -> str:
    return re.sub(r'[\s\-\.\,]', '', str(pn)).upper()

# TN → Bauteil
TN_INDEX = {}
for bt in G["nodes"].get("Bauteil", []):
    tn = bt.get("teilenummer", "")
    if tn:
        TN_INDEX[normalize(tn)] = bt

# ID → Node (alle Typen)
ID_INDEX = {}
for typ, nodes in G["nodes"].items():
    for n in nodes:
        ID_INDEX[n["id"]] = n

# Kanten-Indizes
EDGES_BY_VON = defaultdict(list)
EDGES_BY_NACH = defaultdict(list)
for edge_typ, edges in G["edges"].items():
    for e in edges:
        e_with_type = {**e, "_edge_typ": edge_typ}
        EDGES_BY_VON[e.get("von", "")].append(e_with_type)
        EDGES_BY_NACH[e.get("nach", "")].append(e_with_type)

print(f"Indizes: {len(TN_INDEX)} TN, {len(ID_INDEX)} Knoten")


# ── FastAPI ──────────────────────────────────────────────────────────────────

app = FastAPI(
    title="BPW Running Gear Knowledge Graph API",
    description="Read-only API für den EXPLA Silver Lake Wissensgraph (Running Gear / Fahrwerk)",
    version=G.get("version", "unknown"),
    docs_url="/docs",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── Abfrage-Funktionen ──────────────────────────────────────────────────────

def search_bauteile(query: str, hersteller: str = None, baugruppe: str = None, limit: int = 50):
    q = query.lower()
    q_norm = normalize(query)
    results = []

    # Exakter TN-Match
    if q_norm in TN_INDEX:
        results.append(TN_INDEX[q_norm])

    for bt in G["nodes"].get("Bauteil", []):
        if len(results) >= limit:
            break
        if bt in results:
            continue
        if hersteller and hersteller.lower() not in str(bt.get("hersteller", "")).lower():
            continue
        if baugruppe and bt.get("baugruppe_id") != baugruppe:
            continue
        name = str(bt.get("name", "")).lower()
        tn = str(bt.get("teilenummer", "")).lower()
        typ = str(bt.get("bauteil_typ", "")).lower()
        bg_id = str(bt.get("baugruppe_id", "")).lower()
        if q in name or q in tn or q in typ or q in bg_id or q_norm == normalize(bt.get("teilenummer", "")):
            results.append(bt)

    return [{"id": b["id"], "name": b.get("name", "Unbekannt"),
             "teilenummer": b.get("teilenummer", "—"),
             "hersteller": b.get("hersteller", "Unbekannt"),
             "bauteil_typ": b.get("bauteil_typ"),
             "baugruppe_id": b.get("baugruppe_id"),
             "beschreibung": b.get("beschreibung_einfach", ""),
             "konfidenz": b.get("konfidenz")} for b in results[:limit]]


def get_cross_references(teilenummer: str, hersteller_filter: str = None):
    tn_norm = normalize(teilenummer)
    bt = TN_INDEX.get(tn_norm)
    if not bt:
        return {"error": f"Teilenummer {teilenummer} nicht gefunden", "results": []}

    results = []
    for e in EDGES_BY_VON.get(bt["id"], []):
        if e["_edge_typ"] == "M05_kompatibel_mit":
            other = ID_INDEX.get(e["nach"])
            if other and (not hersteller_filter or hersteller_filter.lower() in str(other.get("hersteller", "")).lower()):
                results.append({"id": other["id"], "teilenummer": other.get("teilenummer"),
                                "hersteller": other.get("hersteller"), "name": other.get("name"),
                                "konfidenz": e.get("konfidenz"), "quelle": e.get("quelle")})

    for e in EDGES_BY_NACH.get(bt["id"], []):
        if e["_edge_typ"] == "M05_kompatibel_mit":
            other = ID_INDEX.get(e["von"])
            if other and other["id"] != bt["id"] and (not hersteller_filter or hersteller_filter.lower() in str(other.get("hersteller", "")).lower()):
                results.append({"id": other["id"], "teilenummer": other.get("teilenummer"),
                                "hersteller": other.get("hersteller"), "name": other.get("name"),
                                "konfidenz": e.get("konfidenz"), "quelle": e.get("quelle")})

    return {"bauteil": {"id": bt["id"], "teilenummer": bt.get("teilenummer"),
                        "hersteller": bt.get("hersteller"), "name": bt.get("name")},
            "cross_references": results, "count": len(results)}


def get_zulieferer(kategorie: str = None):
    results = []
    for org in G["nodes"].get("Organisation", []):
        tier = org.get("tier", "")
        if tier in ("T1", "T2"):
            rolle = org.get("rolle", [])
            if isinstance(rolle, str):
                rolle = [rolle]
            if kategorie and not any(kategorie.lower() in r.lower() for r in rolle):
                continue
            results.append({"id": org["id"], "name": org.get("name"), "sitz": org.get("sitz"),
                            "tier": tier, "rolle": rolle})
    return results


def get_mitbewerber():
    results = []
    for org in G["nodes"].get("Organisation", []):
        tier = org.get("tier", "")
        rolle = str(org.get("rolle", ""))
        if tier == "Mitbewerber" or "Mitbewerber" in rolle:
            results.append({"id": org["id"], "name": org.get("name"), "sitz": org.get("sitz"),
                            "tier": tier, "rolle": org.get("rolle")})
    return results


def get_baugruppen():
    baugruppen = []
    for bg in G["nodes"].get("Baugruppe", []):
        count = sum(1 for bt in G["nodes"].get("Bauteil", []) if bt.get("baugruppe_id") == bg["id"])
        baugruppen.append({
            "id": bg.get("id"), "name": bg.get("name"),
            "beschreibung": bg.get("beschreibung_einfach", ""),
            "oberbaugruppe_id": bg.get("oberbaugruppe_id"),
            "anzahl_bauteile": count,
        })
    return baugruppen


def get_baugruppe_bauteile(baugruppe_id: str, limit: int = 100):
    results = []
    for bt in G["nodes"].get("Bauteil", []):
        if bt.get("baugruppe_id") == baugruppe_id:
            results.append({"id": bt["id"], "teilenummer": bt.get("teilenummer"),
                            "hersteller": bt.get("hersteller"), "name": bt.get("name"),
                            "bauteil_typ": bt.get("bauteil_typ")})
    return {"baugruppe_id": baugruppe_id, "bauteile": results[:limit],
            "count": len(results), "truncated": len(results) > limit}


def get_normen(thema: str = None):
    results = []
    for norm in G["nodes"].get("Norm", []):
        if thema:
            text = " ".join([str(norm.get("name", "")), str(norm.get("beschreibung_fachlich", "")),
                             str(norm.get("beschreibung_einfach", "")), str(norm.get("id", ""))])
            if thema.lower() not in text.lower():
                continue
        results.append({"id": norm["id"], "name": norm.get("name"),
                        "beschreibung": norm.get("beschreibung_einfach", norm.get("beschreibung_fachlich", ""))})
    return results


def get_trailer_modelle(achshersteller: str = None):
    results = []
    for tm in G["nodes"].get("TrailerModell", []):
        if achshersteller:
            text = str(tm.get("beschreibung_fachlich", ""))
            has_match = achshersteller.lower() in text.lower()
            if not has_match:
                for e in EDGES_BY_NACH.get(tm["id"], []):
                    if e["_edge_typ"] == "E01_verbaut_in":
                        bt = ID_INDEX.get(e["von"])
                        if bt and achshersteller.lower() in str(bt.get("hersteller", "")).lower():
                            has_match = True
                            break
            if not has_match:
                continue
        results.append({"id": tm["id"], "name": tm.get("name"),
                        "beschreibung": tm.get("beschreibung_einfach", "")})
    return results


def get_reklamationen(hersteller: str = None):
    results = []
    for rek in G["nodes"].get("Reklamation", []):
        if hersteller:
            if hersteller.lower() not in str(rek.get("betroffene_hersteller", "")).lower() and \
               hersteller.lower() not in str(rek.get("beschreibung_einfach", "")).lower():
                continue
        results.append({"id": rek["id"], "name": rek.get("name"),
                        "beschreibung": rek.get("beschreibung_einfach"),
                        "hersteller": rek.get("betroffene_hersteller"),
                        "fahrzeuge": rek.get("fahrzeuge_betroffen"),
                        "jahr": rek.get("jahr")})
    return results


def get_dokumente_fuer_bauteil(teilenummer: str):
    tn_norm = normalize(teilenummer)
    bt = TN_INDEX.get(tn_norm)
    if not bt:
        return {"error": f"Teilenummer {teilenummer} nicht gefunden", "results": []}

    docs = []
    for e in EDGES_BY_VON.get(bt["id"], []):
        if e["_edge_typ"] == "E12_dokumentiert_in":
            dok = ID_INDEX.get(e["nach"])
            if dok:
                docs.append({"id": dok["id"], "name": dok.get("name"),
                             "seite": e.get("seite"), "hersteller": dok.get("hersteller")})
    return {"bauteil": bt.get("teilenummer"), "dokumente": docs, "count": len(docs)}


def search_patente(query: str = None, anmelder: str = None, ipc_klasse: str = None, limit: int = 50):
    """Sucht Patente nach Titel, Anmelder, IPC-Klasse oder Doc-ID."""
    results = []
    q = (query or "").lower()

    for pat in G["nodes"].get("Patent", []):
        if len(results) >= limit:
            break
        if anmelder:
            pat_orgs = []
            for e in EDGES_BY_VON.get(pat["id"], []):
                if e["_edge_typ"] == "P01_angemeldet_von":
                    org = ID_INDEX.get(e["nach"])
                    if org:
                        pat_orgs.append(org.get("name", ""))
            if not any(anmelder.lower() in o.lower() for o in pat_orgs):
                continue
        if ipc_klasse:
            if not any(ipc_klasse.upper() in ipc.upper() for ipc in pat.get("ipc_classes", [])):
                continue
        if q:
            text = " ".join([str(pat.get("titel", "")), str(pat.get("titel_en", "")),
                             str(pat.get("abstract", "")), str(pat.get("doc_id", ""))]).lower()
            if q not in text:
                continue

        anmelder_namen = []
        for e in EDGES_BY_VON.get(pat["id"], []):
            if e["_edge_typ"] == "P01_angemeldet_von":
                org = ID_INDEX.get(e["nach"])
                if org:
                    anmelder_namen.append(org.get("name", ""))

        results.append({
            "id": pat["id"], "doc_id": pat.get("doc_id"), "titel": pat.get("titel", ""),
            "anmelder": anmelder_namen, "ipc_classes": pat.get("ipc_classes", []),
            "filing_date": pat.get("filing_date", ""), "publication_date": pat.get("publication_date", ""),
            "abstract": (pat.get("abstract", "") or "")[:200],
        })

    return {"patente": results, "count": len(results), "total_im_graph": len(G["nodes"].get("Patent", []))}


def get_patent_statistik(anmelder: str = None):
    """Patent-Statistiken: Top-Anmelder, IPC-Verteilung, Patente pro Jahr."""
    from collections import Counter
    anmelder_count = Counter()
    ipc_count = Counter()
    year_count = Counter()

    for pat in G["nodes"].get("Patent", []):
        for e in EDGES_BY_VON.get(pat["id"], []):
            if e["_edge_typ"] == "P01_angemeldet_von":
                org = ID_INDEX.get(e["nach"])
                if org:
                    anmelder_count[org.get("name", "?")] += 1
        for ipc in pat.get("ipc_classes", []):
            ipc_count[ipc.strip()[:10].strip()] += 1
        pub = pat.get("publication_date", "")
        if len(pub) >= 4:
            year_count[pub[:4]] += 1

    return {
        "total_patente": len(G["nodes"].get("Patent", [])),
        "top_anmelder": [{"name": n, "count": c} for n, c in anmelder_count.most_common(20)],
        "top_ipc_klassen": [{"ipc": i, "count": c} for i, c in ipc_count.most_common(10)],
        "nach_jahr": [{"jahr": y, "count": c} for y, c in sorted(year_count.items())],
    }


def get_graph_stats():
    return {
        "name": "BPW Running Gear Knowledge Graph",
        "version": G.get("version"),
        "knoten_gesamt": TOTAL_N,
        "kanten_gesamt": TOTAL_E,
        "knoten_nach_typ": {k: len(v) for k, v in G["nodes"].items()},
        "kanten_nach_typ": {k: len(v) for k, v in G["edges"].items()},
        "teilenummern_indexiert": len(TN_INDEX),
    }


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/", summary="API-Info")
def root():
    return {
        "name": "BPW Running Gear Knowledge Graph API",
        "version": G.get("version"),
        "graph": f"{TOTAL_N} Knoten, {TOTAL_E} Kanten",
        "docs": "/docs",
        "endpoints": [
            "GET /api/stats",
            "GET /api/bauteile?q=Bremsscheibe",
            "GET /api/cross-references/{teilenummer}",
            "GET /api/baugruppen",
            "GET /api/baugruppen/{id}",
            "GET /api/zulieferer",
            "GET /api/mitbewerber",
            "GET /api/normen",
            "GET /api/trailer",
            "GET /api/reklamationen",
            "GET /api/dokumente/{teilenummer}",
            "GET /api/patente?q=Achse&anmelder=BPW",
            "GET /api/patente/statistik",
        ]
    }

@app.get("/api/stats", summary="Graph-Statistiken")
def api_stats():
    return get_graph_stats()

@app.get("/api/bauteile", summary="Bauteile suchen")
def api_bauteile(
    q: str = Query(..., description="Suchbegriff: Teilenummer, Name oder Typ"),
    hersteller: str = Query(None, description="Hersteller-Filter (z.B. 'BPW', 'SAF')"),
    baugruppe: str = Query(None, description="Baugruppen-ID (z.B. 'BG_BREMSSCHEIBE')"),
    limit: int = Query(50, ge=1, le=500, description="Max. Ergebnisse"),
):
    return search_bauteile(q, hersteller, baugruppe, limit)

@app.get("/api/cross-references/{teilenummer}", summary="Cross-References für Teilenummer")
def api_crossref(
    teilenummer: str,
    hersteller: str = Query(None, description="Filter nach Hersteller"),
):
    return get_cross_references(teilenummer, hersteller)

@app.get("/api/baugruppen", summary="Alle Baugruppen")
def api_baugruppen():
    return get_baugruppen()

@app.get("/api/baugruppen/{baugruppe_id}", summary="Bauteile einer Baugruppe")
def api_baugruppe_detail(
    baugruppe_id: str,
    limit: int = Query(100, ge=1, le=1000),
):
    return get_baugruppe_bauteile(baugruppe_id, limit)

@app.get("/api/zulieferer", summary="Zulieferer-Liste")
def api_zulieferer(kategorie: str = Query(None)):
    return get_zulieferer(kategorie)

@app.get("/api/mitbewerber", summary="Mitbewerber-Liste")
def api_mitbewerber():
    return get_mitbewerber()

@app.get("/api/normen", summary="Normen und Standards")
def api_normen(thema: str = Query(None, description="Filter nach Thema (z.B. 'Bremse')")):
    return get_normen(thema)

@app.get("/api/trailer", summary="Trailer-Modelle")
def api_trailer(achshersteller: str = Query(None, description="Filter nach Achshersteller")):
    return get_trailer_modelle(achshersteller)

@app.get("/api/reklamationen", summary="Reklamationen")
def api_reklamationen(hersteller: str = Query(None)):
    return get_reklamationen(hersteller)

@app.get("/api/dokumente/{teilenummer}", summary="Dokumente zu Teilenummer")
def api_dokumente(teilenummer: str):
    return get_dokumente_fuer_bauteil(teilenummer)

@app.get("/api/patente", summary="Patente suchen")
def api_patente(
    q: str = Query(None, description="Suchbegriff (Titel, Abstract, Doc-ID)"),
    anmelder: str = Query(None, description="Anmelder-Filter (z.B. 'BPW', 'SAF')"),
    ipc: str = Query(None, description="IPC-Klasse (z.B. 'B60B35')"),
    limit: int = Query(50, ge=1, le=500),
):
    return search_patente(q, anmelder, ipc, limit)

@app.get("/api/patente/statistik", summary="Patent-Statistiken")
def api_patent_stats(anmelder: str = Query(None, description="Filter nach Anmelder")):
    return get_patent_statistik(anmelder)


# ── Health Check ─────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "nodes": TOTAL_N, "edges": TOTAL_E}


# ── Start ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print(f"\n🚀 BPW Graph API → http://0.0.0.0:{port}")
    print(f"   Docs: http://0.0.0.0:{port}/docs\n")
    uvicorn.run(app, host="0.0.0.0", port=port)
