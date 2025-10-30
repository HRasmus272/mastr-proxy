// /api/mastr.js  — Node.js Serverless Function (CommonJS)
// Lädt MaStR-Daten paginiert. Standard: 1 Seite, 500 Zeilen -> schnell für Hobby-Timeouts.

const BASE =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetErweiterteOeffentlicheEinheitStromerzeugung";
const FILTER_META =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetFilterColumnsErweiterteOeffentlicheEinheitStromerzeugung";

const COLUMNS = [
  { key: "MaStRNummer", title: "MaStRNummer" },
  { key: "Anlagenbetreiber (Name)",  title: "Betreiber" },
  { key: "Energieträger",            title: "Energietraeger" },
  { key: "Bruttoleistung",           title: "Bruttoleistung" },
  { key: "Nettonennleistung",        title: "Nettonennleistung" },
  { key: "Bundesland",               title: "Bundesland" },
  { key: "Plz",                      title: "PLZ" },
  { key: "Ort",                      title: "Ort" },
  { key: "InbetriebnahmeDatum",      title: "InbetriebnahmeDatum" }
];

// ---- Helpers ----

function toCSV(rows) {
  const header = COLUMNS.map(c => c.title).join(",");
  const esc = (v) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const lines = rows.map(r => COLUMNS.map(c => esc(r[c.title])).join(","));
  return [header, ...lines].join("\n");
}

// Wandelt "YYYY-MM-DD" in ISO (nur Datum) — validiert grob
function toIsoOnlyDate(iso) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso || "");
  if (!m) return null;
  return `${m[1]}-${m[2]}-${m[3]}`;
}

// Baut MaStR-kompatible Datums-Filter-Teile:
// InbetriebnahmeDatum~ge~datetime'YYYY-MM-DDT00:00:00'
function buildDateFilter(field, startISO, endISO) {
  const s = toIsoOnlyDate(startISO);
  const e = toIsoOnlyDate(endISO);
  if (!s || !e) return null;
  // Enddatum exklusiv (lt)
  return `${field}~ge~datetime'${s}T00:00:00'~and~${field}~lt~datetime'${e}T00:00:00'`;
}

async function fetchJSON(url, signal) {
  const resp = await fetch(url, {
    headers: {
      "Accept": "application/json",
      "User-Agent": "mastr-proxy-vercel",
      "X-Requested-With": "XMLHttpRequest",
      "Referer": "https://www.marktstammdatenregister.de/"
    },
    signal
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new Error(`Upstream HTTP ${resp.status}: ${body?.slice(0, 300)}`);
  }
  return resp.json();
}

module.exports = async (req, res) => {
  // --- CORS & caching ---
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "no-store");

  try {
    const url = new URL(req.url, `https://${req.headers.host}`);
    const startISO = url.searchParams.get("start");
    const endISO   = url.searchParams.get("end");
    const carrierQ = (url.searchParams.get("carrier") || "Solare Strahlungsenergie").trim();
    const format   = (url.searchParams.get("format") || "csv").toLowerCase();
    const debug    = url.searchParams.get("debug") === "1";

    // Runtime-Schrauben gegen Timeout
    const pageSize = Math.min(parseInt(url.searchParams.get("pagesize") || "500", 10), 2000);
    const maxPages = Math.min(parseInt(url.searchParams.get("maxpages")  || "1",   10), 20);

    if (!startISO || !endISO) {
      res.status(400).send("Missing 'start' or 'end' (YYYY-MM-DD). Example: ?start=2024-01-01&end=2024-01-31&format=csv");
      return;
    }

    // --- NEU: Korrekte Datums-Syntax für MaStR ---
    const dateField = "InbetriebnahmeDatum";
    const dateExpr = buildDateFilter(dateField, startISO, endISO);
    if (!dateExpr) {
      res.status(400).send("Invalid date format. Use YYYY-MM-DD.");
      return;
    }

    // pro-Request Timeout (8s) – verhindert Hängenbleiben
    const ac = new AbortController();
    const to = setTimeout(() => ac.abort("timeout"), 8000);

    // 1) Energieträger-Code ermitteln (vereinfachter Fallback, kein Meta-Call)
let carrierCode = null;
if (/^\d+$/.test(carrierQ)) {
  // Wenn der Nutzer eine Zahl liefert (z. B. 2495), direkt verwenden
  carrierCode = String(carrierQ);
} else {
  // Sonst vorerst fixer Fallback für PV
  carrierCode = "2495"; // Solare Strahlungsenergie
}
    // 2) Filter zusammenbauen (Datum + Energieträger)
    const filterRaw = `${dateExpr}~and~Energieträger~eq~'${carrierCode}'`;

    // Debug: spätere Einsicht, was genau abgefragt wurde
    const upstreamFirstPage =
      `${BASE}?group=&sort=&aggregate=&page=1&pageSize=${pageSize}` +
      `&skip=0&take=${pageSize}&filter=${encodeURIComponent(filterRaw)}`;

    let page = 1;
    const rows = [];

    while (page <= maxPages) {
      const skip = (page - 1) * pageSize;
      const take = pageSize;

      const q =
        `${BASE}?group=&sort=&aggregate=` +
        `&page=${page}&pageSize=${pageSize}` +
        `&skip=${skip}&take=${take}` +
        `&filter=${encodeURIComponent(filterRaw)}`;

      const j = await fetchJSON(q, ac.signal);

      if (j && j.Error) {
        clearTimeout(to);
        res.status(502).send(`Upstream reported Error (Type=${j.Type || "?"}): ${j.Message || "no message"}`);
        return;
      }

      const data = Array.isArray(j) ? j : (j.Data || j.data || []);
      if (!Array.isArray(data) || data.length === 0) break;

      for (const rec of data) {
        const out = {};
        for (const col of COLUMNS) {
          if (col.key === "Inbetriebnahmedatum der Einheit") {
            out[col.title] =
              rec["Inbetriebnahmedatum der Einheit"] ??
              rec["InbetriebnahmeDatum"] ??
              rec["EegInbetriebnahmeDatum"] ??
              "";
          } else {
            out[col.title] = rec[col.key] ?? "";
          }
        }
        rows.push(out);
      }

      page++;
    }

    clearTimeout(to);

    // Debug-Ausgaben als Header (leicht einzusehen) + optional Body bei JSON
    if (debug) {
      res.setHeader("X-Debug-FilterRaw", filterRaw);
      res.setHeader("X-Debug-Upstream", upstreamFirstPage);
    }

    if (format === "json") {
      res.setHeader("Content-Type", "application/json; charset=utf-8");
      if (debug) {
        res.status(200).send(JSON.stringify({
          debug: true,
          filterRaw,
          upstreamUrl: upstreamFirstPage,
          rows
        }));
      } else {
        res.status(200).send(JSON.stringify(rows));
      }
    } else {
      const csv = toCSV(rows);
      res.setHeader("Content-Type", "text/csv; charset=utf-8");
      res.status(200).send(csv);
    }
  } catch (err) {
    const msg = (err && err.message) ? err.message : String(err);
    res.status(502).send(`Proxy error: ${msg}`);
  }
};
