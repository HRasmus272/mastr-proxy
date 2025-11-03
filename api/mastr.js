// /api/mastr.js  — Node.js Serverless Function (CommonJS), kurz laufend
// Lädt MaStR-Daten paginiert, jetzt MIT serverseitigem Datumsfilter (Inbetriebnahmedatum der Einheit, gt/lt, dd.MM.yyyy).

const BASE =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetErweiterteOeffentlicheEinheitStromerzeugung";
const FILTER_META =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetFilterColumnsErweiterteOeffentlicheEinheitStromerzeugung";

const COLUMNS = [
  { key: "MaStR-Nummer der Einheit", title: "MaStRNummer" },
  { key: "Anlagenbetreiber (Name)",  title: "Betreiber" },
  { key: "Energieträger",            title: "Energietraeger" },
  { key: "Bruttoleistung",           title: "Bruttoleistung" },
  { key: "Nettonennleistung",        title: "Nettonennleistung" },
  { key: "Bundesland",               title: "Bundesland" },
  { key: "Postleitzahl",             title: "PLZ" },
  { key: "Ort",                      title: "Ort" },
  { key: "Inbetriebnahmedatum der Einheit", title: "Inbetriebnahme" }
];

// ---------- Helpers ----------
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

// Robust: parse YYYY-MM-DD (ohne Zeitzonen-Stress), gibt UTC-Date zurück
function parseISODate(iso) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso || "");
  if (!m) return null;
  const [, y, mo, d] = m.map(Number);
  // Nutze UTC, um TZ-Drift zu vermeiden
  return new Date(Date.UTC(y, mo - 1, d, 0, 0, 0, 0));
}

// dd.MM.yyyy aus UTC-Date
function formatDDMMYYYYUTC(d) {
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const yyyy = d.getUTCFullYear();
  return `${dd}.${mm}.${yyyy}`;
}

// Baut die Kendo-Filterklausel für ein inklusives Intervall [start, end)
// Wichtig: gt (strict) auf den VORTAG von start; lt (strict) auf end
function buildMastrDateRange(startISO, endISO) {
  const start = parseISODate(startISO);
  const end = parseISODate(endISO);
  if (!start || !end) return null;

  // untere exklusive Grenze = Tag vor start
  const lower = new Date(start.getTime());
  lower.setUTCDate(lower.getUTCDate() - 1);

  // obere exklusive Grenze = end (z. B. 1. des Folgemonats)
  const upper = end;

  const lowerStr = formatDDMMYYYYUTC(lower);
  const upperStr = formatDDMMYYYYUTC(upper);

  return `Inbetriebnahmedatum der Einheit~gt~'${lowerStr}'~and~Inbetriebnahmedatum der Einheit~lt~'${upperStr}'`;
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

// Versucht optional, einen nicht-numerischen carrier-Begriff in einen Code aufzulösen.
// Blockiert nie lange: kurzer Timeout, fällt im Zweifel auf 2495 zurück.
async function tryResolveCarrierCode(carrierQ, signal) {
  if (/^\d+$/.test(carrierQ)) return String(carrierQ); // bereits Code
  try {
    const meta = await fetchJSON(FILTER_META, signal);
    const carrierFilter = Array.isArray(meta)
      ? meta.find(f => (f.FilterName || "").toLowerCase() === "energieträger")
      : null;
    if (carrierFilter && Array.isArray(carrierFilter.ListObject)) {
      const cq = (carrierQ || "").toLowerCase();
      const exact  = carrierFilter.ListObject.find(x => (x.Name || "").toLowerCase() === cq);
      const starts = carrierFilter.ListObject.find(x => (x.Name || "").toLowerCase().startsWith(cq));
      const incl   = carrierFilter.ListObject.find(x => (x.Name || "").toLowerCase().includes(cq));
      const chosen = exact || starts || incl || null;
      if (chosen) return String(chosen.Value);
    }
  } catch {
    // still fallback
  }
  return "2495"; // Solare Strahlungsenergie
}

// ---------- Handler ----------
module.exports = async (req, res) => {
  // --- CORS & caching ---
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "no-store");

  try {
    const url = new URL(req.url, `https://${req.headers.host}`);
    const startISO = url.searchParams.get("start");
    const endISO   = url.searchParams.get("end");
    const carrierQ = (url.searchParams.get("carrier") || "2495").trim(); // bevorzugt Code
    const statusQ  = (url.searchParams.get("status") || "").trim().toLowerCase(); // z.B. "35" oder "off"
    const format   = (url.searchParams.get("format") || "csv").toLowerCase();

    const pageSize = Math.min(parseInt(url.searchParams.get("pagesize") || "500", 10), 2000);
    const maxPages = Math.min(parseInt(url.searchParams.get("maxpages") || "1", 10), 20);

    if (!startISO || !endISO) {
      res.status(400).send("Missing 'start' or 'end' (YYYY-MM-DD). Example: ?start=2024-01-01&end=2024-02-01&format=csv");
      return;
    }

    const dateClause = buildMastrDateRange(startISO, endISO);
    if (!dateClause) {
      res.status(400).send("Invalid date format. Use YYYY-MM-DD for 'start' and 'end'.");
      return;
    }

    // kurzer Timeout (8s) pro Upstream-Call
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort("timeout"), 8000);

    const carrierCode = await tryResolveCarrierCode(carrierQ, ac.signal);

    // Filter zusammenbauen
    const parts = [dateClause, `Energieträger~eq~'${carrierCode}'`];
    if (statusQ && statusQ !== "off") {
      // akzeptiere z.B. "35" oder "in betrieb" (nur Code ist sicher)
      const statusCode = /^\d+$/.test(statusQ) ? statusQ : "35";
      parts.push(`Betriebs-Status~eq~'${statusCode}'`);
    }
    const filterRaw = parts.join("~and~");

    let page = 1;
    const rows = [];

    while (page <= maxPages) {
      const q =
        `${BASE}?group=&sort=&aggregate=` +
        `&forExport=true` +          // wichtig: stabiler Code-Path
        `&page=${page}&pageSize=${pageSize}` +
        `&filter=${encodeURIComponent(filterRaw)}`;

      const j = await fetchJSON(q, ac.signal);

      if (j && j.Error) {
        clearTimeout(timer);
        res.status(502).send(`Upstream reported Error (Type=${j.Type || "?"}): ${j.Message || "no message"}`);
        return;
      }

      // Der Endpoint liefert typischerweise { Items: [...] } — fallback auf Data für Sicherheit
      const data = Array.isArray(j?.Items) ? j.Items
                 : Array.isArray(j?.Data)  ? j.Data
                 : Array.isArray(j)        ? j
                 : [];

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

    clearTimeout(timer);

    if (format === "json") {
      res.setHeader("Content-Type", "application/json; charset=utf-8");
      res.status(200).send(JSON.stringify(rows));
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
