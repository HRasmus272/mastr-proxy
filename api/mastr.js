// /api/mastr.js  — Node.js Serverless Function (CommonJS), kurz laufend
// Lädt MaStR-Daten paginiert. Standard: 1 Seite, 500 Zeilen -> schnell für Hobby-Timeouts.

const BASE =
  https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetErweiterteOeffentlicheEinheitStromerzeugung;
const FILTER_META =
  https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetFilterColumnsErweiterteOeffentlicheEinheitStromerzeugung;

const COLUMNS = [
  { key: "MaStRNummer", title: "MaStRNummer" },
  { key: "Anlagenbetreiber (Name)",  title: "Betreiber" },
  { key: "Energieträger",            title: "Energietraeger" },
  { key: "Bruttoleistung",           title: "Bruttoleistung" },
  { key: "Nettonennleistung",        title: "Nettonennleistung" },
  { key: "Bundesland",               title: "Bundesland" },
  { key: "Plz",                      title: "PLZ" },
  { key: "Ort",                      title: "Ort" },
  { key: "InbetriebnahmeDatum", title: "InbetriebnahmeDatum" }
];

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

function toTicks(iso) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso || "");
  if (!m) return null;
  const [, y, mo, d] = m;
  const ms = Date.UTC(Number(y), Number(mo) - 1, Number(d), 0, 0, 0, 0);
  return `/Date(${ms})/`;
}

async function fetchJSON(url, signal) {
  const resp = await fetch(url, {
    headers: {
      "Accept": "application/json",
      "User-Agent": "mastr-proxy-vercel",
      "X-Requested-With": "XMLHttpRequest",
      "Referer": https://www.marktstammdatenregister.de/
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

    // Runtime-Schrauben gegen Timeout
    const pageSize = Math.min(parseInt(url.searchParams.get("pagesize") || "500", 10), 2000); // klein halten
    const maxPages = Math.min(parseInt(url.searchParams.get("maxpages") || "1", 10), 20);     // erst 1 Seite

    if (!startISO || !endISO) {
      res.status(400).send("Missing 'start' or 'end' (YYYY-MM-DD). Example: ?start=2024-01-01&end=2024-01-31&format=csv");
      return;
    }

    const startTicks = toTicks(startISO);
    const endTicks   = toTicks(endISO);
    if (!startTicks || !endTicks) {
      res.status(400).send("Invalid date format. Use YYYY-MM-DD.");
      return;
    }

    // pro-Request Timeout (8s) – verhindert Hängenbleiben
    const ac = new AbortController();
    const to = setTimeout(() => ac.abort("timeout"), 8000);

    // 1) Energieträger-Code ermitteln (robust, damit kein Crash bei Meta-Ausfall)
let carrierCode = null;

try {
  const meta = await fetchJSON(FILTER_META, ac.signal);
  const carrierFilter = Array.isArray(meta)
    ? meta.find(f => (f.FilterName || "").toLowerCase() === "energieträger")
    : null;

  if (carrierFilter && Array.isArray(carrierFilter.ListObject)) {
    if (/^\d+$/.test(carrierQ)) {
      const hit = carrierFilter.ListObject.find(x => String(x.Value) === carrierQ);
      if (hit) carrierCode = String(hit.Value);
    }
    if (!carrierCode) {
      const cq = carrierQ.toLowerCase();
      const exact  = carrierFilter.ListObject.find(x => (x.Name || "").toLowerCase() === cq);
      const starts = carrierFilter.ListObject.find(x => (x.Name || "").toLowerCase().startsWith(cq));
      const incl   = carrierFilter.ListObject.find(x => (x.Name || "").toLowerCase().includes(cq));
      const chosen = exact || starts || incl || null;
      if (chosen) carrierCode = String(chosen.Value);
    }
  }
} catch (e) {
  // Meta-Endpunkt down/fehlerhaft/Timeout -> still weiterarbeiten
  // (Crash vermeiden, unten greifen Fallbacks)
}

// Fallbacks, falls oben nichts ermittelt werden konnte:
if (!carrierCode) {
  if (/^\d+$/.test(carrierQ)) {
    carrierCode = String(carrierQ);
  } else {
    carrierCode = "2495"; // Solare Strahlungsenergie (Default)
  }
}

    // 2) Filter: InbetriebnahmeDatum + Energieträger
    const dateField = "InbetriebnahmeDatum";
    const filterRaw =
      `${dateField}~ge~'${startTicks}'` +
      `~and~${dateField}~lt~'${endTicks}'` +
      `~and~Energieträger~eq~'${carrierCode}'`;

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
