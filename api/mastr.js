// /api/mastr.js — Node.js Serverless Function (CommonJS)
// MaStR-Proxy mit SERVERSEITIGEM Datumsfilter (Inbetriebnahmedatum der Einheit, gt/lt, dd.MM.yyyy)
// Features: Pagination, optionaler Status-Filter, CSV/JSON-Ausgabe, Timeout & Retries

// ---------------------- Konfiguration ----------------------
const BASE =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetErweiterteOeffentlicheEinheitStromerzeugung";
const FILTER_META =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetFilterColumnsErweiterteOeffentlicheEinheitStromerzeugung";

const PER_REQUEST_TIMEOUT_MS = parseInt(process.env.MASTR_TIMEOUT_MS || "20000", 10); // 20s
const RETRIES = parseInt(process.env.MASTR_RETRIES || "2", 10);
const BACKOFF_BASE_MS = 600; // 600ms, 1200ms

// Spaltenmapping für CSV/JSON-Normalisierung
const COLUMNS = [
  { key: "MaStRNummer",              title: "MaStR-Nr. der Einheit" },
  { key: "AnlagenbetreiberName",     title: "Betreiber" },
  { key: "EnergietraegerName",       title: "Energietraeger" },
  { key: "Bruttoleistung",           title: "Bruttoleistung" },
  { key: "Nettonennleistung",        title: "Nettonennleistung" },
  { key: "Bundesland",               title: "Bundesland" },
  { key: "Plz",                      title: "PLZ" },
  { key: "Ort",                      title: "Ort" },
  { key: "InbetriebnahmeDatum",      title: "Inbetriebnahme" }
];

// ---------------------- Helper ----------------------
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

// Parse YYYY-MM-DD → Date (UTC)
function parseISODate(iso) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso || "");
  if (!m) return null;
  const y = Number(m[1]), mo = Number(m[2]), d = Number(m[3]);
  return new Date(Date.UTC(y, mo - 1, d, 0, 0, 0, 0));
}

// Format dd.MM.yyyy (UTC)
function formatDDMMYYYYUTC(d) {
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const yyyy = d.getUTCFullYear();
  return `${dd}.${mm}.${yyyy}`;
}

// Baut die Kendo-Filterklausel für inklusives Intervall [start, end)
// -> gt auf Vortag von start, lt auf end
function buildMastrDateRange(startISO, endISO) {
  const start = parseISODate(startISO);
  const end = parseISODate(endISO);
  if (!start || !end) return null;

  const lower = new Date(start.getTime());
  lower.setUTCDate(lower.getUTCDate() - 1); // exklusiv Vortag
  const upper = end; // exklusiv end

  const lowerStr = formatDDMMYYYYUTC(lower);
  const upperStr = formatDDMMYYYYUTC(upper);

  return `Inbetriebnahmedatum der Einheit~gt~'${lowerStr}'~and~Inbetriebnahmedatum der Einheit~lt~'${upperStr}'`;
}

// Fetch mit eigenem Timeout
async function fetchWithTimeout(url, { signal, headers } = {}) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort("timeout"), PER_REQUEST_TIMEOUT_MS);

  if (signal) {
    signal.addEventListener("abort", () => {
      try { ac.abort(signal.reason || "parent_abort"); } catch {}
    }, { once: true });
  }

  try {
    const resp = await fetch(url, {
      headers: {
        "Accept": "application/json",
        "User-Agent": "mastr-proxy-vercel",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.marktstammdatenregister.de/",
        ...(headers || {})
      },
      signal: ac.signal
    });
    return resp;
  } finally {
    clearTimeout(timer);
  }
}

// Fetch JSON mit Retries bei 429/5xx/Timeout/Netzfehlern
async function fetchJSON(url, signal) {
  let attempt = 0;
  while (true) {
    try {
      const resp = await fetchWithTimeout(url, { signal });
      if (!resp.ok) {
        if ([429, 500, 502, 503, 504].includes(resp.status) && attempt < RETRIES) {
          const retryAfter = parseFloat(resp.headers.get("retry-after") || "0");
          const wait = retryAfter > 0 ? retryAfter * 1000 : BACKOFF_BASE_MS * Math.pow(2, attempt);
          await new Promise(r => setTimeout(r, wait));
          attempt++;
          continue;
        }
        const body = await resp.text().catch(() => "");
        throw new Error(`Upstream HTTP ${resp.status}: ${body?.slice(0, 300)}`);
      }
      return await resp.json();
    } catch (err) {
      const transient = (err && (err.name === "AbortError" || /timeout|ECONNRESET|ENOTFOUND|EAI_AGAIN/i.test(String(err))));
      if (transient && attempt < RETRIES) {
        const wait = BACKOFF_BASE_MS * Math.pow(2, attempt);
        await new Promise(r => setTimeout(r, wait));
        attempt++;
        continue;
      }
      throw err;
    }
  }
}

// Optional: nicht-numerischen Energieträger in Code auflösen (best effort)
async function tryResolveCarrierCode(carrierQ, signal) {
  if (/^\d+$/.test(carrierQ)) return String(carrierQ);
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
  } catch {/* ignore, fallback unten */}
  return "2495"; // Solare Strahlungsenergie
}

// ---------------------- Handler ----------------------
module.exports = async (req, res) => {
  // CORS & Cache
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "no-store");

  try {
    const url = new URL(req.url, `https://${req.headers.host}`);
    const startISO = url.searchParams.get("start");
    const endISO   = url.searchParams.get("end");
    const carrierQ = (url.searchParams.get("carrier") || "2495").trim(); // bevorzugt Code
    const statusQ  = (url.searchParams.get("status") || "").trim().toLowerCase(); // z.B. "35" oder "off"
    const format   = (url.searchParams.get("format") || "csv").toLowerCase();

    const pageSize = Math.min(parseInt(url.searchParams.get("pagesize") || "200", 10), 2000);
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

    const ac = new AbortController(); // optional: gesamter Request kann abgebrochen werden
    const carrierCode = await tryResolveCarrierCode(carrierQ, ac.signal);

    // Filter zusammenbauen
    const parts = [dateClause, `Energieträger~eq~'${carrierCode}'`];
    if (statusQ && statusQ !== "off") {
      const statusCode = /^\d+$/.test(statusQ) ? statusQ : "35"; // „In Betrieb“ als Fallback
      parts.push(`Betriebs-Status~eq~'${statusCode}'`);
    }
    const filterRaw = parts.join("~and~");
    const filterEncoded = encodeURIComponent(filterRaw);

    let page = 1;
    const rows = [];

    while (page <= maxPages) {
      const q =
        `${BASE}?group=&sort=&aggregate=` +
        `&forExport=true` +
        `&page=${page}&pageSize=${pageSize}` +
        `&filter=${filterEncoded}`;

      const j = await fetchJSON(q, ac.signal);
      if (j && j.Error) {
        res.status(502).send(`Upstream reported Error (Type=${j.Type || "?"}): ${j.Message || "no message"}`);
        return;
      }

      // Das Grid liefert meist { Items: [...] }
      const data =
        Array.isArray(j?.Items) ? j.Items :
        Array.isArray(j?.Data)  ? j.Data  :
        Array.isArray(j)        ? j       : [];

      if (!data.length) break;

      for (const rec of data) {
        const out = {};
        for (const col of COLUMNS) {
          if (col.key === "InbetriebnahmeDatum") {
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
