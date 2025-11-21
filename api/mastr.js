// /api/mastr.js — Node.js Serverless Function (CommonJS)
// MaStR-Proxy mit SERVERSEITIGEM Datumsfilter (Inbetriebnahmedatum der Einheit, gt/lt, dd.MM.yyyy)
// Features: Pagination, optionaler Status-Filter, CSV/JSON-Ausgabe, Timeout & Retries
// + Debug-Modi: &debug=keys | &debug=sample | &debug=fields
// + SERVERSEITIGE PARALLELISIERUNG über Datumsintervalle (&chunkdays, &maxconcurrent)

// ---------------------- Konfiguration ----------------------
const BASE =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetErweiterteOeffentlicheEinheitStromerzeugung";
const FILTER_META =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetFilterColumnsErweiterteOeffentlicheEinheitStromerzeugung";

const PER_REQUEST_TIMEOUT_MS = parseInt(process.env.MASTR_TIMEOUT_MS || "20000", 10); // 20s
const RETRIES = parseInt(process.env.MASTR_RETRIES || "2", 10);
const BACKOFF_BASE_MS = 600; // 600ms, 1200ms

// ---------------------- Spaltenmapping ----------------------
const COLUMNS = [
  { key: "MaStRNummer",                        title: "MaStR-Nr. der Einheit" },
  { key: "EinheitName",                        title: "Anzeige-Name der Einheit" },
  { key: "BetriebsStatusName",                 title: "Betriebs-Status" },
  { key: "EnergietraegerName",                 title: "Energietraeger" },
  { key: "Bruttoleistung",                     title: "Bruttoleistung" },
  { key: "Nettonennleistung",                  title: "Nettonennleistung" },
  { key: "InbetriebnahmeDatum",                title: "Inbetriebnahme" },
  { key: "InbetriebnahmeDatumAmAktuellenOrt",  title: "Inbetriebnahmedatum der Einheit am aktuellen Standort" },
  { key: "EinheitRegistrierungsdatum",         title: "Registrierungsdatum der Einheit" },
  { key: "Bundesland",                         title: "Bundesland" },
  { key: "Plz",                                title: "PLZ" },
  { key: "Ort",                                title: "Ort" },
  { key: "Strasse",                            title: "Straße" },
  { key: "Hausnummer",                         title: "Hausnummer" },
  { key: "Gemarkung",                          title: "Gemarkung" },
  { key: "Flurstueck",                         title: "Flurstück" },
  { key: "Gemeindeschluessel",                 title: "Gemeindeschluessel" },
  { key: "Gemeinde",                           title: "Gemeinde" },
  { key: "Landkreis",                          title: "Landkreis" },
  { key: "Breitengrad",                        title: "Koordinate: Breitengrad (WGS84)" },
  { key: "Laengengrad",                        title: "Koordinate: Längengrad (WGS84)" },
  { key: "TechnologieStromerzeugung",          title: "Technologie der Stromerzeugung" },
  { key: "ArtDerSolaranlageBezeichnung",       title: "Art der Solaranlage" },
  { key: "AnzahlSolarModule",                  title: "Anzahl der Solar-Module" },
  { key: "HauptausrichtungSolarModuleBezeichnung", title: "Hauptausrichtung der Solar-Module" },
  { key: "HauptneigungswinkelSolarmodule",     title: "Hauptneigungswinkel der Solar-Module" },
  { key: "SolarparkName",                      title: "Name des Solarparks" },
  { key: "SpeicherEinheitMastrNummer",         title: "MaStR-Nr. der Speichereinheit" },
  { key: "StromspeichertechnologieBezeichnung",title: "Speichertechnologie" },
  { key: "NutzbareSpeicherkapazitaet",         title: "Nutzbare Speicherkapazitaet in kWh" },
  { key: "DatumLetzteAktualisierung",          title: "Letzte Aktualisierung" },
  { key: "EndgueltigeStilllegungDatum",        title: "Datum der endgültigen Stilllegung" },
  { key: "GeplantesInbetriebsnahmeDatum",      title: "Datum der geplanten Inbetriebnahme" },
  { key: "AnlagenbetreiberName",               title: "Name des Anlagenbetreibers (nur Org.)" },
  { key: "AnlagenbetreiberMaStRNummer",        title: "MaStR-Nr. des Anlagenbetreibers" },
  { key: "VollTeilEinspeisungBezeichnung",     title: "Volleinspeisung oder Teileinspeisung" },
  { key: "GenehmigungsMastrNummer",            title: "MaStR-Nr. der Genehmigung" },
  { key: "NetzbetreiberNamen",                 title: "Name des Anschluss-Netzbetreibers" },
  { key: "NetzbetreiberMaStRNummer",           title: "MaStR-Nr. des Anschluss-Netzbetreibers" },
  { key: "IsNBPruefungAbgeschlossen",          title: "Netzbetreiberprüfung" },
  { key: "SpannungsebenenNamen",               title: "Spannungsebene" },
  { key: "LokationMastrNr",                    title: "MaStR-Nr. der Lokation" },
  { key: "EegAnlageMastrNummer",               title: "MaStR-Nr. der EEG-Anlage" },
  { key: "EegAnlagenschluessel",               title: "EEG-Anlagenschlüssel" },
  { key: "EegInbetriebnahmeDatum",             title: "Inbetriebnahmedatum der EEG-Anlage" },
  { key: "EegInstallierteLeistung",            title: "Installierte Leistung der EEG-Anlage" },
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

// Fetch JSON mit Retries
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

// Energieträger-Code auflösen
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
  } catch {/* ignore */}
  return "2495"; // Solare Strahlungsenergie
}

// ---------- NEU: Datumsbereich in kleinere Intervalle splitten ----------
function splitRangeIntoChunks(startISO, endISO, daysPerChunk) {
  const chunks = [];
  const start = parseISODate(startISO);
  const end = parseISODate(endISO);
  if (!start || !end || !(daysPerChunk > 0)) return [{ startISO, endISO }];

  let currentStart = new Date(start.getTime());
  const finalEnd = new Date(end.getTime());

  while (currentStart < finalEnd) {
    const currentEnd = new Date(currentStart.getTime());
    currentEnd.setUTCDate(currentEnd.getUTCDate() + daysPerChunk);
    if (currentEnd > finalEnd) currentEnd.setTime(finalEnd.getTime());

    chunks.push({
      startISO: currentStart.toISOString().slice(0, 10), // YYYY-MM-DD
      endISO: currentEnd.toISOString().slice(0, 10),
    });

    currentStart = new Date(currentEnd.getTime());
  }

  return chunks;
}

// ---------- NEU: Concurrency-Limiter ----------
async function runWithConcurrencyLimit(tasks, maxConcurrent) {
  const results = [];
  let currentIndex = 0;
  let active = 0;

  return new Promise((resolve, reject) => {
    const runNext = () => {
      if (currentIndex >= tasks.length && active === 0) {
        return resolve(results);
      }

      while (active < maxConcurrent && currentIndex < tasks.length) {
        const taskIndex = currentIndex++;
        const task = tasks[taskIndex];
        active++;

        task()
          .then((result) => {
            results[taskIndex] = result;
          })
          .catch(reject)
          .finally(() => {
            active--;
            runNext();
          });
      }
    };

    runNext();
  });
}

// ---------- Bestehender Helfer für EINEN Zeitbereich (unverändert) ----------
async function fetchRangeRows({
  startISO,
  endISO,
  carrierCode,
  statusQ,
  pageSize,
  maxPages,
  signal
}) {
  const dateClause = buildMastrDateRange(startISO, endISO);
  if (!dateClause) {
    throw new Error("Invalid date format. Use YYYY-MM-DD for 'start' and 'end'.");
  }

  const parts = [dateClause, `Energieträger~eq~'${carrierCode}'`];
  if (statusQ && statusQ !== "off") {
    const statusCode = /^\d+$/.test(statusQ) ? statusQ : "35";
    parts.push(`Betriebs-Status~eq~'${statusCode}'`);
  }
  const filterRaw = parts.join("~and~");
  const filterEncoded = encodeURIComponent(filterRaw);

  let page = 1;
  const rows = [];
  let pagesFetched = 0;

  while (true) {
    if (maxPages > 0 && page > maxPages) break;

    const q =
      `${BASE}?group=&sort=&aggregate=` +
      `&forExport=true` +
      `&page=${page}&pageSize=${pageSize}` +
      `&filter=${filterEncoded}`;

    const j = await fetchJSON(q, signal);
    if (j && j.Error) {
      throw new Error(`Upstream reported Error (Type=${j.Type || "?"}): ${j.Message || "no message"}`);
    }

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

    pagesFetched++;
    page++;
  }

  return { rows, pagesFetched };
}

// ---------- NEU: Gesamten Zeitraum (ggf. parallel) holen ----------
async function fetchAllRows({
  startISO,
  endISO,
  carrierCode,
  statusQ,
  pageSize,
  maxPages,
  signal,
  chunkDays,
  maxConcurrent
}) {
  const chunkDaysNum = Number(chunkDays) || 0;

  // Falls chunkDays <= 0 → keine Parallelisierung, alter Weg
  if (chunkDaysNum <= 0) {
    return await fetchRangeRows({
      startISO,
      endISO,
      carrierCode,
      statusQ,
      pageSize,
      maxPages,
      signal
    });
  }

  const chunks = splitRangeIntoChunks(startISO, endISO, chunkDaysNum);

  const limit = maxConcurrent && maxConcurrent > 0 ? maxConcurrent : 4;

  const tasks = chunks.map((chunk) => {
    return () =>
      fetchRangeRows({
        startISO: chunk.startISO,
        endISO: chunk.endISO,
        carrierCode,
        statusQ,
        pageSize,
        maxPages,
        signal
      });
  });

  const results = await runWithConcurrencyLimit(tasks, limit);

  const allRows = [];
  let totalPagesFetched = 0;

  for (const r of results) {
    if (!r) continue;
    if (Array.isArray(r.rows)) allRows.push(...r.rows);
    if (typeof r.pagesFetched === "number") totalPagesFetched += r.pagesFetched;
  }

  // Reihenfolge: Chunks in zeitlicher Reihenfolge, innerhalb des Chunks wie im Original
  return { rows: allRows, pagesFetched: totalPagesFetched };
}

// ---------------------- Handler ----------------------
module.exports = async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "no-store");

  try {
    const url = new URL(req.url, `https://${req.headers.host}`);
    const startISO = url.searchParams.get("start");
    const endISO   = url.searchParams.get("end");
    const carrierQ = (url.searchParams.get("carrier") || "2495").trim();
    const statusQ  = (url.searchParams.get("status") || "").trim().toLowerCase();
    const format   = (url.searchParams.get("format") || "csv").toLowerCase();
    const debugQ   = (url.searchParams.get("debug") || "").toLowerCase();

    const pageSizeReq = parseInt(url.searchParams.get("pagesize") || "2000", 10);
    const pageSize = Math.max(1, Math.min(isNaN(pageSizeReq) ? 2000 : pageSizeReq, 5000));

    const maxPagesReq = parseInt(url.searchParams.get("maxpages") || "0", 10);
    const maxPages = Math.max(0, isNaN(maxPagesReq) ? 0 : maxPagesReq);

    // NEU: Parallelisierungs-Parameter
    const chunkDaysReq = parseInt(url.searchParams.get("chunkdays") || "", 10);
    // Default: 3 Tage pro Chunk, 0 = Parallelisierung aus
    const chunkDays = isNaN(chunkDaysReq) ? 3 : Math.max(0, chunkDaysReq);

    const maxConcurrentReq = parseInt(url.searchParams.get("maxconcurrent") || "", 10);
    // Default: max. 4 parallele Zeitintervalle, min. 1
    const maxConcurrent = isNaN(maxConcurrentReq) ? 4 : Math.max(1, maxConcurrentReq);

    if (!startISO || !endISO) {
      res.status(400).send("Missing 'start' or 'end' (YYYY-MM-DD). Example: ?start=2024-01-01&end=2024-02-01&format=csv");
      return;
    }

    // einfache Datumsvalidierung (für saubere 400er)
    if (!buildMastrDateRange(startISO, endISO)) {
      res.status(400).send("Invalid date format. Use YYYY-MM-DD for 'start' and 'end'.");
      return;
    }

    const ac = new AbortController();
    const carrierCode = await tryResolveCarrierCode(carrierQ, ac.signal);

    // Debug-Modi wie bisher, aber über fetchAllRows (inkl. Parallelisierung)
    if (debugQ === "sample" || debugQ === "keys" || debugQ === "fields") {
      const { rows } = await fetchAllRows({
        startISO,
        endISO,
        carrierCode,
        statusQ,
        pageSize,
        maxPages,
        signal: ac.signal,
        chunkDays,
        maxConcurrent
      });

      const items = rows;
      if (debugQ === "sample") {
        res.setHeader("Content-Type", "application/json; charset=utf-8");
        res.status(200).send(JSON.stringify(items[0] || {}, null, 2));
        return;
      }

      if (debugQ === "keys") {
        const sample = items.slice(0, 200);
        const counts = {};
        for (const rec of sample) {
          for (const k of Object.keys(rec || {})) {
            if (!counts[k]) counts[k] = 0;
            if (rec[k] !== null && rec[k] !== "") counts[k]++;
          }
        }
        const keys = Object.entries(counts)
          .sort((a, b) => b[1] - a[1])
          .map(([key, count]) => ({ key, nonNullInSample: count }));
        res.setHeader("Content-Type", "application/json; charset=utf-8");
        res.status(200).send(JSON.stringify({ keys, sampleSize: sample.length }, null, 2));
        return;
      }

      if (debugQ === "fields") {
        const sample = items[0] || {};
        const report = COLUMNS.map(c => ({
          title: c.title,
          key: c.key,
          presentInSample: Object.prototype.hasOwnProperty.call(sample, c.title),
          sampleValue: sample[c.title] ?? null
        }));
        res.setHeader("Content-Type", "application/json; charset=utf-8");
        res.status(200).send(JSON.stringify({ report }, null, 2));
        return;
      }
    }

    // normaler Weg über neuen Helfer (ggf. parallel)
    const { rows, pagesFetched } = await fetchAllRows({
      startISO,
      endISO,
      carrierCode,
      statusQ,
      pageSize,
      maxPages,
      signal: ac.signal,
      chunkDays,
      maxConcurrent
    });

    res.setHeader("X-Pages-Fetched", String(pagesFetched));
    res.setHeader("X-Rows", String(rows.length));
    res.setHeader("X-Chunk-Days", String(chunkDays));
    res.setHeader("X-Max-Concurrent-Intervals", String(maxConcurrent));

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
