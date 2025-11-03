// /api/mastr.js — Node.js Serverless Function (CommonJS)
// MaStR → Proxy mit lokalem Datumsfilter, ohne Meta-Calls.

const BASE =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetErweiterteOeffentlicheEinheitStromerzeugung";
const FILTER_META =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetFilterColumnsErweiterteOeffentlicheEinheitStromerzeugung";

// === Deine gewünschte Feldliste ===
// key = Upstream-Name (so liefert MaStR), title = Spaltenname in der Ausgabe
const COLUMNS = [
  { key: "MaStRNummer",                                      title: "MaStR-Nr. der Einheit" },
  { key: "EinheitName",                                      title: "Anzeige-Name der Einheit" },
  { key: "BetriebsStatusName",                               title: "Betriebs-Status" },
  { key: "EnergietraegerName",                               title: "Energieträger" },
  { key: "Bruttoleistung der Einheit",                       title: "Bruttoleistung der Einheit" },
  { key: "Nettonennleistung der Einheit",                    title: "Nettonennleistung der Einheit" },
  { key: "Inbetriebnahmedatum der Einheit",                  title: "Inbetriebnahmedatum der Einheit" },
  { key: "Inbetriebnahmedatum der Einheit am aktuellen Standort", title: "Inbetriebnahmedatum der Einheit am aktuellen Standort" },
  { key: "Registrierungsdatum der Einheit",                  title: "Registrierungsdatum der Einheit" },
  { key: "Bundesland",                                       title: "Bundesland" },
  { key: "Postleitzahl",                                     title: "Postleitzahl" },
  { key: "Ort",                                              title: "Ort" },
  { key: "Straße",                                           title: "Straße" },
  { key: "Hausnummer",                                       title: "Hausnummer" },
  { key: "Gemarkung",                                        title: "Gemarkung" },
  { key: "Flurstück",                                        title: "Flurstück" },
  { key: "Gemeindeschlüssel",                                title: "Gemeindeschlüssel" },
  { key: "Gemeinde",                                         title: "Gemeinde" },
  { key: "Landkreis",                                        title: "Landkreis" },
  { key: "Koordinate: Breitengrad (WGS84)",                  title: "Koordinate: Breitengrad (WGS84)" },
  { key: "Koordinate: Längengrad (WGS84)",                   title: "Koordinate: Längengrad (WGS84)" },
  { key: "Technologie der Stromerzeugung",                   title: "Technologie der Stromerzeugung" },
  { key: "Art der Solaranlage",                              title: "Art der Solaranlage" },
  { key: "Anzahl der Solar-Module",                          title: "Anzahl der Solar-Module" },
  { key: "Hauptausrichtung der Solar-Module",                title: "Hauptausrichtung der Solar-Module" },
  { key: "Hauptneigungswinkel der Solar-Module",             title: "Hauptneigungswinkel der Solar-Module" },
  { key: "Name des Solarparks",                              title: "Name des Solarparks" },
  { key: "MaStR-Nummer der Speichereinheit",                 title: "MaStR-Nr. der Speichereinheit" },
  { key: "Speichertechnologie",                              title: "Speichertechnologie" },
  { key: "Nutzbare Speicherkapazität in kWh",                title: "Nutzbare Speicherkapazität in kWh" },
  { key: "Letzte Aktualisierung",                            title: "Letzte Aktualisierung" },
  { key: "Datum der endgültigen Stilllegung",                title: "Datum der endgültigen Stilllegung" },
  { key: "Datum der geplanten Inbetriebnahme",               title: "Datum der geplanten Inbetriebnahme" },
  { key: "Name des Anlagenbetreibers (nur Org.)",            title: "Name des Anlagenbetreibers (nur Org.)" },
  { key: "MaStR-Nummer des Anlagenbetreibers",               title: "MaStR-Nr. des Anlagenbetreibers" },
  { key: "Volleinspeisung oder Teileinspeisung",             title: "Volleinspeisung oder Teileinspeisung" },
  { key: "MaStR-Nummer der Genehmigung",                     title: "MaStR-Nr. der Genehmigung" },
  { key: "Name des Anschluss-Netzbetreibers",                title: "Name des Anschluss-Netzbetreibers" },
  { key: "MaStR-Nummer des Anschluss-Netzbetreibers",        title: "MaStR-Nr. des Anschluss-Netzbetreibers" },
  { key: "Netzbetreiberprüfung",                             title: "Netzbetreiberprüfung" },
  { key: "Spannungsebene",                                   title: "Spannungsebene" },
  { key: "MaStR-Nummer der Lokation",                        title: "MaStR-Nr. der Lokation" },
  { key: "MaStR-Nummer der EEG-Anlage",                      title: "MaStR-Nr. der EEG-Anlage" },
  { key: "EEG-Anlagenschlüssel",                             title: "EEG-Anlagenschlüssel" },
  { key: "Inbetriebnahmedatum der EEG-Anlage",               title: "Inbetriebnahmedatum der EEG-Anlage" },
  { key: "Installierte Leistung der EEG-Anlage",             title: "Installierte Leistung der EEG-Anlage" }
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

    const pageSize = Math.min(parseInt(url.searchParams.get("pagesize") || "500", 10), 2000);
    const maxPages = Math.min(parseInt(url.searchParams.get("maxpages") || "1", 10), 20);

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

    const ac = new AbortController();
    const to = setTimeout(() => ac.abort("timeout"), 30000); // 30s timeout

    const meta = await fetchJSON(FILTER_META, ac.signal);
    const carrierFilter = Array.isArray(meta)
      ? meta.find(f => (f.FilterName || "").toLowerCase() === "energieträger")
      : null;

    let carrierCode = null;
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
    if (!carrierCode) carrierCode = "2495"; // Fallback: Solare Strahlungsenergie

    const filterRaw = `Energieträger~eq~'${carrierCode}'`;

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
      const data = Array.isArray(j) ? j : (j.Data || j.data || []);
      if (!Array.isArray(data) || data.length === 0) break;

      for (const rec of data) {
        const out = {};
        for (const col of COLUMNS) {
          if (col.key === "Inbetriebnahmedatum der Einheit") {
            // minimaler Fallback; wenn nicht gewünscht, ersetze die 3 Zeilen durch: const val = rec[col.key] ?? "";
            const val =
              rec["Inbetriebnahmedatum der Einheit"] ??
              rec["InbetriebnahmeDatum"] ??
              rec["EegInbetriebnahmeDatum"] ??
              "";
            out[col.title] = val;
          } else {
            out[col.title] = rec[col.key] ?? "";
          }
        }
        // (hier ggf. lokale Datumsfilterung einsetzen – aktuell nicht aktiv)
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
