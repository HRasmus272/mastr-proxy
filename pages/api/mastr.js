// /api/mastr.js
export const config = { runtime: "edge" };

// Offiziell dokumentierte (Kendo-)Endpoints laut bund.dev-Repo
const BASE =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetErweiterteOeffentlicheEinheitStromerzeugung";
const FILTER_META =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetFilterColumnsErweiterteOeffentlicheEinheitStromerzeugung";

// Spaltenauswahl (Key = Quellspalte, title = Zielspaltenname)
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

async function fetchJSON(url) {
  const resp = await fetch(url, {
    headers: {
      "Accept": "application/json",
      "User-Agent": "mastr-proxy-vercel",
      "X-Requested-With": "XMLHttpRequest",
      "Referer": "https://www.marktstammdatenregister.de/"
    }
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new Error(`Upstream HTTP ${resp.status}: ${body?.slice(0, 300)}`);
  }
  return resp.json();
}

export default async function handler(req) {
  try {
    const url = new URL(req.url);
    const start    = url.searchParams.get("start");
    const end      = url.searchParams.get("end");
    const carrierQ = (url.searchParams.get("carrier") || "Solare Strahlungsenergie").trim();
    const pageSize = Math.min(parseInt(url.searchParams.get("pagesize") || "2000", 10), 5000);
    const format   = (url.searchParams.get("format") || "csv").toLowerCase();

    if (!start || !end) {
      return new Response(
        "Missing 'start' or 'end' (YYYY-MM-DD). Example: ?start=2024-01-01&end=2024-01-31&format=csv",
        { status: 400 }
      );
    }

    // 1) Lade Meta-Filter und ermittle Code für 'Energieträger'
    const meta = await fetchJSON(FILTER_META);
    // meta: [{ FilterName, ListObject: [{Name, Value}], Type }, ...]
    const carrierFilter = Array.isArray(meta)
      ? meta.find(f => (f.FilterName || "").toLowerCase() === "energieträger")
      : null;

    let carrierCode = null;

    if (carrierFilter && Array.isArray(carrierFilter.ListObject)) {
      // a) numerischer Code übergeben?
      if (/^\d+$/.test(carrierQ)) {
        const hit = carrierFilter.ListObject.find(x => String(x.Value) === carrierQ);
        if (hit) carrierCode = String(hit.Value);
      }
      // b) sonst: Name matchen (case-insensitiv, inkl. Teiltreffer am Anfang)
      if (!carrierCode) {
        const cq = carrierQ.toLowerCase();
        // erst exakter Name, dann startsWith, dann includes
        const exact = carrierFilter.ListObject.find(x => (x.Name || "").toLowerCase() === cq);
        const starts = carrierFilter.ListObject.find(x => (x.Name || "").toLowerCase().startsWith(cq));
        const incl = carrierFilter.ListObject.find(x => (x.Name || "").toLowerCase().includes(cq));
        const chosen = exact || starts || incl || null;
        if (chosen) carrierCode = String(chosen.Value);
      }
    }

    if (!carrierCode) {
      // Carrier nicht gefunden → gebe Liste zurück, damit der Nutzer sieht, was möglich ist
      const options = carrierFilter?.ListObject?.map(x => ({ name: x.Name, value: x.Value })) || [];
      const body = JSON.stringify({
        error: "Unknown 'carrier'. Use numeric Value or one of the provided names.",
        tried: carrierQ,
        examples: options.slice(0, 50) // nicht zu groß machen
      });
      return new Response(body, {
        status: 400,
        headers: { "Content-Type": "application/json; charset=utf-8", "Access-Control-Allow-Origin": "*" }
      });
    }

    // 2) Kendo-Filter: Datum [start, end) + Energieträger (NUMERISCH, ohne Anführungszeichen)
    const filterRaw =
      `Inbetriebnahmedatum der Einheit~ge~${start}` +
      `~and~Inbetriebnahmedatum der Einheit~lt~${end}` +
      `~and~Energieträger~eq~${carrierCode}`;

    let page = 1;
    const rows = [];
    const maxPages = 200;

    while (page <= maxPages) {
      const skip = (page - 1) * pageSize;
      const take = pageSize;

      const q =
        `${BASE}?group=&sort=&aggregate=` +
        `&page=${page}&pageSize=${pageSize}` +
        `&skip=${skip}&take=${take}` +
        `&filter=${encodeURIComponent(filterRaw)}`;

      const j = await fetchJSON(q);

      if (j && j.Error) {
        const msg = j.Message || "no message";
        const typ = j.Type || "?";
        return new Response(`Upstream reported Error (Type=${typ}): ${msg}`, { status: 502 });
      }

      const data = Array.isArray(j) ? j : (j.Data || j.data || []);
      if (!Array.isArray(data) || data.length === 0) break;

      for (const rec of data) {
        const out = {};
        for (const col of COLUMNS) {
          out[col.title] = rec[col.key] ?? "";
        }
        rows.push(out);
      }

      page++;
    }

    const commonHeaders = {
      "Cache-Control": "no-store",
      "Access-Control-Allow-Origin": "*"
    };

    if (format === "json") {
      return new Response(JSON.stringify(rows), {
        status: 200,
        headers: { ...commonHeaders, "Content-Type": "application/json; charset=utf-8" }
      });
    } else {
      const csv = toCSV(rows);
      return new Response(csv, {
        status: 200,
        headers: { ...commonHeaders, "Content-Type": "text/csv; charset=utf-8" }
      });
    }
  } catch (err) {
    const msg = (err && err.message) ? err.message : String(err);
    return new Response(`Proxy error: ${msg}`, {
      status: 502,
      headers: { "Access-Control-Allow-Origin": "*" }
    });
  }
}
