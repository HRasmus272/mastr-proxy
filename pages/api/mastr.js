// /api/mastr.js
export const config = { runtime: "edge" };

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

function toDDMMYYYY(iso) {
  // erwartet 'YYYY-MM-DD'
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso || "");
  if (!m) return iso;
  const [, y, mo, d] = m;
  return `${d}.${mo}.${y}`;
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
    const startISO = url.searchParams.get("start");
    const endISO   = url.searchParams.get("end");
    const carrierQ = (url.searchParams.get("carrier") || "Solare Strahlungsenergie").trim();
    const pageSize = Math.min(parseInt(url.searchParams.get("pagesize") || "2000", 10), 5000);
    const format   = (url.searchParams.get("format") || "csv").toLowerCase();

    if (!startISO || !endISO) {
      return new Response(
        "Missing 'start' or 'end' (YYYY-MM-DD). Example: ?start=2024-01-01&end=2024-01-31&format=csv",
        { status: 400 }
      );
    }

    const start = toDDMMYYYY(startISO); // -> 'DD.MM.YYYY'
    const end   = toDDMMYYYY(endISO);

    // 1) Energieträger-Code ermitteln (Name -> Value)
    const meta = await fetchJSON(FILTER_META);
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

    if (!carrierCode) {
      const options = carrierFilter?.ListObject?.map(x => ({ name: x.Name, value: x.Value })) || [];
      const body = JSON.stringify({
        error: "Unknown 'carrier'. Use numeric Value or one of the provided names.",
        tried: carrierQ,
        examples: options.slice(0, 50)
      });
      return new Response(body, {
        status: 400,
        headers: { "Content-Type": "application/json; charset=utf-8", "Access-Control-Allow-Origin": "*" }
      });
    }

    // 2) Filter mit Quotes und deutschem Datumsformat (siehe MaStR-URL-Beispiel)
    const filterRaw =
      `Inbetriebnahmedatum der Einheit~ge~'${start}'` +
      `~and~Inbetriebnahmedatum der Einheit~lt~'${end}'` +
      `~and~Energieträger~eq~'${carrierCode}'`;

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
