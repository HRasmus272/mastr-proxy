export const config = { runtime: "edge" };

const BASE =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetErweiterteOeffentlicheEinheitStromerzeugung";

// Spalten, die wir ausgeben wollen (Key = Quellspalte, title = Zielspaltenname)
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

export default async function handler(req) {
  const url = new URL(req.url);
  const start     = url.searchParams.get("start");
  const end       = url.searchParams.get("end");
  const carrier   = url.searchParams.get("carrier") || "Solare Strahlungsenergie";
  const pageSize  = Math.min(parseInt(url.searchParams.get("pagesize") || "2000", 10), 5000);
  const format    = (url.searchParams.get("format") || "csv").toLowerCase();

  if (!start || !end) {
    return new Response(
      "Missing 'start' or 'end' (YYYY-MM-DD). Example: ?start=2024-01-01&end=2024-01-31&format=csv",
      { status: 400 }
    );
  }

  // Kendo-Filter (zwischen start inkl. und end exklusiv)
  const filterRaw =
    `Inbetriebnahmedatum der Einheit~ge~${start}` +
    `~and~Inbetriebnahmedatum der Einheit~lt~${end}` +
    `~and~Energieträger~eq~"${carrier}"`;

  let page = 1;
  const rows = [];
  const maxPages = 200;

  while (page <= maxPages) {
    const skip = (page - 1) * pageSize;
    const take = pageSize;

    // Kendo-Parameter mitgeben; Filter URL-encoden
    const q =
      `${BASE}?group=&sort=&aggregate=` +
      `&page=${page}&pageSize=${pageSize}` +
      `&skip=${skip}&take=${take}` +
      `&filter=${encodeURIComponent(filterRaw)}`;

    const resp = await fetch(q, {
      headers: {
        "Accept": "application/json",
        "User-Agent": "mastr-proxy-vercel",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.marktstammdatenregister.de/"
      }
    });

    if (!resp.ok) {
      return new Response(`Upstream error ${resp.status}: ${await resp.text()}`, { status: 502 });
    }

    let j;
    try {
      j = await resp.json();
    } catch (e) {
      return new Response(`Upstream returned non-JSON`, { status: 502 });
    }

    // Upstream-Fehler nicht verschlucken
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
}
