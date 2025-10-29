export const config = { runtime: "edge" };

const BASE = "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetErweiterteOeffentlicheEinheitStromerzeugung";

const COLUMNS: { key: string; title: string }[] = [
  { key: "MaStR-Nummer der Einheit", title: "MaStRNummer" },
  { key: "Anlagenbetreiber (Name)",   title: "Betreiber" },
  { key: "Energieträger",             title: "Energietraeger" },
  { key: "Bruttoleistung",            title: "Bruttoleistung" },
  { key: "Nettonennleistung",         title: "Nettonennleistung" },
  { key: "Bundesland",                title: "Bundesland" },
  { key: "Postleitzahl",              title: "PLZ" },
  { key: "Ort",                       title: "Ort" },
  { key: "Inbetriebnahmedatum der Einheit", title: "Inbetriebnahme" }
];

function toCSV(rows: any[]): string {
  const header = COLUMNS.map(c => c.title).join(",");
  const esc = (v: any) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const lines = rows.map(r => COLUMNS.map(c => esc(r[c.title])).join(","));
  return [header, ...lines].join("\n");
}

export default async function handler(req: Request) {
  const url = new URL(req.url);
  const start = url.searchParams.get("start");
  const end   = url.searchParams.get("end");
  const carrier = url.searchParams.get("carrier") ?? "Solare Strahlungsenergie";
  const pageSize = Math.min(parseInt(url.searchParams.get("pagesize") || "2000", 10), 5000);
  const format = (url.searchParams.get("format") || "csv").toLowerCase();

  if (!start || !end) {
    return new Response("Missing 'start' or 'end' (YYYY-MM-DD). Example: ?start=2025-10-01&end=2025-11-01&format=csv", { status: 400 });
  }

  const filterRaw =
    `Inbetriebnahmedatum der Einheit~ge~${start}` +
    `~and~Inbetriebnahmedatum der Einheit~lt~${end}` +
    `~and~Energieträger~eq~"${carrier}"`;
  const filter = encodeURIComponent(filterRaw);

  let page = 1;
  const rows: any[] = [];
  const maxPages = 200;

  while (page <= maxPages) {
    const q = `${BASE}?filter=${filter}&page=${page}&pageSize=${pageSize}`;
    const resp = await fetch(q, {
      headers: {
        "Accept": "application/json",
        "User-Agent": "mastr-proxy-vercel"
      }
    });
    if (!resp.ok) {
      return new Response(`Upstream error ${resp.status}: ${await resp.text()}`, { status: 502 });
    }
    const j: any = await resp.json();
    const data = Array.isArray(j) ? j : (j.Data ?? j.data ?? []);
    if (!Array.isArray(data) || data.length === 0) break;

    for (const rec of data) {
      const out: any = {};
      for (const col of COLUMNS) {
        out[col.title] = rec[col.key] ?? "";
      }
      rows.push(out);
    }
    page++;
  }

  const headers: HeadersInit = {
    "Cache-Control": "no-store",
    "Access-Control-Allow-Origin": "*"
  };

  if (format === "json") {
    return new Response(JSON.stringify(rows), { status: 200, headers: { ...headers, "Content-Type": "application/json; charset=utf-8" } });
  } else {
    const csv = toCSV(rows);
    return new Response(csv, { status: 200, headers: { ...headers, "Content-Type": "text/csv; charset=utf-8" } });
  }
}
