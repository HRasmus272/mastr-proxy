// /api/mastr-detail.js  — Detail-Endpoint per MaStR-Nummer (CommonJS, Node runtime)
// Aufruf:  /api/mastr-detail?mastr=SEE984033548619
// Liefert: genau 1 Datensatz (oder 404)

const BASE =
  "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetErweiterteOeffentlicheEinheitStromerzeugung";

function parseTicksToISO(ticks) {
  // erwartet "/Date(1184889600000)/"
  if (!ticks || typeof ticks !== "string") return null;
  const m = /\/Date\((\d+)\)\//.exec(ticks);
  if (!m) return null;
  const ms = Number(m[1]);
  if (!Number.isFinite(ms)) return null;
  const d = new Date(ms);
  // ISO nur Datum (YYYY-MM-DD)
  return d.toISOString().slice(0, 10);
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
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "no-store");

  try {
    const url = new URL(req.url, `https://${req.headers.host}`);
    const mastr = (url.searchParams.get("mastr") || "").trim();

    if (!mastr) {
      res.status(400).send("Missing 'mastr' query parameter. Example: ?mastr=SEE984033548619");
      return;
    }

    // Pro-Request Timeout (8s)
    const ac = new AbortController();
    const to = setTimeout(() => ac.abort("timeout"), 8000);

    // Kendo-Filter: exakter Vergleich auf MaStRNummer
    const page = 1, pageSize = 1, skip = 0, take = 1;
    const filterRaw = `MaStRNummer~eq~'${mastr}'`;
    const q =
      `${BASE}?group=&sort=&aggregate=` +
      `&page=${page}&pageSize=${pageSize}` +
      `&skip=${skip}&take=${take}` +
      `&filter=${encodeURIComponent(filterRaw)}`;

    const j = await fetchJSON(q, ac.signal);
    clearTimeout(to);

    if (j && j.Error) {
      res.status(502).send(`Upstream reported Error (Type=${j.Type || "?"}): ${j.Message || "no message"}`);
      return;
    }

    const data = Array.isArray(j) ? j : (j.Data || j.data || []);
    if (!Array.isArray(data) || data.length === 0) {
      res.status(404).send(`No unit found for MaStR=${mastr}`);
      return;
    }

    const rec = data[0];

    // Komfort: Datumsfelder zusätzlich als ISO (YYYY-MM-DD) beilegen
    const withDates = {
      ...rec,
      EegInbetriebnahmeISO: parseTicksToISO(rec["EegInbetriebnahmeDatum"]),
      InbetriebnahmeAltISO: parseTicksToISO(
        rec["Inbetriebnahmedatum der Einheit"] ?? rec["InbetriebnahmeDatum"]
      )
    };

    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.status(200).send(JSON.stringify(withDates));
  } catch (err) {
    const msg = (err && err.message) ? err.message : String(err);
    res.status(502).send(`Proxy error: ${msg}`);
  }
};
