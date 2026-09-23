//! Self-contained HTML reports (run report and sweep heatmap). No external scripts; fonts come
//! from Google Fonts with system fallbacks so the file also works offline.

use std::fmt::Write as _;

use super::*;
use crate::sweep::SweepResult;
use crate::util::units::{fmt_pct, fmt_rate, fmt_us};

pub fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn ms(v: f64) -> String {
    fmt_us(v * 1e3)
}

/// Axis / value label with precision adapted to magnitude.
fn num(v: f64) -> String {
    if v == 0.0 {
        "0".into()
    } else if v.abs() < 10.0 {
        format!("{v:.1}")
    } else if v.abs() < 10_000.0 {
        format!("{v:.0}")
    } else {
        format!("{:.1}k", v / 1000.0)
    }
}

const FONTS: &str = r#"<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:ital,wght@0,400;0,500;0,600;0,700;1,400&display=swap">"#;

const CSS: &str = r#"
:root{
  --ground:#F5F6F3; --surface:#FFFFFF; --sunk:#ECEEEA; --ink:#16191D; --muted:#5E6770; --faint:#8A939B;
  --hair:#D9DDDF; --accent:#2F47A8; --accent-soft:#E4E8F6;
  --crit:#B3261E; --crit-soft:#F8E3E1; --warn:#9A5B00; --warn-soft:#F7EBD6; --ok:#2F6B3A; --ok-soft:#E1EFE3; --info:#1D6A80; --info-soft:#DDEEF2;
  --h0:#EEF0EC; --h1:#F3E6C4; --h2:#EBC57C; --h3:#DE8E3F; --h4:#C4512C; --h5:#8E2016;
  --sans:"IBM Plex Sans",system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  --mono:"IBM Plex Mono",ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
}
@media (prefers-color-scheme: dark){
  :root:not([data-theme="light"]){
    color-scheme:dark;
    --ground:#121518; --surface:#191D21; --sunk:#1F2428; --ink:#E8EBEE; --muted:#A0A9B2; --faint:#76808A;
    --hair:#2C3238; --accent:#93A6F2; --accent-soft:#232B45;
    --crit:#F08A80; --crit-soft:#3A1F1D; --warn:#E6B45C; --warn-soft:#352A17; --ok:#86C792; --ok-soft:#1D2E21; --info:#7CC2D6; --info-soft:#17303A;
    --h0:#23282C; --h1:#3B3726; --h2:#6B5626; --h3:#99602A; --h4:#C4512C; --h5:#F07A5E;
  }
}
:root[data-theme="dark"]{
  color-scheme:dark;
  --ground:#121518; --surface:#191D21; --sunk:#1F2428; --ink:#E8EBEE; --muted:#A0A9B2; --faint:#76808A;
  --hair:#2C3238; --accent:#93A6F2; --accent-soft:#232B45;
  --crit:#F08A80; --crit-soft:#3A1F1D; --warn:#E6B45C; --warn-soft:#352A17; --ok:#86C792; --ok-soft:#1D2E21; --info:#7CC2D6; --info-soft:#17303A;
  --h0:#23282C; --h1:#3B3726; --h2:#6B5626; --h3:#99602A; --h4:#C4512C; --h5:#F07A5E;
}
*{box-sizing:border-box}
body{background:var(--ground);color:var(--ink);font-family:var(--sans);font-size:15px;line-height:1.5;margin:0;padding-inline:clamp(16px,4vw,40px);padding-block:32px 64px}
.page{max-width:1180px;margin:0 auto;display:flex;flex-direction:column;gap:40px}
h1,h2,h3{margin:0;text-wrap:balance;font-weight:600;letter-spacing:-0.01em}
h1{font-size:30px;line-height:1.15}
h2{font-size:19px}
h3{font-size:15px}
p{margin:0;max-width:72ch}
code,.mono{font-family:var(--mono);font-size:0.86em}
.eyebrow{font-size:12px;letter-spacing:0.08em;text-transform:uppercase;color:var(--muted);font-weight:500}
.muted{color:var(--muted)}
header.top{display:flex;flex-direction:column;gap:12px}
.chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{display:inline-flex;align-items:center;gap:6px;padding:3px 9px;border:1px solid var(--hair);border-radius:4px;background:var(--surface);font-size:13px;white-space:nowrap}
.chip b{font-weight:600}
.chip.k{font-family:var(--mono);font-size:12px}
section{display:flex;flex-direction:column;gap:14px}
.section-head{display:flex;align-items:baseline;justify-content:space-between;gap:12px;flex-wrap:wrap;border-bottom:1px solid var(--hair);padding-bottom:8px}
.verdict{display:grid;grid-template-columns:auto 1fr;gap:18px;align-items:start;background:var(--surface);border:1px solid var(--hair);border-radius:6px;padding:18px 20px}
.verdict .count{display:flex;gap:10px}
.tally{display:flex;flex-direction:column;align-items:center;min-width:64px;padding:6px 10px;border-radius:4px}
.tally b{font-size:24px;line-height:1.1;font-variant-numeric:tabular-nums}
.tally span{font-size:11px;letter-spacing:0.06em;text-transform:uppercase}
.tally.c{background:var(--crit-soft);color:var(--crit)} .tally.w{background:var(--warn-soft);color:var(--warn)} .tally.o{background:var(--ok-soft);color:var(--ok)}
.verdict p{font-size:16px;align-self:center}
.hot{display:grid;grid-template-columns:4px 1fr;background:var(--surface);border:1px solid var(--hair);border-radius:6px;overflow:hidden}
.hot .stripe{background:var(--info)}
.hot.Critical .stripe{background:var(--crit)} .hot.Warning .stripe{background:var(--warn)}
.hot .body{padding:14px 18px;display:flex;flex-direction:column;gap:8px;min-width:0}
.hot .title{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}
.sev{font-size:11px;font-weight:600;letter-spacing:0.07em;padding:2px 7px;border-radius:3px;text-transform:uppercase}
.sev.Critical{background:var(--crit-soft);color:var(--crit)} .sev.Warning{background:var(--warn-soft);color:var(--warn)} .sev.Info{background:var(--info-soft);color:var(--info)}
.cat{font-size:12px;color:var(--muted);font-family:var(--mono)}
.hot ul{margin:0;padding-left:18px;color:var(--muted);font-size:13.5px}
.hot ul li{overflow-wrap:anywhere}
.watch{display:flex;flex-wrap:wrap;gap:6px;align-items:center;font-size:12px}
.watch code{background:var(--sunk);padding:2px 6px;border-radius:3px;overflow-wrap:anywhere}
.knobs{border-collapse:collapse;font-size:13px;width:100%}
.knobs td{padding:4px 10px 4px 0;vertical-align:top;border-top:1px solid var(--hair)}
.knobs td:first-child{font-family:var(--mono);font-size:12px;white-space:nowrap}
.knobs td:nth-child(2){font-family:var(--mono);font-size:12px;color:var(--accent)}
.knobs td:last-child{color:var(--muted)}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:18px}
.panel{background:var(--surface);border:1px solid var(--hair);border-radius:6px;padding:16px 18px;display:flex;flex-direction:column;gap:12px;min-width:0}
.bars{display:flex;flex-direction:column;gap:7px}
.bar{display:grid;grid-template-columns:minmax(120px,190px) 1fr 52px;gap:10px;align-items:center;font-size:13px}
.bar .name{font-family:var(--mono);font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bar .track{height:12px;background:var(--sunk);border-radius:2px;overflow:hidden;position:relative}
.bar .fill{height:100%;border-radius:2px}
.bar .val{text-align:right;font-variant-numeric:tabular-nums}
.bar .sub{grid-column:2 / 4;color:var(--faint);font-size:12px;margin-top:-5px}
.svc{font-size:12px;letter-spacing:0.06em;text-transform:uppercase;color:var(--muted);margin-top:6px}
.shardmap{display:flex;flex-direction:column;gap:10px}
.shardhost{display:flex;flex-direction:column;gap:4px}
.shardhost .lbl{font-family:var(--mono);font-size:12px;color:var(--muted)}
.cells{display:flex;flex-wrap:wrap;gap:2px}
.cells i{display:block;width:var(--cell,9px);height:var(--cell,9px);border-radius:1px}
.legend{display:flex;align-items:center;gap:8px;font-size:12px;color:var(--muted);flex-wrap:wrap}
.legend .ramp{display:flex}
.legend .ramp i{display:block;width:22px;height:10px}
.spark{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px}
.spark figure{margin:0;background:var(--surface);border:1px solid var(--hair);border-radius:6px;padding:12px 14px;display:flex;flex-direction:column;gap:6px;min-width:0}
.spark figcaption{font-size:13px;display:flex;justify-content:space-between;gap:8px}
.spark figcaption b{font-variant-numeric:tabular-nums;font-weight:600}
.spark svg{width:100%;height:auto;display:block}
.tbl{overflow-x:auto;border:1px solid var(--hair);border-radius:6px;background:var(--surface)}
table.data{border-collapse:collapse;width:100%;font-size:13px;font-variant-numeric:tabular-nums}
table.data th{text-align:left;font-weight:500;color:var(--muted);font-size:12px;padding:8px 12px;border-bottom:1px solid var(--hair);white-space:nowrap;background:var(--sunk)}
table.data td{padding:6px 12px;border-bottom:1px solid var(--hair);white-space:nowrap}
table.data tr:last-child td{border-bottom:none}
table.data td.n,table.data th.n{text-align:right}
table.data td.m{font-family:var(--mono);font-size:12px}
details{background:var(--surface);border:1px solid var(--hair);border-radius:6px}
details>summary{cursor:pointer;padding:12px 16px;font-weight:500;list-style:none;display:flex;justify-content:space-between;gap:12px}
details>summary::-webkit-details-marker{display:none}
details>summary::after{content:"+";color:var(--muted);font-family:var(--mono)}
details[open]>summary::after{content:"–"}
details>.inner{padding:0 16px 16px;display:flex;flex-direction:column;gap:12px}
details .tbl{border:none;border-top:1px solid var(--hair);border-radius:0}
.notes{margin:0;padding-left:18px;font-size:13.5px;color:var(--muted);display:flex;flex-direction:column;gap:4px}
.warnings li{color:var(--warn)}
footer{font-size:12px;color:var(--faint);border-top:1px solid var(--hair);padding-top:12px}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
/* sweep */
.switch{display:flex;flex-wrap:wrap;gap:6px}
.switch button{font:inherit;font-size:13px;padding:5px 11px;border:1px solid var(--hair);background:var(--surface);color:var(--ink);border-radius:4px;cursor:pointer}
.switch button[aria-pressed="true"]{background:var(--accent);border-color:var(--accent);color:var(--surface)}
table.heat{border-collapse:separate;border-spacing:4px;font-size:13px;font-variant-numeric:tabular-nums}
table.heat th{font-weight:500;font-size:12px;color:var(--muted);padding:4px 8px;text-align:center;font-family:var(--mono)}
table.heat th.row{text-align:right;white-space:nowrap}
table.heat td{padding:0}
table.heat button{width:100%;min-width:128px;min-height:62px;border:1px solid transparent;border-radius:5px;cursor:pointer;font:inherit;color:var(--ink);display:flex;flex-direction:column;align-items:center;justify-content:center;gap:3px;padding:6px 8px}
table.heat button[aria-pressed="true"]{border-color:var(--ink);box-shadow:0 0 0 1px var(--ink) inset}
table.heat button .v{font-size:15px;font-weight:600}
table.heat button .s{font-size:10.5px;letter-spacing:0.06em;text-transform:uppercase;padding:1px 6px;border-radius:3px;background:var(--surface)}
.s.CRIT{color:var(--crit)} .s.WARN{color:var(--warn)} .s.OK{color:var(--ok)} .s.ERR{color:var(--crit)}
.axisnote{font-size:13px;color:var(--muted)}
.detail{display:flex;flex-direction:column;gap:12px}
@media (max-width:640px){
  h1{font-size:24px}
  .verdict{grid-template-columns:1fr}
  .bar{grid-template-columns:minmax(90px,120px) 1fr 44px}
}
@media (prefers-reduced-motion:no-preference){ .fill{transition:width .3s ease} }
"#;

/// Utilisation → heat token (0..1).
fn heat(u: f64) -> &'static str {
    match u {
        x if x < 0.05 => "var(--h0)",
        x if x < 0.30 => "var(--h1)",
        x if x < 0.55 => "var(--h2)",
        x if x < 0.75 => "var(--h3)",
        x if x < 0.90 => "var(--h4)",
        _ => "var(--h5)",
    }
}

fn legend_ramp(labels: &[&str]) -> String {
    let mut s = String::from("<div class=\"legend\"><span>");
    s.push_str(labels.first().copied().unwrap_or(""));
    s.push_str("</span><span class=\"ramp\">");
    for t in ["--h0", "--h1", "--h2", "--h3", "--h4", "--h5"] {
        let _ = write!(s, "<i style=\"background:var({t})\"></i>");
    }
    s.push_str("</span><span>");
    s.push_str(labels.last().copied().unwrap_or(""));
    s.push_str("</span></div>");
    s
}

fn wrap_doc(title: &str, head_extra: &str, body: &str, fragment: bool) -> String {
    if fragment {
        format!(
            "<title>{}</title>\n{FONTS}\n<style>{CSS}{head_extra}</style>\n{body}",
            esc(title)
        )
    } else {
        format!(
            "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1, viewport-fit=cover\">\n<title>{}</title>\n{FONTS}\n<style>{CSS}{head_extra}</style>\n</head>\n<body>\n{body}\n</body>\n</html>\n",
            esc(title)
        )
    }
}

/// Small line chart as inline SVG.
fn line_chart(
    points: &[(f64, f64)],
    y_max_hint: f64,
    fmt_y: &dyn Fn(f64) -> String,
    events: &[f64],
    color: &str,
) -> String {
    let (w, h) = (300.0, 96.0);
    let (l, r, t, b) = (38.0, 8.0, 8.0, 18.0);
    if points.len() < 2 {
        return "<svg viewBox=\"0 0 300 96\" role=\"img\" aria-label=\"not enough samples\"><text x=\"150\" y=\"52\" text-anchor=\"middle\" font-size=\"11\" fill=\"var(--faint)\">not enough samples</text></svg>".into();
    }
    let x0 = points.first().unwrap().0;
    let x1 = points.last().unwrap().0.max(x0 + 1e-9);
    let ymax = points
        .iter()
        .map(|p| p.1)
        .fold(y_max_hint, f64::max)
        .max(1e-9);
    let sx = |x: f64| l + (x - x0) / (x1 - x0) * (w - l - r);
    let sy = |y: f64| t + (1.0 - y / ymax) * (h - t - b);
    let mut path = String::new();
    for (i, (x, y)) in points.iter().enumerate() {
        let _ = write!(
            path,
            "{}{:.1},{:.1}",
            if i == 0 { "M" } else { "L" },
            sx(*x),
            sy(*y)
        );
    }
    let area = format!(
        "{path}L{:.1},{:.1}L{:.1},{:.1}Z",
        sx(x1),
        sy(0.0),
        sx(x0),
        sy(0.0)
    );
    let mut s = format!("<svg viewBox=\"0 0 {w} {h}\" role=\"img\" preserveAspectRatio=\"none\">");
    // grid lines at 0, 50%, 100%
    for f in [0.0, 0.5, 1.0] {
        let y = sy(ymax * f);
        let _ = write!(
            s,
            "<line x1=\"{l}\" x2=\"{}\" y1=\"{y:.1}\" y2=\"{y:.1}\" stroke=\"var(--hair)\" stroke-width=\"1\"/><text x=\"{}\" y=\"{:.1}\" text-anchor=\"end\" font-size=\"9\" fill=\"var(--faint)\">{}</text>",
            w - r,
            l - 4.0,
            y + 3.0,
            esc(&fmt_y(ymax * f))
        );
    }
    for e in events {
        if *e >= x0 && *e <= x1 {
            let x = sx(*e);
            let _ = write!(
                s,
                "<line x1=\"{x:.1}\" x2=\"{x:.1}\" y1=\"{t}\" y2=\"{}\" stroke=\"var(--accent)\" stroke-dasharray=\"3 3\" stroke-width=\"1\"/>",
                h - b
            );
        }
    }
    let _ = write!(
        s,
        "<path d=\"{area}\" fill=\"{color}\" fill-opacity=\"0.14\" stroke=\"none\"/>"
    );
    let _ = write!(
        s,
        "<path d=\"{path}\" fill=\"none\" stroke=\"{color}\" stroke-width=\"1.6\" vector-effect=\"non-scaling-stroke\"/>"
    );
    let (lx, ly) = points.last().unwrap();
    let _ = write!(
        s,
        "<circle cx=\"{:.1}\" cy=\"{:.1}\" r=\"2.6\" fill=\"{color}\"/>",
        sx(*lx),
        sy(*ly)
    );
    let _ = write!(
        s,
        "<text x=\"{l}\" y=\"{}\" font-size=\"9\" fill=\"var(--faint)\">{:.0}s</text><text x=\"{}\" y=\"{}\" text-anchor=\"end\" font-size=\"9\" fill=\"var(--faint)\">{:.0}s</text></svg>",
        h - 4.0,
        x0,
        w - r,
        h - 4.0,
        x1
    );
    s
}

fn hotspot_html(h: &Hotspot) -> String {
    let mut s = String::new();
    let sev = format!("{:?}", h.severity);
    let _ = write!(
        s,
        "<article class=\"hot {sev}\"><div class=\"stripe\"></div><div class=\"body\"><div class=\"title\"><span class=\"sev {sev}\">{}</span><h3>{}</h3><span class=\"cat\">{}</span></div><p>{}</p>",
        h.severity.as_str(),
        esc(&h.title),
        esc(&h.category),
        esc(&h.detail)
    );
    if !h.evidence.is_empty() {
        s.push_str("<ul>");
        for e in &h.evidence {
            let _ = write!(s, "<li>{}</li>", esc(e));
        }
        s.push_str("</ul>");
    }
    if !h.metrics.is_empty() {
        s.push_str("<div class=\"watch\"><span class=\"muted\">Watch</span>");
        for m in &h.metrics {
            let _ = write!(s, "<code>{}</code>", esc(m));
        }
        s.push_str("</div>");
    }
    if !h.knobs.is_empty() {
        s.push_str("<div class=\"tbl\" style=\"border:none;background:transparent\"><table class=\"knobs\"><tbody>");
        for k in &h.knobs {
            let _ = write!(
                s,
                "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
                esc(&k.key),
                esc(&k.current),
                esc(&k.hint)
            );
        }
        s.push_str("</tbody></table></div>");
    }
    s.push_str("</div></article>");
    s
}

fn table(head: &[&str], numeric_from: usize, rows: &[Vec<String>], mono_first: bool) -> String {
    let mut s = String::from("<div class=\"tbl\"><table class=\"data\"><thead><tr>");
    for (i, h) in head.iter().enumerate() {
        let _ = write!(
            s,
            "<th{}>{}</th>",
            if i >= numeric_from {
                " class=\"n\""
            } else {
                ""
            },
            esc(h)
        );
    }
    s.push_str("</tr></thead><tbody>");
    for r in rows {
        s.push_str("<tr>");
        for (i, c) in r.iter().enumerate() {
            let cls = if i >= numeric_from {
                " class=\"n\""
            } else if i == 0 && mono_first {
                " class=\"m\""
            } else {
                ""
            };
            let _ = write!(s, "<td{cls}>{}</td>", esc(c));
        }
        s.push_str("</tr>");
    }
    s.push_str("</tbody></table></div>");
    s
}

pub fn render_run(r: &RunResult) -> String {
    render_run_doc(r, false)
}

pub fn render_run_doc(r: &RunResult, fragment: bool) -> String {
    let mut b = String::from("<div class=\"page\">");
    // header
    let rep = &r.config.replicas;
    let _ = write!(
        b,
        "<header class=\"top\"><span class=\"eyebrow\">Temporal {} capacity simulation</span><h1>{}</h1><div class=\"chips\">",
        esc(&r.temporal_version),
        esc(&r.scenario)
    );
    for svc in ["frontend", "history", "matching", "worker"] {
        let _ = write!(
            b,
            "<span class=\"chip\">{svc} <b>{}</b> × {} CPU</span>",
            rep.get(svc).unwrap_or(&0),
            r.config.cpu.get(svc).copied().unwrap_or(0.0)
        );
    }
    let _ = write!(
        b,
        "<span class=\"chip\"><b>{}</b> history shards</span><span class=\"chip\">{} · capacity <b>{}</b></span><span class=\"chip\">client LB <b>{}</b></span><span class=\"chip\">{:.0}s simulated after {:.0}s warm-up</span>",
        r.config.num_history_shards,
        esc(&r.persistence.store),
        r.persistence.capacity,
        esc(&r.config.client_lb),
        r.duration_s,
        r.warmup_s
    );
    if !r.label.is_empty() {
        let _ = write!(b, "<span class=\"chip k\">{}</span>", esc(&r.label));
    }
    b.push_str("</div></header>");

    // verdict
    let crit = r
        .hotspots
        .iter()
        .filter(|h| h.severity == Severity::Critical)
        .count();
    let warn = r
        .hotspots
        .iter()
        .filter(|h| h.severity == Severity::Warning)
        .count();
    let _ = write!(
        b,
        "<section class=\"verdict\" aria-label=\"verdict\"><div class=\"count\">{}{}{}</div><p>{}</p></section>",
        if crit > 0 {
            format!("<div class=\"tally c\"><b>{crit}</b><span>critical</span></div>")
        } else {
            String::new()
        },
        if warn > 0 {
            format!("<div class=\"tally w\"><b>{warn}</b><span>warning</span></div>")
        } else {
            String::new()
        },
        if crit + warn == 0 {
            "<div class=\"tally o\"><b>0</b><span>hotspots</span></div>".to_string()
        } else {
            String::new()
        },
        esc(&r.headline)
    );

    // hotspots
    b.push_str("<section><div class=\"section-head\"><h2>Hotspots</h2><span class=\"muted\">ranked by severity, then impact</span></div>");
    if r.hotspots.is_empty() {
        b.push_str(
            "<p class=\"muted\">No resource crossed the warning thresholds at this load.</p>",
        );
    }
    for h in &r.hotspots {
        b.push_str(&hotspot_html(h));
    }
    b.push_str("</section>");

    // cluster map
    b.push_str("<section><div class=\"section-head\"><h2>Cluster map</h2><span class=\"muted\">CPU per pod · shard IO load by owner</span></div><div class=\"grid2\">");
    b.push_str("<div class=\"panel\"><h3>Pod CPU</h3><div class=\"bars\">");
    for s in &r.services {
        let _ = write!(
            b,
            "<div class=\"svc\">{} · {} pods</div>",
            esc(&s.service),
            s.replicas
        );
        for p in s.pods.iter().filter(|p| p.alive) {
            let owns = match s.service.as_str() {
                "history" => format!("{} shards", p.owned),
                "matching" => format!("{} partitions", p.owned),
                "frontend" if r.config.client_lb == "proxy" => "clients via proxy".to_string(),
                "frontend" => format!("{} client connections", p.owned),
                _ => format!("{} per-namespace workers", p.owned),
            };
            let _ = write!(
                b,
                "<div class=\"bar\" title=\"{} · {}\"><span class=\"name\">{}</span><span class=\"track\"><span class=\"fill\" style=\"display:block;width:{:.1}%;background:{}\"></span></span><span class=\"val\">{}</span><span class=\"sub\">{} · {} · {} · {}</span></div>",
                esc(&p.addr),
                esc(&owns),
                esc(&p.name),
                (p.cpu_util * 100.0).min(100.0),
                heat(p.cpu_util).replace("--h0", "--h1"),
                fmt_pct(p.cpu_util),
                esc(&owns),
                fmt_rate(p.requests_per_s),
                p.top_limit()
                    .map(|(n, u)| format!("{} at {} of limit", esc(n), fmt_pct(u)))
                    .unwrap_or_else(|| "no limiter".into()),
                if p.rejections > 0 {
                    format!("{} rejected", p.rejections)
                } else {
                    "no rejections".into()
                }
            );
        }
    }
    b.push_str("</div></div>");
    // shard map
    b.push_str("<div class=\"panel\"><h3>History shards by owner</h3>");
    let _ = write!(
        b,
        "<p class=\"muted\" style=\"font-size:13px\">Each square is one shard, coloured by how busy its IO semaphore was (<code>history.shardIOConcurrency</code> = {}). Hottest: shard {} at {}.</p>",
        r.history.shard_io_concurrency,
        r.history
            .hot_shards
            .first()
            .map(|s| s.shard.to_string())
            .unwrap_or_default(),
        r.history
            .hot_shards
            .first()
            .map(|s| fmt_pct(s.io_util))
            .unwrap_or_default()
    );
    b.push_str(&legend_ramp(&["idle", "saturated"]));
    b.push_str("<div class=\"shardmap\">");
    let cell = if r.history.num_shards <= 1024 {
        9
    } else if r.history.num_shards <= 4096 {
        5
    } else {
        3
    };
    for (host, shards) in &r.shard_map {
        let _ = write!(
            b,
            "<div class=\"shardhost\"><span class=\"lbl\">{} · {} shards</span><div class=\"cells\" style=\"--cell:{cell}px\">",
            esc(host),
            shards.len()
        );
        for (id, u) in shards {
            let _ = write!(
                b,
                "<i style=\"background:{}\" title=\"shard {id}: {}\"></i>",
                heat(*u),
                fmt_pct(*u)
            );
        }
        b.push_str("</div></div>");
    }
    b.push_str("</div></div></div></section>");

    // time series
    if r.samples.len() >= 2 {
        let events: Vec<f64> = r
            .notes
            .iter()
            .filter_map(|n| {
                n.strip_prefix("t=")
                    .and_then(|x| x.split('s').next())
                    .and_then(|x| x.parse().ok())
            })
            .collect();
        let dt = if r.samples.len() >= 2 {
            (r.samples[1].t - r.samples[0].t).max(1e-9)
        } else {
            1.0
        };
        let series = |f: &dyn Fn(&crate::model::metrics::Sample) -> f64| -> Vec<(f64, f64)> {
            r.samples.iter().map(|s| (s.t, f(s))).collect()
        };
        let fmax = |v: &[f64]| v.iter().cloned().fold(0.0, f64::max);
        b.push_str("<section><div class=\"section-head\"><h2>Over time</h2><span class=\"muted\">5 s samples; dashed lines mark scenario events</span></div><div class=\"spark\">");
        type Chart<'a> = (
            &'a str,
            Vec<(f64, f64)>,
            f64,
            Box<dyn Fn(f64) -> String>,
            &'a str,
        );
        let charts: Vec<Chart<'_>> = vec![
            (
                "Workflows completed /s",
                series(&|s| s.completed as f64 / dt),
                1.0,
                Box::new(num),
                "var(--accent)",
            ),
            (
                "Max history CPU",
                series(&|s| fmax(&s.cpu[1])),
                1.0,
                Box::new(|v: f64| fmt_pct(v)),
                "var(--h4)",
            ),
            (
                "Max frontend CPU",
                series(&|s| fmax(&s.cpu[0])),
                1.0,
                Box::new(|v: f64| fmt_pct(v)),
                "var(--h4)",
            ),
            (
                "Database busy",
                series(&|s| s.db_util),
                1.0,
                Box::new(|v: f64| fmt_pct(v)),
                "var(--info)",
            ),
            (
                "Matching backlog (tasks)",
                series(&|s| s.backlog as f64),
                10.0,
                Box::new(num),
                "var(--warn)",
            ),
            (
                "Rate-limit rejections /s",
                series(&|s| s.rejections as f64 / dt),
                1.0,
                Box::new(num),
                "var(--crit)",
            ),
        ];
        for (title, pts, hint, f, color) in charts {
            let last = pts.last().map(|p| p.1).unwrap_or(0.0);
            let _ = write!(
                b,
                "<figure><figcaption><span>{}</span><b>{}</b></figcaption>{}</figure>",
                esc(title),
                esc(&f(last)),
                line_chart(&pts, hint, &*f, &events, color)
            );
        }
        b.push_str("</div></section>");
    }

    // detail tables
    b.push_str("<section><div class=\"section-head\"><h2>Detail</h2><span class=\"muted\">client-observed latency includes SDK retries</span></div>");
    let wf_rows: Vec<Vec<String>> = r
        .workflows
        .iter()
        .filter(|w| w.started_per_s > 0.0 || w.offered_start_rate > 0.0)
        .map(|w| {
            vec![
                w.workflow_type.clone(),
                fmt_rate(w.offered_start_rate),
                fmt_rate(w.started_per_s),
                fmt_rate(w.completed_per_s),
                ms(w.e2e.p50_ms),
                ms(w.e2e.p99_ms),
                ms(w.wft_schedule_to_start.p99_ms),
                ms(w.activity_schedule_to_start.p99_ms),
                fmt_pct(w.sticky_hit_ratio),
                w.wft_timeouts.to_string(),
            ]
        })
        .collect();
    b.push_str(&table(
        &[
            "workflow type",
            "offered",
            "started",
            "completed",
            "e2e p50",
            "e2e p99",
            "WFT s2s p99",
            "act s2s p99",
            "sticky hit",
            "WFT timeouts",
        ],
        1,
        &wf_rows,
        false,
    ));
    let api_rows: Vec<Vec<String>> = r
        .apis
        .iter()
        .map(|a| {
            vec![
                a.api.clone(),
                fmt_rate(a.per_s),
                ms(a.latency.p50_ms),
                ms(a.latency.p95_ms),
                ms(a.latency.p99_ms),
                if a.errors.is_empty() {
                    "–".into()
                } else {
                    a.errors
                        .iter()
                        .map(|(k, v)| format!("{k} {v}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                },
            ]
        })
        .collect();
    b.push_str(&table(
        &["API", "rate", "p50", "p95", "p99", "errors"],
        1,
        &api_rows,
        true,
    ));

    let op_rows: Vec<Vec<String>> = r
        .persistence
        .ops
        .iter()
        .map(|o| {
            vec![
                o.op.clone(),
                fmt_rate(o.per_s),
                ms(o.latency.p50_ms),
                ms(o.latency.p99_ms),
                o.rejected.to_string(),
            ]
        })
        .collect();
    let _ = write!(
        b,
        "<details><summary><span>Persistence · database {} busy</span></summary><div class=\"inner\">{}</div></details>",
        fmt_pct(r.persistence.utilization),
        table(
            &["operation", "rate", "p50", "p99", "rejected"],
            1,
            &op_rows,
            true
        )
    );
    let task_rows: Vec<Vec<String>> = r
        .history
        .tasks
        .iter()
        .map(|t| {
            vec![
                t.task_type.clone(),
                fmt_rate(t.per_s),
                fmt_pct(t.noop_fraction),
                ms(t.load.p99_ms),
                ms(t.schedule.p99_ms),
                ms(t.processing.p99_ms),
                ms(t.queue.p99_ms),
                format!("{:.2}", t.mean_attempts),
            ]
        })
        .collect();
    let _ = write!(
        b,
        "<details><summary><span>History task queues · lock wait p99 {} · mutable-state cache hit {}</span></summary><div class=\"inner\">{}</div></details>",
        ms(r.history.lock_wait.p99_ms),
        fmt_pct(r.history.cache_hit_ratio),
        table(
            &[
                "task type",
                "rate",
                "no-op",
                "load p99",
                "schedule p99",
                "processing p99",
                "end-to-end p99",
                "attempts"
            ],
            1,
            &task_rows,
            true
        )
    );
    let mut parts: Vec<&PartitionResult> = r
        .matching
        .partitions
        .iter()
        .filter(|p| !p.partition.starts_with("sticky"))
        .collect();
    parts.sort_by(|a, b| b.adds_per_s.partial_cmp(&a.adds_per_s).unwrap());
    let part_rows: Vec<Vec<String>> = parts
        .iter()
        .map(|p| {
            vec![
                format!("{} {} p{}", p.task_queue, p.kind, p.partition),
                p.host.clone(),
                fmt_rate(p.adds_per_s),
                fmt_rate(p.polls_per_s),
                fmt_pct(p.sync_match_ratio),
                format!("{:.0}", p.backlog_mean),
                format!("{:.1}", p.pollers_mean),
                ms(p.task_wait.p99_ms),
                format!("{}/{}", p.forwarded_tasks, p.forwarded_polls),
            ]
        })
        .collect();
    let _ = write!(
        b,
        "<details><summary><span>Matching partitions · sync match {}</span></summary><div class=\"inner\">{}</div></details>",
        fmt_pct(r.matching.sync_match_ratio),
        table(
            &[
                "partition",
                "host",
                "adds",
                "polls",
                "sync",
                "backlog",
                "pollers",
                "dispatch p99",
                "fwd tasks/polls"
            ],
            2,
            &part_rows,
            true
        )
    );
    if !r.limits.is_empty() {
        let rows: Vec<Vec<String>> = r
            .limits
            .iter()
            .map(|l| {
                vec![
                    l.limiter.clone(),
                    l.place.clone(),
                    l.rejected.to_string(),
                    fmt_rate(l.per_s),
                ]
            })
            .collect();
        let _ = write!(
            b,
            "<details open><summary><span>Rate-limit rejections</span></summary><div class=\"inner\">{}</div></details>",
            table(&["limiter", "where", "rejected", "rate"], 2, &rows, true)
        );
    }
    if !r.validation.is_empty() {
        let rows: Vec<Vec<String>> = r
            .validation
            .iter()
            .map(|v| {
                vec![
                    v.metric.clone(),
                    format!("{:.3}", v.observed),
                    format!("{:.3}", v.simulated),
                    format!("{:.2}", v.ratio),
                ]
            })
            .collect();
        let _ = write!(
            b,
            "<details open><summary><span>Validation against observed metrics</span></summary><div class=\"inner\">{}</div></details>",
            table(
                &["metric", "observed", "simulated", "sim / obs"],
                1,
                &rows,
                true
            )
        );
    }
    let dc_rows: Vec<Vec<String>> = r
        .config
        .effective_dynamic_config
        .iter()
        .map(|(k, v)| vec![k.clone(), v.clone()])
        .collect();
    let _ = write!(
        b,
        "<details><summary><span>Effective dynamic config ({} modelled keys)</span></summary><div class=\"inner\">{}</div></details>",
        dc_rows.len(),
        table(&["key", "value"], 99, &dc_rows, true)
    );
    b.push_str("</section>");

    if !r.warnings.is_empty() || !r.notes.is_empty() {
        b.push_str("<section><div class=\"section-head\"><h2>Notes</h2></div>");
        if !r.warnings.is_empty() {
            b.push_str("<ul class=\"notes warnings\">");
            for w in &r.warnings {
                let _ = write!(b, "<li>{}</li>", esc(w));
            }
            b.push_str("</ul>");
        }
        b.push_str("<ul class=\"notes\">");
        for n in r.notes.iter().take(80) {
            let _ = write!(b, "<li>{}</li>", esc(n));
        }
        b.push_str("</ul></section>");
    }
    let _ = write!(
        b,
        "<footer>Generated by tempdes · simulated {:.0}s of cluster time in {:.1}s ({} scheduler steps) · simulated metrics use Temporal {} names</footer>",
        r.duration_s,
        r.wall_ms as f64 / 1000.0,
        r.sim_steps,
        esc(&r.temporal_version)
    );
    b.push_str("</div>");
    wrap_doc(&format!("{} capacity", r.scenario), "", &b, fragment)
}

// --- sweep -------------------------------------------------------------------------------------

pub fn render_sweep(r: &SweepResult) -> String {
    render_sweep_doc(r, false)
}

pub fn render_sweep_doc(r: &SweepResult, fragment: bool) -> String {
    let mut b = String::from("<div class=\"page\">");
    let _ = write!(
        b,
        "<header class=\"top\"><span class=\"eyebrow\">Temporal {} capacity sweep</span><h1>{}</h1><div class=\"chips\"><span class=\"chip\">rows: <b>replica counts</b></span><span class=\"chip\">columns: <b>dynamic config</b></span><span class=\"chip\"><b>{}</b> simulations in {:.1}s</span>{}</div></header>",
        esc(&r.temporal_version),
        esc(&r.scenario),
        r.cells.len(),
        r.wall_ms as f64 / 1000.0,
        if r.base.is_empty() {
            String::new()
        } else {
            format!("<span class=\"chip k\">base: {}</span>", esc(&r.base))
        }
    );
    // metric switcher
    b.push_str("<section><div class=\"section-head\"><h2>Replicas × dynamic config</h2><span class=\"muted\">select a cell for its hotspots</span></div>");
    b.push_str("<div class=\"switch\" role=\"group\" aria-label=\"metric\">");
    let metrics = [
        ("status", "Hotspots"),
        ("throughput", "Completed / offered"),
        ("e2e", "Workflow p99"),
        ("start", "Start p99"),
        ("hcpu", "History CPU"),
        ("fcpu", "Frontend CPU"),
        ("db", "Database"),
        ("shard", "Hottest shard"),
        ("rej", "Rejections /s"),
    ];
    for (i, (id, label)) in metrics.iter().enumerate() {
        let _ = write!(
            b,
            "<button type=\"button\" id=\"m-{id}\" data-m=\"{id}\" aria-pressed=\"{}\">{label}</button>",
            i == 0
        );
    }
    b.push_str("</div>");
    b.push_str(&legend_ramp(&["healthy", "saturated / failing"]));
    b.push_str("<div class=\"tbl\" style=\"border:none;background:transparent\"><table class=\"heat\"><thead><tr><th class=\"row\">replicas \\ config</th>");
    for c in &r.cols {
        let _ = write!(b, "<th>{}</th>", esc(c));
    }
    b.push_str("</tr></thead><tbody>");
    for (ri, rl) in r.rows.iter().enumerate() {
        let _ = write!(b, "<tr><th class=\"row\">{}</th>", esc(rl));
        for ci in 0..r.cols.len() {
            let idx = ri * r.cols.len() + ci;
            let _ = write!(
                b,
                "<td><button type=\"button\" id=\"cell-{idx}\" data-i=\"{idx}\" aria-pressed=\"false\"><span class=\"v\"></span><span class=\"s\"></span></button></td>"
            );
        }
        b.push_str("</tr>");
    }
    b.push_str("</tbody></table></div></section>");
    b.push_str("<section class=\"detail\" id=\"detail\" aria-live=\"polite\"></section>");
    let rows: Vec<Vec<String>> = r
        .cells
        .iter()
        .map(|c| {
            vec![
                c.row_label.clone(),
                c.col_label.clone(),
                c.status().to_string(),
                format!(
                    "{} / {}",
                    fmt_rate(c.completed_per_s),
                    fmt_rate(c.offered_per_s)
                ),
                ms(c.e2e_p99_ms),
                fmt_pct(*c.cpu_max.get("history").unwrap_or(&0.0)),
                fmt_pct(c.db_util),
                fmt_pct(c.shard_io_max),
                fmt_rate(c.rejections_per_s),
            ]
        })
        .collect();
    let _ = write!(
        b,
        "<details><summary><span>All cells</span></summary><div class=\"inner\">{}</div></details>",
        table(
            &[
                "replicas",
                "dynamic config",
                "status",
                "completed / offered",
                "workflow p99",
                "history CPU",
                "database",
                "hottest shard",
                "rejections"
            ],
            3,
            &rows,
            true
        )
    );
    b.push_str("<footer>Generated by tempdes · every cell is an independent deterministic simulation with the same seed</footer></div>");
    // data + script
    let data = serde_json::to_string(&r.cells)
        .unwrap_or_else(|_| "[]".into())
        .replace("</", "<\\/");
    let _ = write!(b, "<script>const CELLS={data};</script>");
    b.push_str(SWEEP_JS);
    wrap_doc(&format!("{} sweep", r.scenario), "", &b, fragment)
}

const SWEEP_JS: &str = r#"<script>
(function(){
  const heat=u=>u<0.05?'var(--h0)':u<0.30?'var(--h1)':u<0.55?'var(--h2)':u<0.75?'var(--h3)':u<0.90?'var(--h4)':'var(--h5)';
  const pct=x=>(x*100).toFixed(x>=0.995&&x<1?1:0)+'%';
  const rate=x=>x===0?'0':x>=10000?(x/1000).toFixed(1)+'k/s':x>=100?x.toFixed(0)+'/s':x>=1?x.toFixed(1)+'/s':x.toFixed(3)+'/s';
  const dur=ms=>ms<1?Math.round(ms*1000)+'µs':ms<1000?(ms<10?ms.toFixed(2):ms<100?ms.toFixed(1):ms.toFixed(0))+'ms':(ms/1000).toFixed(2)+'s';
  const status=c=>c.error?'ERR':c.critical>0?'CRIT':c.warning>0?'WARN':'OK';
  const maxE2E=Math.max(1,...CELLS.map(c=>c.e2e_p99_ms)), minE2E=Math.min(...CELLS.map(c=>c.e2e_p99_ms||Infinity));
  const maxStart=Math.max(1,...CELLS.map(c=>c.start_p99_ms)), minStart=Math.min(...CELLS.map(c=>c.start_p99_ms||Infinity));
  const maxRej=Math.max(1,...CELLS.map(c=>c.rejections_per_s));
  const rel=(v,lo,hi)=>hi>lo?(v-lo)/(hi-lo):0;
  const M={
    status:{v:c=>c.critical+c.warning===0?'OK':c.critical+'c · '+c.warning+'w', u:c=>c.error?1:c.critical>0?0.95:c.warning>0?0.65:0.02},
    throughput:{v:c=>rate(c.completed_per_s), u:c=>c.offered_per_s>0?Math.min(1,Math.max(0,1-c.completed_per_s/c.offered_per_s)*4):0},
    e2e:{v:c=>dur(c.e2e_p99_ms), u:c=>Math.min(1,rel(c.e2e_p99_ms,minE2E,maxE2E))},
    start:{v:c=>dur(c.start_p99_ms), u:c=>Math.min(1,rel(c.start_p99_ms,minStart,maxStart))},
    hcpu:{v:c=>pct(c.cpu_max.history||0), u:c=>c.cpu_max.history||0},
    fcpu:{v:c=>pct(c.cpu_max.frontend||0), u:c=>c.cpu_max.frontend||0},
    db:{v:c=>pct(c.db_util), u:c=>c.db_util},
    shard:{v:c=>pct(c.shard_io_max), u:c=>c.shard_io_max},
    rej:{v:c=>rate(c.rejections_per_s), u:c=>c.rejections_per_s>0?0.35+0.65*c.rejections_per_s/maxRej:0}
  };
  let metric='status', sel=0;
  // default selection: the worst cell (most critical, then most warnings)
  CELLS.forEach((c,i)=>{const w=CELLS[sel]; if((c.critical*100+c.warning)>(w.critical*100+w.warning)) sel=i;});
  const esc=s=>String(s).replace(/[&<>"]/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[ch]));
  function paint(){
    CELLS.forEach((c,i)=>{
      const el=document.getElementById('cell-'+i); if(!el) return;
      const m=M[metric]; const u=c.error?1:m.u(c);
      el.style.background=heat(u);
      el.querySelector('.v').textContent=c.error?'error':m.v(c);
      const s=el.querySelector('.s'); s.textContent=status(c); s.className='s '+status(c);
      el.setAttribute('aria-pressed', i===sel?'true':'false');
      el.title=(c.row_label+' | '+c.col_label+'\n'+(c.top_hotspot||'no hotspots'));
    });
    document.querySelectorAll('.switch button').forEach(bt=>bt.setAttribute('aria-pressed', bt.dataset.m===metric?'true':'false'));
    const c=CELLS[sel]; const d=document.getElementById('detail');
    let h='<div class="section-head"><h2>'+esc(c.row_label)+' · '+esc(c.col_label)+'</h2><span class="muted">'+status(c)+'</span></div>';
    if(c.error){h+='<p>'+esc(c.error)+'</p>'; d.innerHTML=h; return;}
    h+='<p>'+esc(c.headline)+'</p>';
    if(!c.hotspots.length){h+='<p class="muted">No hotspots in this cell.</p>';}
    c.hotspots.forEach(x=>{
      h+='<article class="hot '+x.severity+'"><div class="stripe"></div><div class="body"><div class="title"><span class="sev '+x.severity+'">'+x.severity+'</span><h3>'+esc(x.title)+'</h3><span class="cat">'+esc(x.category)+'</span></div><p>'+esc(x.detail)+'</p>';
      if(x.evidence.length){h+='<ul>'+x.evidence.map(e=>'<li>'+esc(e)+'</li>').join('')+'</ul>';}
      if(x.metrics.length){h+='<div class="watch"><span class="muted">Watch</span>'+x.metrics.map(m=>'<code>'+esc(m)+'</code>').join('')+'</div>';}
      if(x.knobs.length){h+='<table class="knobs"><tbody>'+x.knobs.map(k=>'<tr><td>'+esc(k.key)+'</td><td>'+esc(k.current)+'</td><td>'+esc(k.hint)+'</td></tr>').join('')+'</tbody></table>';}
      h+='</div></article>';
    });
    d.innerHTML=h;
  }
  document.querySelectorAll('.switch button').forEach(bt=>bt.addEventListener('click',()=>{metric=bt.dataset.m; paint();}));
  CELLS.forEach((c,i)=>{const el=document.getElementById('cell-'+i); if(el) el.addEventListener('click',()=>{sel=i; paint();});});
  paint();
})();
</script>"#;
