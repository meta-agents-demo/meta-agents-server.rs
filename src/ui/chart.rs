//! Server-rendered SVG chart for the per-agent metacognition timeline:
//! confidence (single series) over time with strategy-shift markers.
//!
//! Mark specs follow the dataviz conventions: 2px line, 8px point markers,
//! hairline grid, muted axis ink, direct label on the latest value (single
//! series, so no legend box — the section title names it). Native SVG
//! `<title>` elements provide per-mark hover tooltips.

use std::fmt::Write;

const W: f64 = 720.0;
const H: f64 = 220.0;
const PAD_L: f64 = 44.0;
const PAD_R: f64 = 56.0;
const PAD_T: f64 = 14.0;
const PAD_B: f64 = 30.0;

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn rel_label(now: u64, at: u64) -> String {
    let d = now.saturating_sub(at);
    if d < 1_000 {
        "now".to_string()
    } else if d < 120_000 {
        format!("-{}s", d / 1_000)
    } else if d < 7_200_000 {
        format!("-{}m", d / 60_000)
    } else {
        format!("-{}h", d / 3_600_000)
    }
}

/// Build the confidence-over-time SVG. `points` are (ms, 0.0..=1.0) in
/// chronological order; `shifts` are (ms, strategy-name) markers.
pub fn confidence_svg(points: &[(u64, f64)], shifts: &[(u64, String)], now: u64) -> String {
    if points.is_empty() {
        return String::new();
    }
    let t_min = points
        .iter()
        .map(|(t, _)| *t)
        .chain(shifts.iter().map(|(t, _)| *t))
        .min()
        .unwrap_or(now);
    let t_max = points
        .iter()
        .map(|(t, _)| *t)
        .chain(shifts.iter().map(|(t, _)| *t))
        .max()
        .unwrap_or(now)
        .max(t_min + 1);
    let span = (t_max - t_min) as f64;
    let x = |t: u64| PAD_L + ((t - t_min) as f64 / span) * (W - PAD_L - PAD_R);
    let y = |c: f64| PAD_T + (1.0 - c.clamp(0.0, 1.0)) * (H - PAD_T - PAD_B);

    let mut s = String::new();
    let _ = write!(
        s,
        r#"<svg viewBox="0 0 {W} {H}" role="img" aria-label="Confidence over time, {n} reports" preserveAspectRatio="xMidYMid meet">"#,
        n = points.len()
    );

    // Hairline grid + y labels at 0 / 50 / 100%.
    for (frac, label) in [(0.0, "0%"), (0.5, "50%"), (1.0, "100%")] {
        let gy = y(frac);
        let _ = write!(
            s,
            r#"<line x1="{PAD_L}" y1="{gy:.1}" x2="{x2}" y2="{gy:.1}" stroke="var(--grid)" stroke-width="1"/>"#,
            x2 = W - PAD_R
        );
        let _ = write!(
            s,
            r#"<text x="{tx}" y="{ty:.1}" text-anchor="end" class="axis-label">{label}</text>"#,
            tx = PAD_L - 8.0,
            ty = gy + 4.0
        );
    }

    // X axis labels: oldest and newest, relative to now.
    let _ = write!(
        s,
        r#"<text x="{PAD_L}" y="{ty}" text-anchor="start" class="axis-label">{l}</text>"#,
        ty = H - 8.0,
        l = rel_label(now, t_min)
    );
    let _ = write!(
        s,
        r#"<text x="{tx}" y="{ty}" text-anchor="end" class="axis-label">{l}</text>"#,
        tx = W - PAD_R,
        ty = H - 8.0,
        l = rel_label(now, t_max)
    );

    // Strategy-shift markers: dashed vertical hairlines with hover titles.
    for (at, strategy) in shifts {
        let sx = x(*at);
        let _ = write!(
            s,
            r#"<g><line x1="{sx:.1}" y1="{PAD_T}" x2="{sx:.1}" y2="{y2}" stroke="var(--baseline)" stroke-width="1" stroke-dasharray="3 3"/><title>strategy → {t}</title></g>"#,
            y2 = H - PAD_B,
            t = esc(strategy)
        );
    }

    // The series: 2px line + 8px markers with native tooltips.
    if points.len() > 1 {
        let mut path = String::new();
        for (i, (t, c)) in points.iter().enumerate() {
            let _ = write!(
                path,
                "{}{:.1},{:.1}",
                if i == 0 { "" } else { " " },
                x(*t),
                y(*c)
            );
        }
        let _ = write!(
            s,
            r#"<polyline points="{path}" fill="none" stroke="var(--series-1)" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>"#
        );
    }
    for (t, c) in points {
        let _ = write!(
            s,
            r#"<g><circle cx="{cx:.1}" cy="{cy:.1}" r="4" fill="var(--series-1)" stroke="var(--surface-1)" stroke-width="2"/><title>{pct:.0}% · {when}</title></g>"#,
            cx = x(*t),
            cy = y(*c),
            pct = c * 100.0,
            when = rel_label(now, *t)
        );
    }

    // Direct label on the latest value (text ink, not series color).
    if let Some((t, c)) = points.last() {
        let _ = write!(
            s,
            r#"<text x="{tx:.1}" y="{ty:.1}" text-anchor="start" class="value-label">{pct:.0}%</text>"#,
            tx = x(*t) + 8.0,
            ty = y(*c) + 4.0,
            pct = c * 100.0
        );
    }

    s.push_str("</svg>");
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_points_render_nothing() {
        assert!(confidence_svg(&[], &[], 0).is_empty());
    }

    #[test]
    fn renders_series_and_shift_markers() {
        let now = 100_000;
        let points = vec![(10_000u64, 0.2), (50_000, 0.6), (90_000, 0.9)];
        let shifts = vec![(50_000u64, "depth-first <search>".to_string())];
        let svg = confidence_svg(&points, &shifts, now);
        assert!(svg.starts_with("<svg"));
        assert!(svg.contains("polyline"));
        assert_eq!(svg.matches("<circle").count(), 3);
        assert!(svg.contains("stroke-dasharray"));
        // Dynamic text is escaped.
        assert!(svg.contains("depth-first &lt;search&gt;"));
        assert!(!svg.contains("depth-first <search>"));
    }
}
