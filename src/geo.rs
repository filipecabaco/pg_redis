//! Redis's GEO geometry: 52-bit geohash scores and the box search around them.
//!
//! A GEO set is a sorted set whose scores are geohashes, so everything here is
//! arithmetic over scores; the commands in `composite.rs` read and write the
//! sorted set with `ZADD` and `ZRANGEBYSCORE`. Ported from `geohash.c`,
//! `geohash_helper.c` and `geo.c` (Redis 7.2) so that scores, hashes,
//! distances and the order results come back in match a real Redis exactly.
#![forbid(unsafe_code)]

pub const STEP_MAX: u8 = 26;
pub const LAT_MIN: f64 = -85.05112878;
pub const LAT_MAX: f64 = 85.05112878;
pub const LONG_MIN: f64 = -180.0;
pub const LONG_MAX: f64 = 180.0;

const D_R: f64 = std::f64::consts::PI / 180.0;
const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;
const MERCATOR_MAX: f64 = 20037726.37;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Bits {
    pub bits: u64,
    pub step: u8,
}

impl Bits {
    fn is_zero(&self) -> bool {
        self.bits == 0 && self.step == 0
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Range {
    min: f64,
    max: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct Area {
    longitude: Range,
    latitude: Range,
}

#[derive(Debug, Clone, Copy)]
pub enum Shape {
    Radius(f64),
    /// Width, height.
    Box(f64, f64),
}

#[derive(Debug, Clone, Copy)]
pub struct Search {
    pub center: [f64; 2],
    pub shape: Shape,
    /// Meters per unit.
    pub conversion: f64,
}

fn interleave64(xlo: u32, ylo: u32) -> u64 {
    const B: [u64; 5] = [
        0x5555555555555555,
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
    ];
    const S: [u32; 5] = [1, 2, 4, 8, 16];
    let (mut x, mut y) = (xlo as u64, ylo as u64);
    for i in (0..5).rev() {
        x = (x | (x << S[i])) & B[i];
        y = (y | (y << S[i])) & B[i];
    }
    x | (y << 1)
}

fn deinterleave64(interleaved: u64) -> u64 {
    const B: [u64; 6] = [
        0x5555555555555555,
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
        0x00000000FFFFFFFF,
    ];
    const S: [u32; 6] = [0, 1, 2, 4, 8, 16];
    let (mut x, mut y) = (interleaved, interleaved >> 1);
    for i in 0..6 {
        x = (x | (x >> S[i])) & B[i];
        y = (y | (y >> S[i])) & B[i];
    }
    x | (y << 32)
}

const WGS84: (Range, Range) = (
    Range {
        min: LONG_MIN,
        max: LONG_MAX,
    },
    Range {
        min: LAT_MIN,
        max: LAT_MAX,
    },
);

fn encode(long_r: Range, lat_r: Range, lon: f64, lat: f64, step: u8) -> Option<Bits> {
    if step > 32 || step == 0 {
        return None;
    }
    if !(LONG_MIN..=LONG_MAX).contains(&lon) || !(LAT_MIN..=LAT_MAX).contains(&lat) {
        return None;
    }
    if lat < lat_r.min || lat > lat_r.max || lon < long_r.min || lon > long_r.max {
        return None;
    }
    let lat_off = (lat - lat_r.min) / (lat_r.max - lat_r.min) * (1u64 << step) as f64;
    let long_off = (lon - long_r.min) / (long_r.max - long_r.min) * (1u64 << step) as f64;
    Some(Bits {
        bits: interleave64(lat_off as u32, long_off as u32),
        step,
    })
}

fn decode(long_r: Range, lat_r: Range, hash: Bits) -> Option<Area> {
    if hash.is_zero() {
        return None;
    }
    let sep = deinterleave64(hash.bits);
    let (ilat, ilon) = (sep as u32 as f64, (sep >> 32) as u32 as f64);
    let div = (1u64 << hash.step) as f64;
    let (lat_s, long_s) = (lat_r.max - lat_r.min, long_r.max - long_r.min);
    Some(Area {
        latitude: Range {
            min: lat_r.min + (ilat / div) * lat_s,
            max: lat_r.min + ((ilat + 1.0) / div) * lat_s,
        },
        longitude: Range {
            min: long_r.min + (ilon / div) * long_s,
            max: long_r.min + ((ilon + 1.0) / div) * long_s,
        },
    })
}

/// A sorted-set score for a position: the 52-bit geohash.
pub fn score_of(lon: f64, lat: f64) -> Option<u64> {
    let h = encode(WGS84.0, WGS84.1, lon, lat, STEP_MAX)?;
    Some(align52(h))
}

/// The position a score stands for: the centre of its geohash cell.
pub fn position_of(score: f64) -> [f64; 2] {
    let area = decode(
        WGS84.0,
        WGS84.1,
        Bits {
            bits: score as u64,
            step: STEP_MAX,
        },
    )
    .expect("a 26-step hash always decodes");
    [
        ((area.longitude.min + area.longitude.max) / 2.0).clamp(LONG_MIN, LONG_MAX),
        ((area.latitude.min + area.latitude.max) / 2.0).clamp(LAT_MIN, LAT_MAX),
    ]
}

fn align52(h: Bits) -> u64 {
    h.bits << (52 - h.step as u32 * 2)
}

/// `GEOHASH`: the standard 11-character geohash, re-encoded over ±90° of
/// latitude rather than the ±85° the scores use.
pub fn geohash_string(score: f64) -> String {
    const ALPHABET: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let [lon, lat] = position_of(score);
    let h = encode(
        Range {
            min: -180.0,
            max: 180.0,
        },
        Range {
            min: -90.0,
            max: 90.0,
        },
        lon,
        lat,
        26,
    )
    .unwrap_or_default();
    (0..11)
        .map(|i| {
            let idx = if i == 10 {
                0
            } else {
                (h.bits >> (52 - (i + 1) * 5)) & 0x1f
            };
            ALPHABET[idx as usize] as char
        })
        .collect()
}

fn move_x(h: &mut Bits, d: i8) {
    if d == 0 {
        return;
    }
    let mut x = h.bits & 0xaaaaaaaaaaaaaaaa;
    let y = h.bits & 0x5555555555555555;
    let zz = 0x5555555555555555u64 >> (64 - h.step as u32 * 2);
    if d > 0 {
        x = x.wrapping_add(zz + 1);
    } else {
        x |= zz;
        x = x.wrapping_sub(zz + 1);
    }
    x &= 0xaaaaaaaaaaaaaaaau64 >> (64 - h.step as u32 * 2);
    h.bits = x | y;
}

fn move_y(h: &mut Bits, d: i8) {
    if d == 0 {
        return;
    }
    let x = h.bits & 0xaaaaaaaaaaaaaaaa;
    let mut y = h.bits & 0x5555555555555555;
    let zz = 0xaaaaaaaaaaaaaaaau64 >> (64 - h.step as u32 * 2);
    if d > 0 {
        y = y.wrapping_add(zz + 1);
    } else {
        y |= zz;
        y = y.wrapping_sub(zz + 1);
    }
    y &= 0x5555555555555555u64 >> (64 - h.step as u32 * 2);
    h.bits = x | y;
}

fn moved(h: Bits, dx: i8, dy: i8) -> Bits {
    let mut n = h;
    move_x(&mut n, dx);
    move_y(&mut n, dy);
    n
}

fn estimate_steps(mut range_meters: f64, lat: f64) -> u8 {
    if range_meters == 0.0 {
        return 26;
    }
    let mut step: i32 = 1;
    while range_meters < MERCATOR_MAX {
        range_meters *= 2.0;
        step += 1;
    }
    step -= 2;
    if !(-66.0..=66.0).contains(&lat) {
        step -= 1;
        if !(-80.0..=80.0).contains(&lat) {
            step -= 1;
        }
    }
    step.clamp(1, 26) as u8
}

fn deg_rad(a: f64) -> f64 {
    a * D_R
}
fn rad_deg(a: f64) -> f64 {
    a / D_R
}

fn lat_distance(lat1: f64, lat2: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (deg_rad(lat2) - deg_rad(lat1)).abs()
}

/// Haversine distance in meters, as `geohashGetDistance`.
pub fn distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let (lon1r, lon2r) = (deg_rad(lon1), deg_rad(lon2));
    let v = ((lon2r - lon1r) / 2.0).sin();
    if v == 0.0 {
        return lat_distance(lat1, lat2);
    }
    let (lat1r, lat2r) = (deg_rad(lat1), deg_rad(lat2));
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().asin()
}

impl Search {
    /// Distance from the centre in meters, or `None` when the point is
    /// outside the shape — Redis's `geoWithinShape`.
    pub fn within(&self, xy: [f64; 2]) -> Option<f64> {
        let [x1, y1] = self.center;
        let [x2, y2] = xy;
        match self.shape {
            Shape::Radius(r) => {
                let d = distance(x1, y1, x2, y2);
                (d <= r * self.conversion).then_some(d)
            }
            Shape::Box(w, h) => {
                let (w, h) = (w * self.conversion, h * self.conversion);
                if lat_distance(y2, y1) > h / 2.0 {
                    return None;
                }
                if distance(x2, y2, x1, y2) > w / 2.0 {
                    return None;
                }
                Some(distance(x1, y1, x2, y2))
            }
        }
    }

    fn bounds(&self) -> [f64; 4] {
        let [lon, lat] = self.center;
        let (half_h, half_w) = match self.shape {
            Shape::Radius(r) => (r, r),
            Shape::Box(w, h) => (h / 2.0, w / 2.0),
        };
        let (height, width) = (self.conversion * half_h, self.conversion * half_w);
        let lat_delta = rad_deg(height / EARTH_RADIUS_IN_METERS);
        let top = rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(lat + lat_delta).cos());
        let bottom = rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(lat - lat_delta).cos());
        let south = lat < 0.0;
        [
            if south { lon - bottom } else { lon - top },
            lat - lat_delta,
            if south { lon + bottom } else { lon + top },
            lat + lat_delta,
        ]
    }

    /// The score ranges to scan, `[min, max)` each: the centre cell and its
    /// eight neighbours, in Redis's order, with the useless ones dropped and
    /// a repeat of the previous one skipped — `geohashCalculateAreasByShapeWGS84`
    /// and `membersOfAllNeighbors`.
    pub fn score_ranges(&self) -> Vec<(u64, u64)> {
        let [min_lon, min_lat, max_lon, max_lat] = self.bounds();
        let [lon, lat] = self.center;
        let radius = match self.shape {
            Shape::Radius(r) => r,
            Shape::Box(w, h) => ((w / 2.0) * (w / 2.0) + (h / 2.0) * (h / 2.0)).sqrt(),
        } * self.conversion;
        let mut steps = estimate_steps(radius, lat);
        let (lr, ar) = WGS84;
        let cells = |steps: u8| {
            let hash = encode(lr, ar, lon, lat, steps).unwrap_or_default();
            let area = decode(lr, ar, hash).unwrap_or_default();
            (hash, area)
        };
        let (mut hash, mut area) = cells(steps);
        let dec = |h: Bits| decode(lr, ar, h).unwrap_or_default();
        let decrease = dec(moved(hash, 0, 1)).latitude.max < max_lat
            || dec(moved(hash, 0, -1)).latitude.min > min_lat
            || dec(moved(hash, 1, 0)).longitude.max < max_lon
            || dec(moved(hash, -1, 0)).longitude.min > min_lon;
        if steps > 1 && decrease {
            steps -= 1;
            (hash, area) = cells(steps);
        }
        // north, south, east, west, NE, NW, SE, SW
        let mut n = [
            moved(hash, 0, 1),
            moved(hash, 0, -1),
            moved(hash, 1, 0),
            moved(hash, -1, 0),
            moved(hash, 1, 1),
            moved(hash, -1, 1),
            moved(hash, 1, -1),
            moved(hash, -1, -1),
        ];
        if steps >= 2 {
            let zero = Bits::default();
            if area.latitude.min < min_lat {
                (n[1], n[7], n[6]) = (zero, zero, zero);
            }
            if area.latitude.max > max_lat {
                (n[0], n[4], n[5]) = (zero, zero, zero);
            }
            if area.longitude.min < min_lon {
                (n[3], n[7], n[5]) = (zero, zero, zero);
            }
            if area.longitude.max > max_lon {
                (n[2], n[6], n[4]) = (zero, zero, zero);
            }
        }
        let all = std::iter::once(hash).chain(n);
        let mut out = Vec::new();
        let mut last: Option<Bits> = None;
        for (i, h) in all.enumerate() {
            if h.is_zero() {
                continue;
            }
            if i > 0 && last == Some(h) {
                continue;
            }
            let min = align52(h);
            let max = align52(Bits {
                bits: h.bits + 1,
                step: h.step,
            });
            out.push((min, max));
            last = Some(h);
        }
        out
    }
}

/// A distance as Redis prints one: four decimal places.
pub fn fmt_distance(d: f64) -> String {
    format!("{d:.4}")
}

/// A coordinate as Redis's `addReplyHumanLongDouble` prints it: 17 decimal
/// places, trailing zeros and a trailing point removed.
pub fn fmt_coord(x: f64) -> String {
    let s = format!("{x:.17}");
    let s = s.trim_end_matches('0');
    s.trim_end_matches('.').to_string()
}

/// Meters per unit, or Redis's error for an unknown unit.
pub fn unit(u: &[u8]) -> Result<f64, String> {
    match String::from_utf8_lossy(u).to_lowercase().as_str() {
        "m" => Ok(1.0),
        "km" => Ok(1000.0),
        "ft" => Ok(0.3048),
        "mi" => Ok(1609.34),
        _ => Err("ERR unsupported unit provided. please use M, KM, FT, MI".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scores_and_positions_match_redis() {
        // `GEOADD Sicily 13.361389 38.115556 Palermo` on Redis 7.2 stores
        // the score 3479099956230698, and GEOPOS reads back these digits.
        let s = score_of(13.361389, 38.115556).unwrap();
        assert_eq!(s, 3479099956230698);
        let [lon, lat] = position_of(s as f64);
        assert_eq!(fmt_coord(lon), "13.36138933897018433");
        assert_eq!(fmt_coord(lat), "38.11555639549629859");
        assert_eq!(geohash_string(s as f64), "sqc8b49rny0");
    }

    #[test]
    fn distance_matches_redis() {
        // GEODIST Sicily Palermo Catania on Redis 7.2.
        let p = position_of(score_of(13.361389, 38.115556).unwrap() as f64);
        let c = position_of(score_of(15.087269, 37.502669).unwrap() as f64);
        assert_eq!(
            fmt_distance(distance(p[0], p[1], c[0], c[1])),
            "166274.1516"
        );
    }

    #[test]
    fn search_walks_cells_in_redis_order() {
        // `GEORADIUS Sicily 15 37 200 km` on Redis 7.2 answers Palermo then
        // Catania unsorted, at 190.4424 and 56.4413 km.
        let members = [
            ("Palermo", score_of(13.361389, 38.115556).unwrap()),
            ("Catania", score_of(15.087269, 37.502669).unwrap()),
        ];
        let search = Search {
            center: [15.0, 37.0],
            shape: Shape::Radius(200.0),
            conversion: 1000.0,
        };
        let mut found = Vec::new();
        for (min, max) in search.score_ranges() {
            let mut in_range: Vec<_> = members
                .iter()
                .filter(|(_, s)| *s >= min && *s < max)
                .collect();
            in_range.sort_by_key(|(_, s)| *s);
            for (name, score) in in_range {
                if let Some(d) = search.within(position_of(*score as f64)) {
                    found.push((*name, fmt_distance(d / 1000.0)));
                }
            }
        }
        assert_eq!(
            found,
            vec![
                ("Palermo", "190.4424".to_string()),
                ("Catania", "56.4413".to_string())
            ]
        );
    }

    #[test]
    fn out_of_range_positions_have_no_score() {
        assert!(score_of(181.0, 0.0).is_none());
        assert!(score_of(0.0, 86.0).is_none());
    }
}
