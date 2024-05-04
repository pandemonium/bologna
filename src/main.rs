use std::{fmt, fs, str, thread};
mod hashish;

//type StatTable<'a> = rustc_hash::FxHashMap<&'a str, Stat>;
type StatTable<'a> = hashish::Table<14813, &'a str, Stat>;

#[derive(Debug, Default)]
struct StatChunk<'a> {
    data: StatTable<'a>,
}

impl<'a> StatChunk<'a> {
    #[inline]
    fn merge_with(&mut self, StatChunk { data }: StatChunk<'a>) {
        //        for (city, stat) in &data {
        //            self.data.entry(city).or_default().merge_with(&stat);
        //        }
        for (city, stat) in data.iter() {
            self.data.emplace(&city).merge_with(&stat)
        }
    }
}

impl<'a> fmt::Display for StatChunk<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entries = self.data.iter().collect::<Vec<_>>();
        entries.sort_by_cached_key(|(key, _)| *key);

        write!(f, "{{")?;
        for (city, stat) in entries {
            write!(f, "{city}={stat},")?;
        }
        write!(f, "}}")?;

        Ok(())
    }
}

#[derive(Copy, Clone, Debug)]
struct Stat {
    min: f32,
    sum: f32,
    count: u32,
    max: f32,
}

impl Stat {
    const DEFAULT_INSTANCE: Self = Self {
        min: f32::MAX,
        sum: 0.0,
        count: 0,
        max: f32::MIN,
    };

    #[inline]
    fn add(&mut self, x: f32) {
        self.min = if self.min < x { self.min } else { x };
        self.sum += x;
        self.count += 1;
        self.max = if self.max > x { self.max } else { x };
    }

    #[inline]
    fn merge_with(&mut self, rhs: &Self) {
        self.min = f32::min(self.min, rhs.min);
        self.sum += rhs.sum;
        self.count += rhs.count;
        self.max = f32::max(self.max, rhs.max);
    }

    fn average(&self) -> f32 {
        self.sum / (self.count as f32)
    }
}

impl Default for Stat {
    #[inline]
    fn default() -> Self {
        Self::DEFAULT_INSTANCE
    }
}

impl fmt::Display for Stat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{:.1}/{}", self.min, self.average(), self.max)
    }
}

#[inline]
fn aggregate_chunk<'a>(chunk: &'a [u8]) -> StatChunk<'a> {
    //    let mut stat_map = StatTable::with_capacity_and_hasher(5003, Default::default());
    let mut stat_map = StatTable::new();
    let mut cursor = chunk;

    loop {
        let mut city_pos = 3;
        while city_pos < cursor.len() && cursor[city_pos] != b';' {
            city_pos += 1
        }

        if city_pos < cursor.len() {
            let city = unsafe { str::from_utf8_unchecked(&cursor[..city_pos]) };
            let (temperature, remains) = parse_temperature(&cursor[(city_pos + 1)..]);
            //            stat_map.entry(city).or_default().add(temperature);
            stat_map.emplace(city).add(temperature);
            cursor = remains;
        } else {
            break StatChunk { data: stat_map };
        }
    }
}

fn chunkify<'a>(extent: &'a [u8], count: usize) -> Vec<&'a [u8]> {
    let mut chunks = Vec::with_capacity(count);
    let extent_size = extent.len();
    let chunk_size = extent_size / count;
    let mut base = 0;
    let mut offset = chunk_size;

    for _ in 0..count {
        while offset < extent_size && extent[offset] != b'\n' {
            offset += 1;
        }

        chunks.push(&extent[base..(offset + 1)]);
        base = offset + 1;
        offset += usize::min(chunk_size, extent_size - base);
    }

    chunks
}

#[inline]
fn parse_temperature<'a>(image: &'a [u8]) -> (f32, &'a [u8]) {
    let mut float;
    let neg = image[0] == b'-';

    let index = if neg {
        float = (image[1] - b'0') as f32;
        2
    } else {
        float = (image[0] - b'0') as f32;
        1
    };

    let remains = if image[index] == b'.' {
        float += (image[index + 1] - b'0') as f32 / 10.0;
        &image[(index + 3)..]
    } else {
        float = 10.0 * float + (image[index] - b'0') as f32;
        float += (image[index + 2] - b'0') as f32 / 10.0;
        &image[(index + 4)..]
    };

    if neg {
        (-float, remains)
    } else {
        (float, remains)
    }
}

fn main() {
    let file = fs::File::open("measurements.txt").unwrap();
    let data = unsafe { memmap::Mmap::map(&file).unwrap() };

    let stat = thread::scope(|s| {
        let count = thread::available_parallelism().unwrap().get();
        let chunk_count = count * 3;

        chunkify(&data, chunk_count)
            .iter()
            .map(|chunk| s.spawn(|| aggregate_chunk(chunk)))
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .reduce(|mut p, q| {
                p.merge_with(q);
                p
            })
            .unwrap()
    });

    println!("{stat}");
}
