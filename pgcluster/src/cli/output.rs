use serde::Serialize;

pub enum OutputFormat {
    Table,
    Json,
}

impl OutputFormat {
    pub fn from_flag(json: bool) -> Self {
        if json {
            Self::Json
        } else {
            Self::Table
        }
    }
}

pub fn print_json<T: Serialize>(value: &T) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_default()
    );
}

pub fn print_kv(pairs: &[(&str, &str)]) {
    let max_key = pairs.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
    for (k, v) in pairs {
        println!("{:<width$}  {}", k, v, width = max_key);
    }
}

pub fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    let widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            rows.iter()
                .map(|r| r.get(i).map(|s| s.len()).unwrap_or(0))
                .max()
                .unwrap_or(0)
                .max(h.len())
        })
        .collect();

    let row_str = |cells: &[&str]| -> String {
        cells
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{:<width$}", c, width = widths[i]))
            .collect::<Vec<_>>()
            .join("  ")
    };

    let hdr: Vec<&str> = headers.to_vec();
    println!("{}", row_str(&hdr));
    println!(
        "{}",
        widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("  ")
    );
    for row in rows {
        let cells: Vec<&str> = row.iter().map(|s| s.as_str()).collect();
        println!("{}", row_str(&cells));
    }
}
