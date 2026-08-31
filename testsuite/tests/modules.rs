//! Contains the tests that are stored in additional modules.

mod parser;

fn sanitize_snapshot_name(name: &str) -> String {
    let mut sanitized = String::with_capacity(name.len());

    for c in name.chars() {
        match c {
            '*' => sanitized.push_str("star"),
            '&' => sanitized.push_str("amp"),
            '|' => sanitized.push_str("pipe"),
            c if c.is_ascii_alphanumeric() => sanitized.push(c),
            _ => sanitized.push('_'),
        }
    }

    sanitized
}
