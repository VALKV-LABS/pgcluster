use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize global tracing subscriber.
///
/// Format is controlled by the `LOG_FORMAT` environment variable:
///   - `json`  → structured JSON (production default)
///   - anything else → pretty human-readable (default in dev)
///
/// Log level is controlled by `RUST_LOG` (default: `info`).
pub fn init() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let format = std::env::var("LOG_FORMAT").unwrap_or_default();

    if format == "json" {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt::layer().pretty())
            .init();
    }
}
