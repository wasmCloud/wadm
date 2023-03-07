use async_nats::jetstream::{
    self,
    stream::{Config, Stream},
    Context,
};
use clap::Parser;
use futures::StreamExt;
use opentelemetry::sdk::{
    trace::{IdGenerator, Sampler},
    Resource,
};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use tracing::{debug, error, trace, warn, Event as TracingEvent, Subscriber};
use tracing_subscriber::fmt::{
    format::{Format, Full, Json, Writer},
    time::SystemTime,
    FmtContext, FormatEvent, FormatFields,
};
use tracing_subscriber::{
    layer::{Layered, SubscriberExt},
    registry::LookupSpan,
    EnvFilter, Layer, Registry,
};

use wadm::consumers::*;
use wadm::{
    commands::*, events::*, DEFAULT_COMMANDS_TOPIC, DEFAULT_EVENTS_TOPIC, DEFAULT_EXPIRY_TIME,
};

#[derive(Parser, Debug)]
#[command(name = clap::crate_name!(), version = clap::crate_version!(), about = "wasmCloud Application Deployment Manager", long_about = None)]
struct Args {
    /// The ID for this wadm process. Defaults to a random UUIDv4 if none is provided. This is used
    /// to help with debugging when identifying which process is doing the work
    #[arg(short = 'i', long = "host-id", env = "WADM_HOST_ID")]
    host_id: Option<String>,

    /// Whether or not to use structured log output (as JSON)
    #[arg(
        short = 's',
        long = "structured-logging",
        default_value = "false",
        env = "WADM_STRUCTURED_LOGGING"
    )]
    structured_logging: bool,

    /// Whether or not to enable opentelemetry tracing
    #[arg(
        short = 't',
        long = "tracing",
        default_value = "false",
        env = "WADM_TRACING_ENABLED"
    )]
    tracing_enabled: bool,

    /// The endpoint to use for tracing. Setting this flag enables tracing, even if --tracing is set
    /// to false. Defaults to http://localhost:55681/v1/traces if not set and tracing is enabled
    #[arg(short = 'e', long = "tracing-endpoint", env = "WADM_TRACING_ENDPOINT")]
    tracing_endpoint: Option<String>,

    /// Name of the events stream to use
    #[arg(
        long = "event-stream",
        default_value = "wadm_events",
        env = "WADM_EVENT_STREAM"
    )]
    event_stream_name: String,

    /// Name of the commands stream to use
    #[arg(
        long = "command-stream",
        default_value = "wadm_commands",
        env = "WADM_COMMAND_STREAM"
    )]
    command_stream_name: String,

    /// The NATS JetStream domain to connect to
    #[arg(short = 'd', env = "WADM_JETSTREAM_DOMAIN")]
    domain: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    configure_tracing(
        args.structured_logging,
        args.tracing_enabled,
        args.tracing_endpoint,
    );

    // TODO: All the NATS connection options and jetstream stuff
    let client = async_nats::connect("127.0.0.1:4222").await?;
    let context = if let Some(domain) = args.domain {
        jetstream::with_domain(client.clone(), domain)
    } else {
        jetstream::new(client.clone())
    };

    let event_stream = context
        .get_or_create_stream(Config {
            name: args.event_stream_name,
            description: Some(
                "A stream that stores all events coming in on the wasmbus.evt topics in a cluster"
                    .to_string(),
            ),
            num_replicas: 1,
            retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
            subjects: vec![DEFAULT_EVENTS_TOPIC.to_owned()],
            max_age: DEFAULT_EXPIRY_TIME,
            storage: async_nats::jetstream::stream::StorageType::File,
            allow_rollup: false,
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    let command_stream = context
        .get_or_create_stream(Config {
            name: args.command_stream_name,
            description: Some("A stream that stores all commands for wadm".to_string()),
            num_replicas: 1,
            retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
            subjects: vec![DEFAULT_COMMANDS_TOPIC.to_owned()],
            max_age: DEFAULT_EXPIRY_TIME,
            storage: async_nats::jetstream::stream::StorageType::File,
            allow_rollup: false,
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    let host_id = args
        .host_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    tokio::select! {
        res = handle_events(event_stream, context.clone(), host_id) => {
            res?
        }
        res = handle_commands(command_stream) => {
            res?
        }
    }
    Ok(())
}

// NOTE: These functions will probably need to be replaced with something in the library code, but
// they're good enough for testing
async fn handle_events(stream: Stream, context: Context, host_id: String) -> anyhow::Result<()> {
    let mut consumer = EventConsumer::new(stream, "wasmbus.evt.default")
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    while let Some(res) = consumer.next().await {
        match res {
            Err(e) => {
                error!(error = %e, "Error when trying to fetch message from consumer");
                continue;
            }
            Ok(mut evt) => {
                debug!(event = ?evt.as_ref(), "Handling received event");
                // THIS IS WHERE WE'D DO REAL WORK
                if let Err(e) = evt.ack().await {
                    warn!(error = %e, "Error when acking");
                    // If we can't ack, loop back around to try on the redelivery
                    continue;
                }

                // NOTE: There is a possible race condition here where we send the command, and it
                // doesn't work even though we've acked the message. Worst case here is that we end
                // up not starting/creating something, which would be fixed on the next heartbeat.
                // This is better than the other option of double starting something when the ack
                // fails (think if something resulted in starting 100 actors on a host and then it
                // did it again)
                if let Err(e) = send_fake_command(&context, &host_id, &evt).await {
                    error!(error = %e, "Got error when sending command, will have to catch up on next reconcile");
                }
            }
        }
    }
    Ok(())
}

// For testing only, to send a fake command in response to an event
async fn send_fake_command(context: &Context, host_id: &str, event: &Event) -> anyhow::Result<()> {
    use wadm::nats_utils::ensure_send;
    let command: Command = match event {
        Event::ActorStarted(actor) => {
            StartActor {
                reference: actor.image_ref.clone(),
                // So we know where it came from
                host_id: host_id.to_owned(),
                count: 2,
            }
            .into()
        }
        Event::ProviderStopped(prov) => {
            StopProvider {
                // So we know where it came from
                contract_id: prov.contract_id.clone(),
                host_id: host_id.to_owned(),
                provider_id: prov.public_key.clone(),
                link_name: Some(prov.link_name.clone()),
            }
            .into()
        }
        Event::LinkdefSet(ld) => {
            PutLinkdef {
                // So we know where it came from
                contract_id: ld.linkdef.contract_id.clone(),
                actor_id: ld.linkdef.actor_id.clone(),
                provider_id: ld.linkdef.provider_id.clone(),
                link_name: ld.linkdef.link_name.clone(),
                values: vec![("wadm_host".to_string(), host_id.to_owned())]
                    .into_iter()
                    .collect(),
            }
            .into()
        }
        _ => {
            StopActor {
                // So we know where it came from
                actor_id: host_id.to_owned(),
                host_id: "notreal".to_string(),
                count: 2,
            }
            .into()
        }
    };
    trace!(?command, "Sending command");
    ensure_send(context, "wadm.cmd.default".to_string(), &command).await
}

async fn handle_commands(stream: Stream) -> anyhow::Result<()> {
    let mut consumer = CommandConsumer::new(stream, "wadm.cmd.default")
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    while let Some(res) = consumer.next().await {
        match res {
            Err(e) => {
                error!(error = %e, "Error when trying to fetch message from consumer");
                continue;
            }
            Ok(mut cmd) => {
                debug!(event = ?cmd.as_ref(), "Handling received command");
                if let Err(e) = cmd.ack().await {
                    warn!(error = %e, "Error when acking");
                    // If we can't ack, loop back around to try on the redelivery
                    continue;
                }

                // NOTE: There is a possible race condition here where we send the lattice control
                // message, and it doesn't work even though we've acked the message. Worst case here
                // is that we end up not starting/creating something, which would be fixed on the
                // next heartbeat. This is better than the other option of double starting something
                // when the ack fails (think if something resulted in starting 100 actors on a host
                // and then it did it again)

                // THIS IS WHERE WE'D DO REAL WORK
                trace!("I'm sending something to the lattice control topics!");
            }
        }
    }
    Ok(())
}

const TRACING_PATH: &str = "/v1/traces";

/// A struct that allows us to dynamically choose JSON formatting without using dynamic dispatch.
/// This is just so we avoid any sort of possible slow down in logging code
enum JsonOrNot {
    Not(Format<Full, SystemTime>),
    Json(Format<Json, SystemTime>),
}

impl<S, N> FormatEvent<S, N> for JsonOrNot
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        writer: Writer<'_>,
        event: &TracingEvent<'_>,
    ) -> std::fmt::Result
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        match self {
            JsonOrNot::Not(f) => f.format_event(ctx, writer, event),
            JsonOrNot::Json(f) => f.format_event(ctx, writer, event),
        }
    }
}

fn configure_tracing(
    structured_logging: bool,
    tracing_enabled: bool,
    tracing_endpoint: Option<String>,
) {
    let env_filter_layer = get_env_filter();
    let log_layer = get_log_layer(structured_logging);
    let subscriber = tracing_subscriber::Registry::default()
        .with(env_filter_layer)
        .with(log_layer);
    if !tracing_enabled && tracing_endpoint.is_none() {
        if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
            eprintln!("Logger/tracer was already initted, continuing: {}", e);
        }
        return;
    }

    let mut tracing_endpoint =
        tracing_endpoint.unwrap_or_else(|| format!("http://localhost:55681{}", TRACING_PATH));
    if !tracing_endpoint.ends_with(TRACING_PATH) {
        tracing_endpoint.push_str(TRACING_PATH);
    }
    let res = match opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .http()
                .with_endpoint(tracing_endpoint)
                .with_protocol(Protocol::HttpBinary),
        )
        .with_trace_config(
            opentelemetry::sdk::trace::config()
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(IdGenerator::default())
                .with_max_events_per_span(64)
                .with_max_attributes_per_span(16)
                .with_max_events_per_span(16)
                .with_resource(Resource::new(vec![opentelemetry::KeyValue::new(
                    "service.name",
                    "wadm",
                )])),
        )
        .install_batch(opentelemetry::runtime::Tokio)
    {
        Ok(t) => tracing::subscriber::set_global_default(
            subscriber.with(tracing_opentelemetry::layer().with_tracer(t)),
        ),
        Err(e) => {
            eprintln!(
                "Unable to configure OTEL tracing, defaulting to logging only: {:?}",
                e
            );
            tracing::subscriber::set_global_default(subscriber)
        }
    };
    if let Err(e) = res {
        eprintln!("Logger/tracer was already initted, continuing: {}", e);
    }
}

fn get_log_layer(structured_logging: bool) -> impl Layer<Layered<EnvFilter, Registry>> {
    let log_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(atty::is(atty::Stream::Stderr));
    if structured_logging {
        log_layer.event_format(JsonOrNot::Json(Format::default().json()))
    } else {
        log_layer.event_format(JsonOrNot::Not(Format::default()))
    }
}

fn get_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|e| {
        eprintln!("RUST_LOG was not set or the given directive was invalid: {e:?}\nDefaulting logger to `info` level");
        EnvFilter::default().add_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
    })
}
