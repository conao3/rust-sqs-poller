use std::io::Write as _;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use sqs_poller::{run, Config, HandlerResult, Message};
use tokio_util::sync::CancellationToken;

/// Polls an SQS queue with an exponential backoff while the queue is
/// empty, resetting the moment a message arrives.
///
/// Each message goes to exactly one sink: an HTTP endpoint, a command,
/// or standard output (the default). A message is deleted only when its
/// sink accepts it.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// The queue to poll.
    #[arg(long, env = "SQS_POLLER_QUEUE_URL")]
    queue_url: String,

    /// POST each message body to this URL. The message is deleted when
    /// the endpoint answers with a 2xx status.
    #[arg(long, conflicts_with = "command")]
    endpoint: Option<String>,

    /// Run this command once per message, with the message body on
    /// standard input and its ID in $SQS_MESSAGE_ID. The message is
    /// deleted when the command exits with status 0.
    #[arg(long, num_args = 1.., allow_hyphen_values = true)]
    command: Option<Vec<String>>,

    /// Sleep after the first empty poll, in seconds.
    #[arg(long, default_value_t = 1.0)]
    min_interval_secs: f64,

    /// Longest sleep between polls, in seconds.
    #[arg(long, default_value_t = 300.0)]
    max_interval_secs: f64,

    /// Backoff growth factor per empty poll.
    #[arg(long, default_value_t = 2.0)]
    multiplier: f64,

    /// WaitTimeSeconds of each receive (long polling).
    #[arg(long, default_value_t = 20)]
    wait_time_seconds: i32,

    /// MaxNumberOfMessages of each receive.
    #[arg(long, default_value_t = 10)]
    max_messages: i32,

    /// Disable jitter on the backoff sleeps.
    #[arg(long)]
    no_jitter: bool,

    /// AWS region override.
    #[arg(long)]
    region: Option<String>,

    /// AWS profile override.
    #[arg(long)]
    profile: Option<String>,
}

fn json_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out
}

fn print_message(message: &Message) -> HandlerResult {
    let mut line = String::from("{");
    line.push_str(&format!(
        "\"messageId\":\"{}\"",
        json_escape(message.message_id.as_deref().unwrap_or_default())
    ));
    line.push_str(&format!(
        ",\"body\":\"{}\"",
        json_escape(message.body.as_deref().unwrap_or_default())
    ));
    line.push_str(",\"attributes\":{");
    let mut first = true;
    if let Some(attributes) = &message.attributes {
        for (key, value) in attributes {
            if !first {
                line.push(',');
            }
            first = false;
            line.push_str(&format!(
                "\"{}\":\"{}\"",
                json_escape(key.as_str()),
                json_escape(value)
            ));
        }
    }
    line.push_str("}}");
    let stdout = std::io::stdout();
    let mut lock = stdout.lock();
    writeln!(lock, "{line}")?;
    lock.flush()?;
    Ok(())
}

async fn run_command(command: &[String], message: &Message) -> HandlerResult {
    use tokio::io::AsyncWriteExt as _;
    let mut child = tokio::process::Command::new(&command[0])
        .args(&command[1..])
        .env(
            "SQS_MESSAGE_ID",
            message.message_id.as_deref().unwrap_or_default(),
        )
        .stdin(std::process::Stdio::piped())
        .spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(message.body.as_deref().unwrap_or_default().as_bytes())
            .await?;
    }
    let status = child.wait().await?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!("command exited with {status}")).into())
    }
}

#[cfg(feature = "http")]
async fn post_message(http: &reqwest::Client, endpoint: &str, message: &Message) -> HandlerResult {
    let response = http
        .post(endpoint)
        .header(
            "x-sqs-message-id",
            message.message_id.as_deref().unwrap_or_default(),
        )
        .body(message.body.clone().unwrap_or_default())
        .send()
        .await?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!("endpoint answered with {}", response.status())).into())
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    if args.min_interval_secs <= 0.0
        || args.max_interval_secs < args.min_interval_secs
        || args.multiplier < 1.0
    {
        eprintln!("error: need 0 < min-interval-secs <= max-interval-secs and multiplier >= 1.0");
        return ExitCode::from(2);
    }

    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(region) = &args.region {
        loader = loader.region(aws_sdk_sqs::config::Region::new(region.clone()));
    }
    if let Some(profile) = &args.profile {
        loader = loader.profile_name(profile);
    }
    let aws = loader.load().await;
    let client = aws_sdk_sqs::Client::new(&aws);

    let mut config = Config::new(args.queue_url.clone());
    config.min_interval = Duration::from_secs_f64(args.min_interval_secs);
    config.max_interval = Duration::from_secs_f64(args.max_interval_secs);
    config.multiplier = args.multiplier;
    config.wait_time_seconds = args.wait_time_seconds;
    config.max_messages = args.max_messages;
    config.jitter = !args.no_jitter;
    config.on_error = Some(Arc::new(|error| eprintln!("sqs-poller: {error}")));

    let shutdown = CancellationToken::new();
    let trigger = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        trigger.cancel();
    });
    #[cfg(unix)]
    {
        let trigger = shutdown.clone();
        tokio::spawn(async move {
            if let Ok(mut term) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                term.recv().await;
                trigger.cancel();
            }
        });
    }

    let stats = if let Some(endpoint) = &args.endpoint {
        #[cfg(feature = "http")]
        {
            let http = reqwest::Client::new();
            let endpoint = endpoint.clone();
            run(
                config,
                client,
                move |message| {
                    let http = http.clone();
                    let endpoint = endpoint.clone();
                    async move { post_message(&http, &endpoint, &message).await }
                },
                shutdown,
            )
            .await
        }
        #[cfg(not(feature = "http"))]
        {
            let _ = endpoint;
            eprintln!("error: --endpoint needs the http feature, which this build lacks");
            return ExitCode::from(2);
        }
    } else if let Some(command) = &args.command {
        let command = command.clone();
        run(
            config,
            client,
            move |message| {
                let command = command.clone();
                async move { run_command(&command, &message).await }
            },
            shutdown,
        )
        .await
    } else {
        run(
            config,
            client,
            |message| async move { print_message(&message) },
            shutdown,
        )
        .await
    };

    eprintln!(
        "sqs-poller: stopped. received {}, deleted {}, failed {}, empty polls {}, receive errors {}",
        stats.received, stats.deleted, stats.failed, stats.empty_polls, stats.receive_errors
    );
    ExitCode::SUCCESS
}
