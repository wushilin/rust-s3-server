mod config;
mod list;
mod messages;
mod mirror;
mod output;
mod timefilter;
mod transfer;
mod urls;

use std::collections::{BTreeMap, VecDeque};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::Client;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use output::{OutputOpts, init_output, out, print_error, print_msg};

use config::{Alias, client_for_alias, load_config, save_config};
use list::ObjectPaginator;
use messages::{
    ContentMessage, CopyMessage, MakeBucketMessage, RemoveBucketMessage, RmMessage, StatMessage,
    SummaryMessage, TransferSession,
};
use transfer::{download_object, transfer_object_between_s3, upload_file};
use urls::{DEFAULT_PART_SIZE, is_s3_url, parse_s3_url, parse_size};

/// Fallback timestamp for listing entries that have no real modification
/// time (e.g. `CommonPrefixes` "directory" entries, which S3 doesn't stamp).
fn epoch() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(0, 0).unwrap()
}

fn smithy_to_chrono(t: &aws_smithy_types::DateTime) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(t.secs(), t.subsec_nanos()).unwrap_or_else(epoch)
}

/// `ls` displays entries relative to the listed directory (e.g. `mc ls
/// bucket/dir/` shows `f.txt`, not `dir/f.txt`), mirroring how a plain `ls`
/// shows names relative to the queried path rather than full absolute
/// object keys.
///
/// Real `mc` gets this by auto-detecting a bare (non-slash-terminated)
/// directory target and re-listing it with a trailing separator appended
/// *before* computing the strip-prefix (`cmd/ls.go`'s `mainList` +
/// `generateContentMessages`'s `prefixPath` truncation, [SEM] §11). `ls`'s
/// main object-listing path replicates that exactly (a `HeadObject` +
/// child-probe resolution, then mc's parent-directory truncation of the
/// strip boundary -- see `ls`'s `(Some(bucket), key)` arm), so it always
/// calls this helper with a `prefix` that ends in `/` or is empty, i.e.
/// only the plain `strip_prefix` branches below.
///
/// `list_incomplete` has no equivalent "does an in-progress upload exist
/// at exactly this key" probe available, so it still calls this with a
/// possibly-bare `prefix`; the boundary-consuming fallback below covers
/// that case reasonably (a matching key must cross exactly one `/`
/// boundary right after a bare `prefix` to be treated as living "under"
/// it; one that continues with a non-`/` character was only a
/// partial-filename match, e.g. `pre` vs. `prefix1.txt`, and mc leaves
/// those unstripped too) without needing its own probe.
fn strip_listing_prefix(full_key: &str, prefix: &str) -> String {
    if prefix.is_empty() {
        return full_key.to_string();
    }
    let Some(rest) = full_key.strip_prefix(prefix) else {
        return full_key.to_string();
    };
    if prefix.ends_with('/') {
        return rest.to_string();
    }
    match rest.strip_prefix('/') {
        Some(after_boundary) => after_boundary.to_string(),
        None => full_key.to_string(),
    }
}

/// mc trims exactly one leading and one trailing literal `"` from an S3
/// ETag before displaying/serializing it (`cmd/ls.go:146-148`,
/// `cmd/stat.go:189-190` -- `strings.TrimPrefix`/`TrimSuffix`, not a
/// general quote-stripping loop).
fn strip_etag_quotes(etag: &str) -> String {
    let etag = etag.strip_prefix('"').unwrap_or(etag);
    let etag = etag.strip_suffix('"').unwrap_or(etag);
    etag.to_string()
}

#[derive(Parser, Debug)]
#[command(
    name = "rs3",
    version,
    about = "S3-compatible object storage client",
    long_about = "rs3 implements portable S3-compatible bucket and object workflows in Rust."
)]
struct Cli {
    #[arg(long, global = true, help = "enable JSON lines formatted output")]
    json: bool,
    #[arg(long, global = true, help = "disable progress bar display")]
    no_color: bool,
    #[arg(long, global = true, help = "disable paging for help output")]
    disable_pager: bool,
    #[arg(long, global = true, help = "suppress chatty console output")]
    quiet: bool,
    #[arg(long, global = true, help = "install auto-completion for your shell")]
    autocompletion: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(about = "manage server credentials in configuration file")]
    Alias(AliasArgs),
    #[command(about = "list buckets and objects")]
    Ls(LsArgs),
    #[command(about = "make a bucket")]
    Mb(MbArgs),
    #[command(about = "remove a bucket")]
    Rb(RbArgs),
    #[command(about = "upload an object to a bucket")]
    Put(PutArgs),
    #[command(about = "copy objects")]
    Cp(CpArgs),
    #[command(about = "get s3 object to local")]
    Get(GetArgs),
    #[command(about = "display object contents")]
    Cat(CatArgs),
    #[command(about = "remove object(s)")]
    Rm(RmArgs),
    #[command(about = "show object metadata")]
    Stat(StatArgs),
    #[command(about = "synchronize object(s) to a remote site")]
    Mirror(MirrorArgs),
}

#[derive(Args, Debug)]
struct AliasArgs {
    #[command(subcommand)]
    command: AliasCommand,
}

#[derive(Subcommand, Debug)]
enum AliasCommand {
    #[command(about = "set a new alias to configuration file")]
    Set {
        alias: String,
        url: String,
        access_key: String,
        secret_key: String,
        #[arg(
            default_value = "S3v4",
            help = "API signature; accepted for mc compatibility"
        )]
        api: String,
        #[arg(default_value = "auto", help = "path lookup style: auto, on, or off")]
        path: String,
    },
    #[command(about = "list aliases in configuration file")]
    List,
    #[command(about = "remove an alias from configuration file")]
    Remove { alias: String },
    #[command(about = "export configuration info to stdout")]
    Export { alias: Option<String> },
    #[command(about = "import configuration info from stdin")]
    Import,
}

#[derive(Args, Debug)]
struct LsArgs {
    #[arg(long)]
    rewind: Option<String>,
    #[arg(long)]
    versions: bool,
    #[arg(short = 'r', long)]
    recursive: bool,
    #[arg(short = 'I', long)]
    incomplete: bool,
    #[arg(long)]
    summarize: bool,
    #[arg(long = "storage-class", visible_alias = "sc")]
    storage_class: Option<String>,
    #[arg(long)]
    zip: bool,
    targets: Vec<String>,
}

#[derive(Args, Debug)]
struct MbArgs {
    #[arg(long, default_value = "us-east-1")]
    region: String,
    #[arg(short = 'p', long)]
    ignore_existing: bool,
    #[arg(short = 'l', long)]
    with_lock: bool,
    #[arg(long)]
    with_versioning: bool,
    targets: Vec<String>,
}

#[derive(Args, Debug)]
struct RbArgs {
    #[arg(long)]
    force: bool,
    #[arg(long)]
    dangerous: bool,
    targets: Vec<String>,
}

#[derive(Args, Debug)]
struct PutArgs {
    #[arg(short = 'P', long, default_value_t = 4)]
    parallel: usize,
    #[arg(short = 's', long, default_value = "256MiB", help = "each part size")]
    part_size: String,
    #[arg(long)]
    disable_multipart: bool,
    #[arg(long = "storage-class", visible_alias = "sc")]
    storage_class: Option<String>,
    #[arg(required = true)]
    source: PathBuf,
    #[arg(required = true)]
    target: String,
}

#[derive(Args, Debug)]
struct CpArgs {
    #[arg(short = 'r', long)]
    recursive: bool,
    #[arg(short = 'P', long, default_value_t = 4)]
    parallel: usize,
    #[arg(short = 's', long, default_value = "256MiB", help = "each part size")]
    part_size: String,
    #[arg(long)]
    older_than: Option<String>,
    #[arg(long)]
    newer_than: Option<String>,
    #[arg(long)]
    disable_multipart: bool,
    #[arg(long = "storage-class", visible_alias = "sc")]
    storage_class: Option<String>,
    #[arg(required = true)]
    paths: Vec<String>,
}

#[derive(Args, Debug)]
pub(crate) struct MirrorArgs {
    #[arg(short = 'P', long, default_value_t = 4)]
    pub(crate) parallel: usize,
    #[arg(short = 's', long, default_value = "256MiB", help = "each part size")]
    pub(crate) part_size: String,
    #[arg(long)]
    pub(crate) overwrite: bool,
    #[arg(long)]
    pub(crate) remove: bool,
    #[arg(long)]
    pub(crate) dry_run: bool,
    #[arg(long, alias = "fake")]
    pub(crate) fake: bool,
    #[arg(short = 'w', long)]
    pub(crate) watch: bool,
    #[arg(long)]
    pub(crate) disable_multipart: bool,
    #[arg(long)]
    pub(crate) older_than: Option<String>,
    #[arg(long)]
    pub(crate) newer_than: Option<String>,
    pub(crate) source: String,
    pub(crate) target: String,
}

#[derive(Args, Debug)]
struct GetArgs {
    #[arg(long = "version-id", visible_alias = "vid")]
    version_id: Option<String>,
    source: String,
    target: Option<PathBuf>,
}

#[derive(Args, Debug)]
struct CatArgs {
    #[arg(long)]
    offset: Option<i64>,
    #[arg(long)]
    tail: Option<i64>,
    targets: Vec<String>,
}

#[derive(Args, Debug)]
struct RmArgs {
    #[arg(short = 'r', long)]
    recursive: bool,
    #[arg(long)]
    force: bool,
    #[arg(long)]
    versions: bool,
    #[arg(long = "version-id", visible_alias = "vid")]
    version_id: Option<String>,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    older_than: Option<String>,
    #[arg(long)]
    newer_than: Option<String>,
    targets: Vec<String>,
}

#[derive(Args, Debug)]
struct StatArgs {
    #[arg(short = 'r', long)]
    recursive: bool,
    #[arg(short = 'v', long)]
    verbose: bool,
    targets: Vec<String>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    init_output(OutputOpts {
        json: cli.json,
        quiet: cli.quiet,
        no_color: cli.no_color,
        stdout_tty: std::io::stdout().is_terminal(),
    });
    if let Err(e) = run(cli).await {
        print_error(&format!("{e:#}"), "", true);
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Alias(args) => alias(args).await,
        Commands::Ls(args) => ls(args).await,
        Commands::Mb(args) => mb(args).await,
        Commands::Rb(args) => rb(args).await,
        Commands::Put(args) => put(args).await,
        Commands::Cp(args) => cp(args).await,
        Commands::Get(args) => get(args).await,
        Commands::Cat(args) => cat(args).await,
        Commands::Rm(args) => rm(args).await,
        Commands::Stat(args) => stat(args).await,
        Commands::Mirror(args) => mirror(args).await,
    }
}

async fn alias(args: AliasArgs) -> Result<()> {
    match args.command {
        AliasCommand::Set {
            alias,
            url,
            access_key,
            secret_key,
            api,
            path,
        } => {
            let mut cfg = load_config().await?;
            cfg.version = "10".into();
            cfg.aliases.insert(
                alias.clone(),
                Alias {
                    url,
                    access_key,
                    secret_key,
                    api,
                    path,
                    region: std::env::var("AWS_S3_REGION")
                        .ok()
                        .or_else(|| std::env::var("AWS_REGION").ok()),
                },
            );
            save_config(&cfg).await?;
            println!("Added `{alias}` successfully.");
            Ok(())
        }
        AliasCommand::List => {
            let cfg = load_config().await?;
            for (name, alias) in cfg.aliases {
                println!("{:<16} {}", name, alias.url);
            }
            Ok(())
        }
        AliasCommand::Remove { alias } => {
            let mut cfg = load_config().await?;
            if cfg.aliases.remove(&alias).is_none() {
                return Err(anyhow!("alias `{alias}` not found"));
            }
            save_config(&cfg).await?;
            println!("Removed `{alias}` successfully.");
            Ok(())
        }
        AliasCommand::Export { alias } => {
            let cfg = load_config().await?;
            if let Some(alias) = alias {
                let item = cfg
                    .aliases
                    .get(&alias)
                    .ok_or_else(|| anyhow!("alias `{alias}` not found"))?;
                println!("{}", serde_json::to_string_pretty(item)?);
            } else {
                println!("{}", serde_json::to_string_pretty(&cfg)?);
            }
            Ok(())
        }
        AliasCommand::Import => Err(anyhow!("alias import is not implemented yet")),
    }
}

async fn ls(args: LsArgs) -> Result<()> {
    if args.rewind.is_some() {
        return Err(anyhow!("ls --rewind is not implemented yet"));
    }
    if args.versions {
        return Err(anyhow!("ls --versions is not implemented yet"));
    }
    if args.zip {
        return Err(anyhow!("ls --zip is not implemented yet"));
    }
    // Client-side post-filter: an object with an *empty* storage class is
    // never excluded, even when a specific class was requested; "*" (or
    // no flag at all) disables filtering entirely ([SEM] §11).
    let storage_filter = args
        .storage_class
        .as_deref()
        .filter(|s| !s.is_empty() && *s != "*");
    let targets = if args.targets.is_empty() {
        vec!["".to_string()]
    } else {
        args.targets
    };
    for target in targets {
        let parsed = parse_s3_url(&target)?;
        let (client, _) = client_for_alias(&parsed.alias).await?;
        match (parsed.bucket, parsed.key) {
            (None, _) => {
                let resp = client.list_buckets().send().await?;
                for bucket in resp.buckets() {
                    let name = bucket.name().unwrap_or_default();
                    let time = bucket
                        .creation_date()
                        .map(smithy_to_chrono)
                        .unwrap_or_else(epoch);
                    print_msg(&ContentMessage {
                        status: "success".into(),
                        filetype: "folder".into(),
                        time,
                        size: 0,
                        key: format!("{name}/"),
                        etag: String::new(),
                        storage_class: None,
                    });
                }
            }
            (Some(bucket), key) => {
                let mut prefix = key.unwrap_or_default();
                if args.incomplete {
                    list_incomplete(&client, &bucket, &prefix, args.recursive).await?;
                    continue;
                }
                // mc's actual resolution order for a bare (non-empty,
                // non-slash-terminated) target (`cmd/ls-main.go`'s
                // `mainList`, [SEM] §11): `Stat()` it first. A hit means
                // the target names a real object, which is listed as
                // exactly that one entry, keyed relative to its parent
                // directory. A miss means it's being used as a directory,
                // so re-list with a trailing separator appended -- from
                // that point on it behaves identically to a target the
                // caller already slash-terminated.
                if !prefix.is_empty() && !prefix.ends_with('/') {
                    match client
                        .head_object()
                        .bucket(&bucket)
                        .key(&prefix)
                        .send()
                        .await
                    {
                        Ok(resp) => {
                            let size = resp.content_length().unwrap_or_default() as u64;
                            let storage_class = resp
                                .storage_class()
                                .map(|s| s.as_str().to_string())
                                .unwrap_or_default();
                            let filtered_out = storage_filter.is_some_and(|filter| {
                                !storage_class.is_empty() && storage_class != filter
                            });
                            if !filtered_out {
                                let time = resp
                                    .last_modified()
                                    .map(smithy_to_chrono)
                                    .unwrap_or_else(epoch);
                                let etag = strip_etag_quotes(resp.e_tag().unwrap_or_default());
                                let basename =
                                    prefix.rsplit('/').next().unwrap_or(&prefix).to_string();
                                print_msg(&ContentMessage {
                                    status: "success".into(),
                                    filetype: "file".into(),
                                    time,
                                    size,
                                    key: basename,
                                    etag,
                                    storage_class: if storage_class.is_empty() {
                                        None
                                    } else {
                                        Some(storage_class)
                                    },
                                });
                                if args.summarize {
                                    print_msg(&SummaryMessage {
                                        total_objects: 1,
                                        total_size: size,
                                    });
                                }
                            } else if args.summarize {
                                print_msg(&SummaryMessage {
                                    total_objects: 0,
                                    total_size: 0,
                                });
                            }
                            continue;
                        }
                        Err(_) => {
                            // No object at exactly `prefix` -- but that
                            // alone doesn't mean it's a directory. mc only
                            // commits to the directory interpretation once
                            // a 1-key probe under `prefix + "/"` actually
                            // finds something (`client-s3.go:1684-1697`);
                            // otherwise the bare prefix is kept as-is and
                            // listed literally, which is what makes a
                            // partial-filename search like `ls bucket/pre`
                            // (matching `prefix1.txt`) work.
                            let probe = client
                                .list_objects_v2()
                                .bucket(&bucket)
                                .prefix(format!("{prefix}/"))
                                .max_keys(1)
                                .send()
                                .await;
                            // (the `common_prefixes` half mirrors mc's own
                            // condition; this probe sets no delimiter, so S3
                            // never populates it -- it's kept only so the
                            // check reads the same as the Go original.)
                            let is_directory = probe.is_ok_and(|r| {
                                !r.contents().is_empty() || !r.common_prefixes().is_empty()
                            });
                            if is_directory {
                                prefix.push('/');
                            }
                        }
                    }
                }
                // The boundary displayed keys are stripped of. mc truncates
                // the prefix to its *parent directory* before stripping
                // (`prefixPath = prefixPath[:strings.LastIndex(prefixPath,
                // "/")+1]`, `cmd/ls.go`'s `generateContentMessages`), which
                // matters for a bare prefix kept as a partial-filename
                // search: `ls bucket/dir/pre` matching `dir/prefix1.txt`
                // displays `prefix1.txt`, relative to `dir/`. For every
                // other path `prefix` is already slash-terminated (or
                // empty) by this point, and this truncation is a no-op.
                let strip_from = match prefix.rfind('/') {
                    Some(i) => prefix[..i + 1].to_string(),
                    None => String::new(),
                };
                let delimiter = if args.recursive { None } else { Some("/") };
                let mut token = None;
                let mut total_objects = 0u64;
                let mut total_size = 0u64;
                loop {
                    let resp = client
                        .list_objects_v2()
                        .bucket(&bucket)
                        .prefix(&prefix)
                        .set_delimiter(delimiter.map(String::from))
                        .set_continuation_token(token)
                        .send()
                        .await?;
                    for p in resp.common_prefixes() {
                        let full = p.prefix().unwrap_or_default();
                        print_msg(&ContentMessage {
                            status: "success".into(),
                            filetype: "folder".into(),
                            time: epoch(),
                            size: 0,
                            key: strip_listing_prefix(full, &strip_from),
                            etag: String::new(),
                            storage_class: None,
                        });
                    }
                    for obj in resp.contents() {
                        let size = obj.size().unwrap_or_default() as u64;
                        let key = strip_listing_prefix(obj.key().unwrap_or_default(), &strip_from);
                        let etag = strip_etag_quotes(obj.e_tag().unwrap_or_default());
                        let time = obj
                            .last_modified()
                            .map(smithy_to_chrono)
                            .unwrap_or_else(epoch);
                        let storage_class = obj
                            .storage_class()
                            .map(|s| s.as_str().to_string())
                            .unwrap_or_default();
                        if let Some(filter) = storage_filter {
                            if !storage_class.is_empty() && storage_class != filter {
                                continue;
                            }
                        }
                        if args.summarize {
                            total_objects += 1;
                            total_size += size;
                        }
                        print_msg(&ContentMessage {
                            status: "success".into(),
                            filetype: "file".into(),
                            time,
                            size,
                            key,
                            etag,
                            storage_class: if storage_class.is_empty() {
                                None
                            } else {
                                Some(storage_class)
                            },
                        });
                    }
                    if resp.is_truncated().unwrap_or(false) {
                        token = resp.next_continuation_token().map(String::from);
                    } else {
                        break;
                    }
                }
                if args.summarize {
                    print_msg(&SummaryMessage {
                        total_objects,
                        total_size,
                    });
                }
            }
        }
    }
    Ok(())
}

/// `ls --incomplete`: lists in-progress multipart uploads instead of
/// committed objects, mapped onto the same `ContentMessage` shape ([SEM]
/// §11). Size is bytes-uploaded-so-far, summed from `list_parts`; a
/// listing failure there is tolerated and reported as size 0.
async fn list_incomplete(
    client: &Client,
    bucket: &str,
    prefix: &str,
    recursive: bool,
) -> Result<()> {
    let delimiter = if recursive { None } else { Some("/") };
    let mut key_marker: Option<String> = None;
    let mut upload_id_marker: Option<String> = None;
    loop {
        let resp = client
            .list_multipart_uploads()
            .bucket(bucket)
            .prefix(prefix)
            .set_delimiter(delimiter.map(String::from))
            .set_key_marker(key_marker.take())
            .set_upload_id_marker(upload_id_marker.take())
            .send()
            .await?;
        for p in resp.common_prefixes() {
            let full = p.prefix().unwrap_or_default();
            print_msg(&ContentMessage {
                status: "success".into(),
                filetype: "folder".into(),
                time: epoch(),
                size: 0,
                key: strip_listing_prefix(full, prefix),
                etag: String::new(),
                storage_class: None,
            });
        }
        for upload in resp.uploads() {
            let key = upload.key().unwrap_or_default().to_string();
            let upload_id = upload.upload_id().unwrap_or_default().to_string();
            let time = upload
                .initiated()
                .map(smithy_to_chrono)
                .unwrap_or_else(epoch);
            let size = incomplete_upload_size(client, bucket, &key, &upload_id).await;
            print_msg(&ContentMessage {
                status: "success".into(),
                filetype: "file".into(),
                time,
                size,
                key: strip_listing_prefix(&key, prefix),
                etag: String::new(),
                storage_class: None,
            });
        }
        if resp.is_truncated().unwrap_or(false) {
            key_marker = resp.next_key_marker().map(String::from);
            upload_id_marker = resp.next_upload_id_marker().map(String::from);
            if key_marker.is_none() && upload_id_marker.is_none() {
                break;
            }
        } else {
            break;
        }
    }
    Ok(())
}

/// Sum of part sizes for an in-progress multipart upload. Best-effort: a
/// `list_parts` error simply yields whatever total had accumulated so far
/// (acceptable to report 0 for an upload with no parts yet).
async fn incomplete_upload_size(client: &Client, bucket: &str, key: &str, upload_id: &str) -> u64 {
    let mut total = 0u64;
    let mut marker: Option<String> = None;
    loop {
        let resp = match client
            .list_parts()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .set_part_number_marker(marker.take())
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => return total,
        };
        for part in resp.parts() {
            total += part.size().unwrap_or_default() as u64;
        }
        if resp.is_truncated().unwrap_or(false) {
            marker = resp.next_part_number_marker().map(String::from);
            if marker.is_none() {
                break;
            }
        } else {
            break;
        }
    }
    total
}

async fn mb(args: MbArgs) -> Result<()> {
    if args.with_lock {
        return Err(anyhow!("mb --with-lock is not implemented yet"));
    }
    if args.with_versioning {
        return Err(anyhow!("mb --with-versioning is not implemented yet"));
    }
    for target in args.targets {
        let parsed = parse_s3_url(&target)?;
        let bucket = parsed
            .bucket
            .ok_or_else(|| anyhow!("bucket is required in target `{target}`"))?;
        let (client, _) = client_for_alias(&parsed.alias).await?;
        let mut req = client.create_bucket().bucket(&bucket);
        if args.region != "us-east-1" {
            req = req.create_bucket_configuration(
                CreateBucketConfiguration::builder()
                    .location_constraint(BucketLocationConstraint::from(args.region.as_str()))
                    .build(),
            );
        }
        let result = req.send().await;
        match result {
            Ok(_) => print_msg(&MakeBucketMessage { bucket: target }),
            Err(err) => {
                let svc = err.as_service_error();
                let already_exists = svc.is_some_and(|e| {
                    e.is_bucket_already_owned_by_you() || e.is_bucket_already_exists()
                });
                if args.ignore_existing && already_exists {
                    // Human-only informational line -- not one of mc's
                    // message structs, so it must not corrupt a --json
                    // stream (bare prose on stdout wouldn't parse as JSON).
                    if !out().json {
                        println!("Bucket `{target}` already exists.");
                    }
                } else {
                    return Err(err.into());
                }
            }
        }
    }
    Ok(())
}

async fn rb(args: RbArgs) -> Result<()> {
    let mut failures = 0u64;
    for target in &args.targets {
        let parsed = match parse_s3_url(target) {
            Ok(p) => p,
            Err(err) => {
                eprintln!("rb: {target}: {err:#}");
                failures += 1;
                continue;
            }
        };
        let result: Result<()> = async {
            let (client, _) = client_for_alias(&parsed.alias).await?;
            match &parsed.bucket {
                Some(bucket) => remove_bucket(&client, &parsed.alias, bucket, args.force).await,
                None => {
                    if !args.dangerous {
                        return Err(anyhow!(
                            "removing all buckets on `{}` requires --dangerous",
                            parsed.alias
                        ));
                    }
                    let resp = client.list_buckets().send().await?;
                    let mut bucket_failures = 0u64;
                    for bucket in resp.buckets() {
                        let name = bucket.name().unwrap_or_default();
                        if let Err(err) =
                            remove_bucket(&client, &parsed.alias, name, args.force).await
                        {
                            eprintln!("rb: {}/{name}: {err:#}", parsed.alias);
                            bucket_failures += 1;
                        }
                    }
                    if bucket_failures > 0 {
                        return Err(anyhow!("{bucket_failures} bucket(s) failed"));
                    }
                    Ok(())
                }
            }
        }
        .await;
        if let Err(err) = result {
            eprintln!("rb: {target}: {err:#}");
            failures += 1;
        }
    }
    if failures > 0 {
        return Err(anyhow!("{failures} target(s) failed"));
    }
    Ok(())
}

async fn remove_bucket(client: &Client, alias: &str, bucket: &str, force: bool) -> Result<()> {
    if force {
        abort_incomplete_uploads(client, bucket).await?;
        remove_prefix(client, alias, bucket, "", false, None, None).await?;
    }
    client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .map_err(|err| {
            let not_empty = err
                .as_service_error()
                .map(|e| format!("{e:?}").contains("BucketNotEmpty"))
                .unwrap_or(false);
            if not_empty {
                anyhow!("`{alias}/{bucket}` is not empty; use --force to remove its contents")
            } else {
                anyhow!(err)
            }
        })?;
    print_msg(&RemoveBucketMessage {
        bucket: format!("{alias}/{bucket}"),
    });
    Ok(())
}

async fn abort_incomplete_uploads(client: &Client, bucket: &str) -> Result<()> {
    let mut key_marker: Option<String> = None;
    let mut id_marker: Option<String> = None;
    loop {
        let resp = client
            .list_multipart_uploads()
            .bucket(bucket)
            .set_key_marker(key_marker.take())
            .set_upload_id_marker(id_marker.take())
            .send()
            .await?;
        for upload in resp.uploads() {
            let (Some(key), Some(id)) = (upload.key(), upload.upload_id()) else {
                continue;
            };
            client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(id)
                .send()
                .await?;
        }
        if resp.is_truncated().unwrap_or(false) {
            key_marker = resp.next_key_marker().map(String::from);
            id_marker = resp.next_upload_id_marker().map(String::from);
            if key_marker.is_none() && id_marker.is_none() {
                // Server claims truncation but gave us nothing to page
                // forward with; treat as done rather than looping forever
                // re-requesting the same first page.
                return Ok(());
            }
        } else {
            return Ok(());
        }
    }
}

async fn put(args: PutArgs) -> Result<()> {
    let part_size = parse_size(&args.part_size)?;
    let session = TransferSession::new("put");
    let outcome = upload_file(
        &args.source,
        &args.target,
        part_size,
        args.parallel,
        args.disable_multipart,
        args.storage_class.as_deref(),
    )
    .await?;
    session.add_total(outcome.size);
    let (total_count, total_size) = session.totals();
    let msg = CopyMessage {
        source: args.source.display().to_string(),
        target: outcome.target,
        size: outcome.size,
        total_count,
        total_size,
    };
    session.object_done(&msg, outcome.size);
    session.finish();
    Ok(())
}

async fn cp(args: CpArgs) -> Result<()> {
    timefilter::validate_time_filters(args.older_than.as_deref(), args.newer_than.as_deref())?;
    if args.paths.len() < 2 {
        return Err(anyhow!("cp requires SOURCE [SOURCE...] TARGET"));
    }
    let part_size = parse_size(&args.part_size)?;
    let target = args.paths.last().unwrap().clone();
    let session = TransferSession::new("cp");
    let mut used_session = false;
    for source in &args.paths[..args.paths.len() - 1] {
        if is_s3_url(source) && is_s3_url(&target) {
            if args.recursive {
                cp_recursive(source, &target, &args).await?;
            } else {
                copy_s3_object_to_s3(
                    source,
                    &target,
                    part_size,
                    args.disable_multipart,
                    args.parallel,
                    &session,
                    args.older_than.as_deref(),
                    args.newer_than.as_deref(),
                )
                .await?;
                used_session = true;
            }
        } else if is_s3_url(source) && !is_s3_url(&target) {
            if args.recursive {
                cp_recursive(source, &target, &args).await?;
            } else {
                let parsed = parse_s3_url(source)?;
                let bucket = parsed
                    .bucket
                    .clone()
                    .ok_or_else(|| anyhow!("bucket is required in source `{source}`"))?;
                let key = parsed
                    .key
                    .clone()
                    .ok_or_else(|| anyhow!("object key is required in source `{source}`"))?;
                let (head_client, _) = client_for_alias(&parsed.alias).await?;
                let head = head_client
                    .head_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|err| anyhow!("stat `{bucket}/{key}`: {err}"))?;
                let object_time = head.last_modified().map(smithy_to_chrono);
                if timefilter::passes_filters(
                    object_time,
                    args.older_than.as_deref(),
                    args.newer_than.as_deref(),
                )? {
                    download_object(
                        source,
                        Some(PathBuf::from(&target)),
                        part_size,
                        args.parallel,
                        &session,
                    )
                    .await?;
                }
                used_session = true;
            }
        } else if !is_s3_url(source) && is_s3_url(&target) {
            let source_path = Path::new(source);
            if source_path.is_dir() {
                if !args.recursive {
                    return Err(anyhow!(
                        "source `{source}` is a directory; use --recursive to copy it"
                    ));
                }
                cp_recursive(source, &target, &args).await?;
            } else {
                let metadata = fs::metadata(source_path)
                    .await
                    .with_context(|| format!("stat {}", source_path.display()))?;
                let object_time = metadata.modified().ok().map(DateTime::<Utc>::from);
                if timefilter::passes_filters(
                    object_time,
                    args.older_than.as_deref(),
                    args.newer_than.as_deref(),
                )? {
                    let outcome = upload_file(
                        source_path,
                        &target,
                        part_size,
                        args.parallel,
                        args.disable_multipart,
                        args.storage_class.as_deref(),
                    )
                    .await?;
                    session.add_total(outcome.size);
                    let (total_count, total_size) = session.totals();
                    let msg = CopyMessage {
                        source: source_path.display().to_string(),
                        target: outcome.target,
                        size: outcome.size,
                        total_count,
                        total_size,
                    };
                    session.object_done(&msg, outcome.size);
                }
                used_session = true;
            }
        } else {
            copy_local_path(
                Path::new(source),
                Path::new(&target),
                args.recursive,
                &session,
                args.older_than.as_deref(),
                args.newer_than.as_deref(),
            )
            .await?;
            used_session = true;
        }
    }
    if used_session {
        session.finish();
    }
    Ok(())
}

/// `cp --recursive` always copies everything (it is not a sync), so it drives
/// the mirror planner with `overwrite: true` and `remove: false`.
async fn cp_recursive(source: &str, target: &str, args: &CpArgs) -> Result<()> {
    let mirror_args = MirrorArgs {
        parallel: args.parallel,
        part_size: args.part_size.clone(),
        overwrite: true,
        remove: false,
        dry_run: false,
        fake: false,
        watch: false,
        disable_multipart: args.disable_multipart,
        older_than: args.older_than.clone(),
        newer_than: args.newer_than.clone(),
        source: source.to_string(),
        target: target.to_string(),
    };
    mirror::run_mirror(&mirror_args).await
}

async fn mirror(args: MirrorArgs) -> Result<()> {
    mirror::run_mirror(&args).await
}

#[allow(clippy::too_many_arguments)]
async fn copy_s3_object_to_s3(
    source: &str,
    target: &str,
    part_size: u64,
    disable_multipart: bool,
    parallel: usize,
    session: &TransferSession,
    older_than: Option<&str>,
    newer_than: Option<&str>,
) -> Result<()> {
    let source_url = parse_s3_url(source)?;
    let source_bucket = source_url
        .bucket
        .ok_or_else(|| anyhow!("bucket is required in source `{source}`"))?;
    let source_key = source_url
        .key
        .ok_or_else(|| anyhow!("object key is required in source `{source}`"))?;
    let target_url = parse_s3_url(target)?;
    let target_bucket = target_url
        .bucket
        .ok_or_else(|| anyhow!("bucket is required in target `{target}`"))?;
    let target_key = match target_url.key {
        Some(key) if key.ends_with('/') => {
            format!(
                "{}{}",
                key,
                source_key.rsplit('/').next().unwrap_or(&source_key)
            )
        }
        Some(key) if !key.is_empty() => key,
        _ => source_key
            .rsplit('/')
            .next()
            .unwrap_or(&source_key)
            .to_string(),
    };
    let (source_client, source_alias) = client_for_alias(&source_url.alias).await?;
    let (target_client, target_alias) = client_for_alias(&target_url.alias).await?;
    let head = source_client
        .head_object()
        .bucket(&source_bucket)
        .key(&source_key)
        .send()
        .await?;
    let size = head.content_length().unwrap_or_default() as u64;
    let object_time = head.last_modified().map(smithy_to_chrono);
    if !timefilter::passes_filters(object_time, older_than, newer_than)? {
        return Ok(());
    }
    transfer_object_between_s3(
        &source_client,
        &source_alias,
        &source_bucket,
        &source_key,
        &target_client,
        &target_alias,
        &target_bucket,
        &target_key,
        size,
        part_size,
        disable_multipart,
        parallel,
    )
    .await?;
    session.add_total(size);
    let (total_count, total_size) = session.totals();
    let msg = CopyMessage {
        source: format!("{}/{source_bucket}/{source_key}", source_url.alias),
        target: format!("{}/{target_bucket}/{target_key}", target_url.alias),
        size,
        total_count,
        total_size,
    };
    session.object_done(&msg, size);
    Ok(())
}

async fn copy_local_path(
    source: &Path,
    target: &Path,
    recursive: bool,
    session: &TransferSession,
    older_than: Option<&str>,
    newer_than: Option<&str>,
) -> Result<()> {
    let metadata = fs::metadata(source)
        .await
        .with_context(|| format!("stat {}", source.display()))?;
    if metadata.is_dir() {
        if !recursive {
            return Err(anyhow!(
                "source `{}` is a directory; use --recursive to copy it",
                source.display()
            ));
        }
        let mut dirs = VecDeque::from([source.to_path_buf()]);
        while let Some(dir) = dirs.pop_front() {
            let mut entries = fs::read_dir(&dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let metadata = entry.metadata().await?;
                if metadata.is_dir() {
                    dirs.push_back(path);
                } else if metadata.is_file() {
                    let object_time = metadata.modified().ok().map(DateTime::<Utc>::from);
                    if !timefilter::passes_filters(object_time, older_than, newer_than)? {
                        continue;
                    }
                    let rel = path.strip_prefix(source)?;
                    let output = target.join(rel);
                    if let Some(parent) = output.parent() {
                        fs::create_dir_all(parent).await?;
                    }
                    fs::copy(&path, &output).await?;
                    let size = metadata.len();
                    session.add_total(size);
                    let (total_count, total_size) = session.totals();
                    let msg = CopyMessage {
                        source: path.display().to_string(),
                        target: output.display().to_string(),
                        size,
                        total_count,
                        total_size,
                    };
                    session.object_done(&msg, size);
                }
            }
        }
    } else {
        let object_time = metadata.modified().ok().map(DateTime::<Utc>::from);
        if !timefilter::passes_filters(object_time, older_than, newer_than)? {
            return Ok(());
        }
        let output = if target.is_dir() {
            target.join(
                source
                    .file_name()
                    .ok_or_else(|| anyhow!("invalid source filename"))?,
            )
        } else {
            target.to_path_buf()
        };
        if let Some(parent) = output.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).await?;
            }
        }
        fs::copy(source, &output).await?;
        let size = metadata.len();
        session.add_total(size);
        let (total_count, total_size) = session.totals();
        let msg = CopyMessage {
            source: source.display().to_string(),
            target: output.display().to_string(),
            size,
            total_count,
            total_size,
        };
        session.object_done(&msg, size);
    }
    Ok(())
}

async fn get(args: GetArgs) -> Result<()> {
    if args.version_id.is_some() {
        return Err(anyhow!("get --version-id is not implemented yet"));
    }
    let session = TransferSession::new("get");
    download_object(&args.source, args.target, DEFAULT_PART_SIZE, 4, &session).await?;
    session.finish();
    Ok(())
}

async fn cat(args: CatArgs) -> Result<()> {
    if args.offset.is_some() {
        return Err(anyhow!("cat --offset is not implemented yet"));
    }
    if args.tail.is_some() {
        return Err(anyhow!("cat --tail is not implemented yet"));
    }
    for target in args.targets {
        let parsed = parse_s3_url(&target)?;
        let bucket = parsed
            .bucket
            .ok_or_else(|| anyhow!("bucket is required in target `{target}`"))?;
        let key = parsed
            .key
            .ok_or_else(|| anyhow!("object key is required in target `{target}`"))?;
        let (client, _) = client_for_alias(&parsed.alias).await?;
        let resp = client.get_object().bucket(bucket).key(key).send().await?;
        let mut reader = resp.body.into_async_read();
        let mut stdout = tokio::io::stdout();
        tokio::io::copy(&mut reader, &mut stdout).await?;
        stdout.flush().await?;
    }
    Ok(())
}

async fn rm(args: RmArgs) -> Result<()> {
    if args.versions || args.version_id.is_some() {
        return Err(anyhow!("rm --versions/--version-id is not implemented yet"));
    }
    timefilter::validate_time_filters(args.older_than.as_deref(), args.newer_than.as_deref())?;
    if args.recursive && !args.force && !args.dry_run {
        return Err(anyhow!(
            "removal with --recursive requires --force (or use --dry-run)"
        ));
    }
    let mut failures = 0u64;
    for target in &args.targets {
        if let Err(err) = rm_one_target(target, &args).await {
            eprintln!("rm: {target}: {err:#}");
            failures += 1;
        }
    }
    if failures > 0 {
        return Err(anyhow!("{failures} target(s) failed"));
    }
    Ok(())
}

async fn rm_one_target(target: &str, args: &RmArgs) -> Result<()> {
    let parsed = parse_s3_url(target)?;
    let bucket = parsed
        .bucket
        .ok_or_else(|| anyhow!("bucket is required in target `{target}`"))?;
    let (client, _) = client_for_alias(&parsed.alias).await?;
    if args.recursive {
        let prefix = parsed.key.unwrap_or_default();
        let removed = remove_prefix(
            &client,
            &parsed.alias,
            &bucket,
            &prefix,
            args.dry_run,
            args.older_than.as_deref(),
            args.newer_than.as_deref(),
        )
        .await?;
        // Human-only informational line -- not an RmMessage (nothing was
        // actually removed), so it must not corrupt a --json stream; a
        // zero-match `rm -r` under --json legitimately emits nothing. Also
        // covers a filtered-to-zero `--older-than`/`--newer-than` match --
        // that's success too, just with nothing left to report.
        if removed == 0 && !out().json {
            println!("Nothing to remove under `{target}`.");
        }
        Ok(())
    } else {
        let key = parsed
            .key
            .ok_or_else(|| anyhow!("object key is required in target `{target}`"))?;
        // DeleteObject succeeds for missing keys; stat first for an mc-like error.
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|_| anyhow!("object does not exist"))?;
        let object_time = head.last_modified().map(smithy_to_chrono);
        if !timefilter::passes_filters(
            object_time,
            args.older_than.as_deref(),
            args.newer_than.as_deref(),
        )? {
            return Ok(());
        }
        if args.dry_run {
            print_msg(&RmMessage {
                key: target.to_string(),
                dry_run: true,
                mod_time: None,
            });
            return Ok(());
        }
        client
            .delete_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await?;
        print_msg(&RmMessage {
            key: target.to_string(),
            dry_run: false,
            mod_time: None,
        });
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn remove_prefix(
    client: &Client,
    alias: &str,
    bucket: &str,
    prefix: &str,
    dry_run: bool,
    older_than: Option<&str>,
    newer_than: Option<&str>,
) -> Result<u64> {
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    // S3 prefix matching is a raw string match, so a listing prefix of `p`
    // (no trailing slash) also matches sibling keys like `prefix2/x.txt`.
    // When the prefix doesn't already end at a path boundary, filter each
    // page down to the exact key or its `prefix/`-rooted descendants so
    // `rm -r bucket/p` only ever touches `p` itself and things under `p/`.
    let boundary_safe = prefix.is_empty() || prefix.ends_with('/');
    let descendant_prefix = format!("{prefix}/");
    // Raw paginator: unlike listing/mirror paths, deletion must also sweep
    // up zero-byte folder-marker keys (keys ending in `/`), or they survive
    // `rm -r` / `rb --force` and leave the bucket non-empty.
    let mut pager =
        ObjectPaginator::new_raw(client.clone(), bucket.to_string(), prefix.to_string());
    let mut removed = 0u64;
    while let Some(mut page) = pager.next_page().await? {
        if !boundary_safe {
            page.retain(|obj| obj.key == prefix || obj.key.starts_with(&descendant_prefix));
        }
        if older_than.is_some() || newer_than.is_some() {
            let mut kept = Vec::with_capacity(page.len());
            for obj in page {
                if timefilter::passes_filters(obj.modified, older_than, newer_than)? {
                    kept.push(obj);
                }
            }
            page = kept;
        }
        if page.is_empty() {
            continue;
        }
        if dry_run {
            for obj in &page {
                print_msg(&RmMessage {
                    key: format!("{alias}/{bucket}/{}", obj.key),
                    dry_run: true,
                    mod_time: None,
                });
            }
            removed += page.len() as u64;
            continue;
        }
        for chunk in page.chunks(1000) {
            let ids = chunk
                .iter()
                .map(|o| ObjectIdentifier::builder().key(&o.key).build())
                .collect::<Result<Vec<_>, _>>()?;
            let delete = Delete::builder().set_objects(Some(ids)).build()?;
            let resp = client
                .delete_objects()
                .bucket(bucket)
                .delete(delete)
                .send()
                .await?;
            for err in resp.errors() {
                return Err(anyhow!(
                    "delete failed for `{}`: {}",
                    err.key().unwrap_or("?"),
                    err.message().unwrap_or("unknown error")
                ));
            }
            for obj in chunk {
                print_msg(&RmMessage {
                    key: format!("{alias}/{bucket}/{}", obj.key),
                    dry_run: false,
                    mod_time: None,
                });
            }
            removed += chunk.len() as u64;
        }
    }
    Ok(removed)
}

async fn stat(args: StatArgs) -> Result<()> {
    if args.recursive {
        return Err(anyhow!("stat --recursive is not implemented yet"));
    }
    for target in args.targets {
        let parsed = parse_s3_url(&target)?;
        let bucket = parsed
            .bucket
            .ok_or_else(|| anyhow!("bucket is required in target `{target}`"))?;
        let key = parsed
            .key
            .ok_or_else(|| anyhow!("object key is required in target `{target}`"))?;
        let (client, _) = client_for_alias(&parsed.alias).await?;
        let resp = client.head_object().bucket(bucket).key(&key).send().await?;
        let date = resp
            .last_modified()
            .map(smithy_to_chrono)
            .unwrap_or_else(epoch);
        let metadata: BTreeMap<String, String> = resp
            .metadata()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        print_msg(&StatMessage {
            key,
            date,
            size: resp.content_length().unwrap_or_default() as u64,
            etag: strip_etag_quotes(resp.e_tag().unwrap_or_default()),
            content_type: resp.content_type().map(str::to_string),
            metadata,
        });
    }
    Ok(())
}
