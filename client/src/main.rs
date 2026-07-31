mod attr;
mod budget;
mod config;
mod diff;
mod findcmd;
mod list;
mod messages;
mod mirror;
mod output;
mod progress;
mod share;
mod timefilter;
mod transfer;
mod urls;

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::Client;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

use output::{OutputOpts, init_output, out, print_error, print_msg};

use config::{Alias, client_for_alias, load_config, save_config};
use list::ObjectPaginator;
use messages::{
    ContentMessage, CopyMessage, DuMessage, MakeBucketMessage, PipeMessage, RemoveBucketMessage,
    RmMessage, StatMessage, SummaryMessage, TransferSession, TreeMessage,
};
use transfer::{download_object, transfer_object_between_s3, upload_file, upload_stream};
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
    #[command(about = "move objects")]
    Mv(CpArgs),
    #[command(about = "get s3 object to local")]
    Get(GetArgs),
    #[command(about = "display object contents")]
    Cat(CatArgs),
    #[command(about = "display first n lines of an object")]
    Head(HeadArgs),
    #[command(about = "remove object(s)")]
    Rm(RmArgs),
    #[command(about = "show object metadata")]
    Stat(StatArgs),
    #[command(about = "synchronize object(s) to a remote site")]
    Mirror(MirrorArgs),
    #[command(about = "summarize disk usage recursively")]
    Du(DuArgs),
    #[command(about = "list buckets and objects in a tree format")]
    Tree(TreeArgs),
    #[command(about = "stream stdin to an object (or, with no target, to stdout)")]
    Pipe(PipeArgs),
    #[command(about = "list differences in object name, size, and date between two folders")]
    Diff(DiffArgs),
    #[command(about = "search for objects")]
    Find(FindArgs),
    #[command(about = "generate URL for temporary access to an object")]
    Share(ShareArgs),
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
pub(crate) struct ShareArgs {
    #[command(subcommand)]
    pub(crate) command: ShareCommand,
}

#[derive(Subcommand, Debug)]
pub(crate) enum ShareCommand {
    #[command(about = "generate URL for download access")]
    Download(ShareDownloadArgs),
    #[command(
        about = "generate a `curl` command to upload objects without requiring access/secret keys"
    )]
    Upload(ShareUploadArgs),
    #[command(about = "list previously shared objects", visible_alias = "ls")]
    List(ShareListArgs),
}

#[derive(Args, Debug)]
pub(crate) struct ShareDownloadArgs {
    #[arg(short = 'r', long, help = "share all objects recursively")]
    pub(crate) recursive: bool,
    #[arg(
        short = 'E',
        long,
        default_value = "168h",
        help = "set expiry in NN[h|m|s]"
    )]
    pub(crate) expire: String,
    #[arg(required = true)]
    pub(crate) targets: Vec<String>,
}

#[derive(Args, Debug)]
pub(crate) struct ShareUploadArgs {
    #[arg(
        short = 'r',
        long,
        help = "recursively upload any object matching the prefix"
    )]
    pub(crate) recursive: bool,
    #[arg(
        short = 'E',
        long,
        default_value = "168h",
        help = "set expiry in NN[h|m|s]"
    )]
    pub(crate) expire: String,
    #[arg(
        short = 'T',
        long = "content-type",
        help = "specify a content-type to allow"
    )]
    pub(crate) content_type: Option<String>,
    #[arg(required = true)]
    pub(crate) targets: Vec<String>,
}

#[derive(Args, Debug)]
pub(crate) struct ShareListArgs {
    #[arg(help = "one of `upload` or `download`")]
    pub(crate) kind: String,
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
    #[arg(long, help = "add custom metadata for the object")]
    attr: Option<String>,
    #[arg(long, help = "upload only if the object does not already exist")]
    if_not_exists: bool,
    #[arg(
        short = 'a',
        long,
        help = "preserve filesystem attributes (mode, ownership, timestamps)"
    )]
    preserve: bool,
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
    #[arg(long, help = "add custom metadata for the object")]
    attr: Option<String>,
    #[arg(
        short = 'a',
        long,
        help = "preserve filesystem attributes (mode, ownership, timestamps)"
    )]
    preserve: bool,
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
    #[arg(long, help = "add custom metadata for the object")]
    pub(crate) attr: Option<String>,
    #[arg(
        short = 'a',
        long,
        help = "preserve filesystem attributes (mode, ownership, timestamps)"
    )]
    pub(crate) preserve: bool,
    /// Not a CLI flag -- set only when `mv` drives the mirror machinery for
    /// a recursive move ([SEM] §3): after each object's copy succeeds, its
    /// source is deleted. Delete failures are logged but do not affect the
    /// operation's overall exit status.
    #[arg(skip)]
    pub(crate) delete_source_after: bool,
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
    #[arg(long = "part-number")]
    part_number: Option<i32>,
    targets: Vec<String>,
}

#[derive(Args, Debug)]
struct HeadArgs {
    #[arg(
        short = 'n',
        long = "lines",
        default_value_t = 10,
        allow_hyphen_values = true
    )]
    lines: i64,
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

#[derive(Args, Debug)]
struct DuArgs {
    /// mc can't tell `-d 0` apart from "not set" ([SEM] §5's `mainDu`
    /// resolution): both fall through to the same default-depth branch, so
    /// treating `0` as "unset" here reproduces that quirk faithfully rather
    /// than special-casing it away.
    #[arg(short = 'd', long, default_value_t = 0)]
    depth: i32,
    #[arg(short = 'r', long)]
    recursive: bool,
    targets: Vec<String>,
}

/// `target` is a single **required** positional, not `Option<String>`:
/// ground-truth-verified against the real `mc` binary (RELEASE.2025-08-13,
/// `cmd/pipe-main.go`'s `checkPipeSyntax`), `mc pipe` with zero args prints
/// its full command help to stdout and exits `1` -- it does *not*
/// passthrough. The "no target -> `cat` stdin to stdout" behavior
/// documented in `mc-research-semantics.md` §8 is real but only reachable
/// via a single **empty-string** argument (`mc pipe ""`, confirmed against
/// the real binary): `checkPipeSyntax` requires `len(args) == 1` before
/// `mainPipe` ever looks at whether that one argument is empty. `pipe()`
/// below reproduces that exact shape (see its doc comment) rather than
/// making `target` optional at the clap level.
#[derive(Args, Debug)]
struct PipeArgs {
    #[arg(long = "storage-class", visible_alias = "sc")]
    storage_class: Option<String>,
    #[arg(long, help = "add custom metadata for the object")]
    attr: Option<String>,
    /// Default is **528MiB**, not the naively-expected 16MiB. Ground-truth
    /// verified against the real `mc` binary's own `--help` output
    /// (`default: "528 MiB"`) and independently re-derived by hand from
    /// vendored `minio-go/v7`'s `OptimalPartInfo(-1, 0)`
    /// (`api-put-object-common.go`): with `objectSize == -1` substituted to
    /// `maxMultipartPutObjectSize` (5TiB) and `configuredPartSize == 0`
    /// substituted to `minPartSize` (16MiB), the unknown-size branch computes
    /// `ceil((5TiB / 10000) / 16MiB) * 16MiB` = `ceil(32.768) * 16MiB` =
    /// `33 * 16MiB` = `528MiB` -- i.e. `defaultPartSize()`'s 16MiB
    /// `minPartSize` constant is only an input to that formula, not the
    /// final default itself. `mc-research-semantics.md` §8 stops its
    /// analysis at the 16MiB constant and is wrong about the actual
    /// default; this flag's default corrects that.
    #[arg(
        short = 's',
        long,
        default_value = "528MiB",
        help = "each part size (pipe's total size is unknown up front, unlike put/cp)"
    )]
    part_size: String,
    #[arg(
        long,
        default_value_t = 1,
        help = "allow N concurrent uploads [WARNING: will use more memory, use it with caution]"
    )]
    concurrent: usize,
    target: String,
}

#[derive(Args, Debug)]
struct TreeArgs {
    #[arg(short = 'f', long)]
    files: bool,
    /// `-1` (unlimited) is the only negative value accepted; `0` and any
    /// other negative value are rejected by [`tree`]'s own validation
    /// ([SEM] §6's `parseTreeSyntax`), not by clap itself.
    #[arg(short = 'd', long, default_value_t = -1, allow_negative_numbers = true)]
    depth: i32,
    targets: Vec<String>,
}

/// `diff FIRST SECOND`: exactly two positionals, no flags of its own
/// ([SEM] §9 -- `diffFlags = []cli.Flag{}` beyond the CLI globals).
#[derive(Args, Debug)]
struct DiffArgs {
    first: String,
    second: String,
}

/// `find TARGET [FLAGS]` ([SEM] §7). `--watch`/`--metadata`/`--tags` are
/// deliberately *not* declared here -- clap rejects them as unknown flags,
/// which is the whole refusal mechanism for those three (no runtime code
/// needed). `target` is a single required positional: unlike `ls`/`du`,
/// real `mc find` has no rs3-relevant local-filesystem behavior worth
/// replicating (see [`findcmd::run_find`]'s local-target guard), so this
/// only ever resolves against an S3 alias.
#[derive(Args, Debug)]
pub(crate) struct FindArgs {
    #[arg(long, help = "spawn an external process for each matching object")]
    pub(crate) exec: Option<String>,
    #[arg(long, help = "exclude objects matching the wildcard pattern")]
    pub(crate) ignore: Option<String>,
    #[arg(long, help = "find object names matching wildcard pattern")]
    pub(crate) name: Option<String>,
    #[arg(long, help = "match all objects newer than value in duration string")]
    pub(crate) newer_than: Option<String>,
    #[arg(long, help = "match all objects older than value in duration string")]
    pub(crate) older_than: Option<String>,
    #[arg(long, help = "match directory names matching wildcard pattern")]
    pub(crate) path: Option<String>,
    #[arg(long, help = "print in custom format to STDOUT")]
    pub(crate) print: Option<String>,
    #[arg(long, help = "match directory and object name with RE2 regex pattern")]
    pub(crate) regex: Option<String>,
    #[arg(long, help = "match all objects larger than specified size in units")]
    pub(crate) larger: Option<String>,
    #[arg(long, help = "match all objects smaller than specified size in units")]
    pub(crate) smaller: Option<String>,
    #[arg(long, help = "limit directory navigation to specified depth")]
    pub(crate) maxdepth: Option<u32>,
    #[arg(required = true)]
    pub(crate) target: String,
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
        Commands::Mv(args) => mv(args).await,
        Commands::Get(args) => get(args).await,
        Commands::Cat(args) => cat(args).await,
        Commands::Head(args) => head(args).await,
        Commands::Rm(args) => rm(args).await,
        Commands::Stat(args) => stat(args).await,
        Commands::Mirror(args) => mirror(args).await,
        Commands::Du(args) => du(args).await,
        Commands::Tree(args) => tree(args).await,
        Commands::Pipe(args) => pipe(args).await,
        Commands::Diff(args) => diff(args).await,
        Commands::Find(args) => findcmd::run_find(args).await,
        Commands::Share(args) => share::run_share(args).await,
    }
}

async fn diff(args: DiffArgs) -> Result<()> {
    diff::run_diff(&args.first, &args.second).await
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
                    extra: BTreeMap::new(),
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
    if args.preserve && !cfg!(unix) {
        return Err(anyhow!("--preserve is not supported on this platform"));
    }
    let part_size = parse_size(&args.part_size)?;
    let metadata = match args.attr.as_deref() {
        Some(spec) => attr::parse_attrs(spec)?,
        None => BTreeMap::new(),
    };
    let session = TransferSession::new("put");
    let outcome = upload_file(
        &args.source,
        &args.target,
        part_size,
        args.parallel,
        args.disable_multipart,
        args.storage_class.as_deref(),
        &metadata,
        args.if_not_exists,
        args.preserve,
        session.ui(),
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
    run_cp_or_mv(args, false).await
}

/// `mv` = `cp` plus a per-object delete of the source once that object's
/// copy has succeeded ([SEM] §3). Non-transactional: a copy failure leaves
/// the source in place and fails the whole operation exactly like `cp`; a
/// delete failure is logged to stderr but does not affect exit status.
async fn mv(args: CpArgs) -> Result<()> {
    // Up-front, mv-only guard ([SEM] §3): only checked when there's exactly
    // one source and one target (2 positional args total).
    if args.paths.len() == 2 {
        check_mv_subdirectory_guard(&args.paths[0], &args.paths[1])?;
    }
    run_cp_or_mv(args, true).await
}

/// `mv`-only guard: fatal if `source`/`target` are subdirectories of each
/// other (either nested inside the other, or identical). Applies when both
/// sides are S3 URLs (string-prefix compare on the raw alias/bucket/key
/// path with a `/` boundary) or both are local paths (`Path::starts_with`
/// both directions) -- a mixed local/S3 pair can never collide this way.
fn check_mv_subdirectory_guard(source: &str, target: &str) -> Result<()> {
    let conflict = if is_s3_url(source) && is_s3_url(target) {
        let a = source.trim_end_matches('/');
        let b = target.trim_end_matches('/');
        a == b || b.starts_with(&format!("{a}/")) || a.starts_with(&format!("{b}/"))
    } else if !is_s3_url(source) && !is_s3_url(target) {
        let a = Path::new(source);
        let b = Path::new(target);
        a.starts_with(b) || b.starts_with(a)
    } else {
        false
    };
    if conflict {
        return Err(anyhow!(
            "The source `{source}` and destination `{target}` cannot be subdirectories of each other"
        ));
    }
    Ok(())
}

async fn run_cp_or_mv(args: CpArgs, is_mv: bool) -> Result<()> {
    timefilter::validate_time_filters(args.older_than.as_deref(), args.newer_than.as_deref())?;
    if args.paths.len() < 2 {
        let verb = if is_mv { "mv" } else { "cp" };
        return Err(anyhow!("{verb} requires SOURCE [SOURCE...] TARGET"));
    }
    if args.preserve && !cfg!(unix) {
        return Err(anyhow!("--preserve is not supported on this platform"));
    }
    let part_size = parse_size(&args.part_size)?;
    let attrs = match args.attr.as_deref() {
        Some(spec) => attr::parse_attrs(spec)?,
        None => BTreeMap::new(),
    };
    let target = args.paths.last().unwrap().clone();
    let session = TransferSession::new(if is_mv { "mv" } else { "cp" });
    let mut used_session = false;
    for source in &args.paths[..args.paths.len() - 1] {
        if is_s3_url(source) && is_s3_url(&target) {
            if args.recursive {
                cp_or_mv_recursive(source, &target, &args, is_mv).await?;
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
                    &attrs,
                    args.preserve,
                    is_mv,
                )
                .await?;
                used_session = true;
            }
        } else if is_s3_url(source) && !is_s3_url(&target) {
            if args.recursive {
                cp_or_mv_recursive(source, &target, &args, is_mv).await?;
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
                        args.preserve,
                    )
                    .await?;
                    if is_mv {
                        // Delete only after the copy of *this* object has
                        // succeeded; a delete failure is logged but must not
                        // fail the overall `mv` exit status ([SEM] §3).
                        if let Err(err) = head_client
                            .delete_object()
                            .bucket(&bucket)
                            .key(&key)
                            .send()
                            .await
                        {
                            eprintln!("mv: remove `{bucket}/{key}` failed: {err}");
                        }
                    }
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
                cp_or_mv_recursive(source, &target, &args, is_mv).await?;
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
                        &attrs,
                        false,
                        args.preserve,
                        session.ui(),
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
                    if is_mv {
                        if let Err(err) = fs::remove_file(source_path).await {
                            eprintln!("mv: remove `{}` failed: {err}", source_path.display());
                        }
                    }
                }
                used_session = true;
            }
        } else {
            if args.preserve {
                return Err(anyhow!(
                    "cp --preserve for local-to-local copies is not implemented yet"
                ));
            }
            copy_local_path(
                Path::new(source),
                Path::new(&target),
                args.recursive,
                &session,
                args.older_than.as_deref(),
                args.newer_than.as_deref(),
                is_mv,
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
/// the mirror planner with `overwrite: true` and `remove: false`. For `mv`,
/// `delete_source_after` additionally tells the mirror copy loop to delete
/// each source object right after its own copy succeeds; a local source's
/// now-empty directories are pruned best-effort afterward (S3 sources have
/// no directories to prune -- their per-object deletes are sufficient).
async fn cp_or_mv_recursive(source: &str, target: &str, args: &CpArgs, is_mv: bool) -> Result<()> {
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
        attr: args.attr.clone(),
        preserve: args.preserve,
        delete_source_after: is_mv,
        source: source.to_string(),
        target: target.to_string(),
    };
    mirror::run_mirror(&mirror_args).await?;
    if is_mv && !is_s3_url(source) {
        prune_empty_dirs(Path::new(source)).await;
    }
    Ok(())
}

/// Best-effort removal of now-empty directories under `root` (including
/// `root` itself), deepest first, after a recursive local-source `mv`. Any
/// directory that's still non-empty (a file survived, e.g. because its copy
/// failed) is silently left in place -- this is cleanup, not part of the
/// operation's success/failure signal.
async fn prune_empty_dirs(root: &Path) {
    let mut stack = vec![root.to_path_buf()];
    let mut dirs = Vec::new();
    while let Some(dir) = stack.pop() {
        let Ok(mut entries) = fs::read_dir(&dir).await else {
            continue;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Ok(metadata) = entry.metadata().await {
                if metadata.is_dir() {
                    stack.push(entry.path());
                }
            }
        }
        dirs.push(dir);
    }
    dirs.sort_by_key(|d| std::cmp::Reverse(d.components().count()));
    for dir in dirs {
        let _ = fs::remove_dir(&dir).await;
    }
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
    attrs: &BTreeMap<String, String>,
    preserve: bool,
    delete_source: bool,
) -> Result<()> {
    if !attrs.is_empty() {
        return Err(anyhow!("--attr for S3-to-S3 copies is not implemented yet"));
    }
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
        preserve,
        session.ui(),
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
    if delete_source {
        // Copy of this object has already succeeded above; delete failures
        // are logged but must not fail the overall `mv` exit status.
        if let Err(err) = source_client
            .delete_object()
            .bucket(&source_bucket)
            .key(&source_key)
            .send()
            .await
        {
            eprintln!(
                "mv: remove `{}/{source_bucket}/{source_key}` failed: {err}",
                source_url.alias
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn copy_local_path(
    source: &Path,
    target: &Path,
    recursive: bool,
    session: &TransferSession,
    older_than: Option<&str>,
    newer_than: Option<&str>,
    delete_after: bool,
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
                    if delete_after {
                        if let Err(err) = fs::remove_file(&path).await {
                            eprintln!("mv: remove `{}` failed: {err}", path.display());
                        }
                    }
                }
            }
        }
        if delete_after {
            prune_empty_dirs(source).await;
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
        if delete_after {
            if let Err(err) = fs::remove_file(source).await {
                eprintln!("mv: remove `{}` failed: {err}", source.display());
            }
        }
    }
    Ok(())
}

async fn get(args: GetArgs) -> Result<()> {
    if args.version_id.is_some() {
        return Err(anyhow!("get --version-id is not implemented yet"));
    }
    let session = TransferSession::new("get");
    // GetArgs has no `--preserve` flag ([SEM] §2 lists it on cp/mirror/put
    // only), so `get` never preserves filesystem attributes.
    download_object(
        &args.source,
        args.target,
        DEFAULT_PART_SIZE,
        4,
        &session,
        false,
    )
    .await?;
    session.finish();
    Ok(())
}

/// `cat`: streams each target's body to stdout, optionally range-restricted
/// ([SEM] §12). `--offset N` becomes a `Range: bytes=N-` GET; `--tail N` is
/// client-side sugar computed from a prior `HeadObject` call --
/// `RangeStart = max(size - N, 0)`, so a tail larger than the object just
/// returns the whole thing (matches POSIX `tail -c`). `--part-number`
/// selects a specific part of a multipart-uploaded object via
/// `x-amz-part-number` and is mutually exclusive with both `--tail` and
/// `--offset`. Mutual-exclusion / validation is checked up front, verbatim
/// against mc's `parseCatSyntax`: `--tail`+`--offset` together, either
/// negative, or `--part-number` combined with either, are all fatal.
async fn cat(args: CatArgs) -> Result<()> {
    if args.offset.is_some() && args.tail.is_some() {
        return Err(anyhow!("You cannot specify both --tail and --offset"));
    }
    if args.offset.is_some_and(|o| o < 0) || args.tail.is_some_and(|t| t < 0) {
        return Err(anyhow!("You cannot specify negative --tail or --offset"));
    }
    if args.part_number.is_some() && (args.offset.is_some() || args.tail.is_some()) {
        return Err(anyhow!(
            "You cannot use --part-number with --tail or --offset"
        ));
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

        let mut start = args.offset.unwrap_or(0);
        if let Some(tail) = args.tail {
            if tail > 0 {
                let head = client
                    .head_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await?;
                let size = head.content_length().unwrap_or(0);
                start = (size - tail).max(0);
            }
        }

        let mut req = client.get_object().bucket(&bucket).key(&key);
        if start > 0 {
            req = req.range(format!("bytes={start}-"));
        }
        if let Some(part_number) = args.part_number {
            req = req.part_number(part_number);
        }
        let resp = req.send().await?;
        let mut reader = resp.body.into_async_read();
        let mut stdout = tokio::io::stdout();
        // mc treats EPIPE on stdout (reader side closed, e.g. piping into
        // `head`) as a clean/silent success, not an error ([SEM] §12).
        match tokio::io::copy(&mut reader, &mut stdout).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        if let Err(e) = stdout.flush().await {
            if e.kind() == std::io::ErrorKind::BrokenPipe {
                return Ok(());
            }
            return Err(e.into());
        }
    }
    Ok(())
}

/// `pipe`: streams stdin to `TARGET` -- or, when `TARGET` is the **empty
/// string** (`rs3 pipe ""`), straight through to stdout, exactly like
/// `cat`. This mirrors real `mc`'s exact, ground-truth-verified shape
/// ([`PipeArgs`]'s doc comment): `TARGET` is a required positional (clap
/// itself rejects zero or 2+ of them, matching `checkPipeSyntax`'s
/// `len(args) != 1` gate), and *only once exactly one argument is present*
/// does its content (empty vs. non-empty) pick cat-passthrough vs. upload
/// -- there is no "omit the argument entirely" passthrough shape in real
/// `mc`, despite `mc-research-semantics.md` §8 describing one.
/// `--json`/`--quiet` have no effect on the passthrough case, same as
/// `cat`/`head`. With a non-empty target, total size is unknown up front,
/// so the default part size is **528MiB** (`--part-size`'s own displayed
/// default -- see [`PipeArgs::part_size`]'s doc comment for the full
/// derivation), deliberately different from `put`/`cp`'s 256MiB -- see
/// [`transfer::upload_stream`]. `--concurrent N` bounds in-flight
/// `UploadPart` calls; mc's own flag help warns this trades memory for
/// throughput, so no clamp is applied here beyond the usual 5MiB part-size
/// floor. mc has no `--tags` support surfaced through this struct at all
/// ([SEM] §8 lists it as a real `pipe` flag, but rs3 refuses tags
/// everywhere else in this codebase, so it's simply absent here too, same
/// as the rest of rs3's tier-2 commands).
async fn pipe(args: PipeArgs) -> Result<()> {
    if args.target.is_empty() {
        let mut stdin = tokio::io::stdin();
        let mut stdout = tokio::io::stdout();
        match tokio::io::copy(&mut stdin, &mut stdout).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        if let Err(e) = stdout.flush().await {
            if e.kind() == std::io::ErrorKind::BrokenPipe {
                return Ok(());
            }
            return Err(e.into());
        }
        return Ok(());
    }
    let target = args.target;
    let part_size = parse_size(&args.part_size)?;
    let metadata = match args.attr.as_deref() {
        Some(spec) => attr::parse_attrs(spec)?,
        None => BTreeMap::new(),
    };
    let parsed = parse_s3_url(&target)?;
    let bucket = parsed
        .bucket
        .ok_or_else(|| anyhow!("bucket is required in target `{target}`"))?;
    let key = parsed
        .key
        .ok_or_else(|| anyhow!("object key is required in target `{target}`"))?;
    let (client, _) = client_for_alias(&parsed.alias).await?;
    let mut stdin = tokio::io::stdin();
    let size = upload_stream(
        &client,
        &mut stdin,
        &bucket,
        &key,
        part_size,
        args.concurrent.max(1),
        &metadata,
        args.storage_class.as_deref(),
    )
    .await?;
    // Unlike `cp`/`put`/`get`/`mirror`, real `mc pipe` has no accounting
    // wrapper at all (`cmd/pipe-main.go`'s `pipe()` calls `printMsg`
    // directly, once, on success -- no `newProgressBar`/`accountStat`
    // summary line the way the other transfer commands' non-bar path
    // does). Ground-truth verified: `mc pipe`/`mc --json pipe` both emit
    // exactly one line. A `TransferSession` here would print a second
    // `AccountStat` line real `mc` never does -- so this intentionally
    // does NOT go through `TransferSession`, printing `PipeMessage`
    // directly instead.
    print_msg(&PipeMessage {
        target: format!("{}/{bucket}/{key}", parsed.alias),
        size,
    });
    Ok(())
}

/// Strips a trailing line terminator from a `read_until(b'\n', ..)` buffer:
/// the `\n` itself (if present, i.e. not the final unterminated line at
/// EOF) and, if the source used CRLF, the preceding `\r` too ([SEM] §4:
/// `head` always re-emits Unix `\n` regardless of the source's line
/// ending).
fn strip_line_terminator(mut buf: Vec<u8>) -> Vec<u8> {
    if buf.last() == Some(&b'\n') {
        buf.pop();
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
    }
    buf
}

/// Reads up to `lines` lines off an async buffered reader and writes each
/// one to stdout followed by an explicit `\n`, stopping (and dropping the
/// underlying stream without draining it) once `lines` is reached or the
/// stream ends -- mirrors mc's `bufio.Reader.ReadLine()` loop in
/// `headURL` ([SEM] §4).
async fn head_stream_async<R>(reader: &mut R, lines: i64) -> Result<()>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut stdout = tokio::io::stdout();
    let mut count = 0i64;
    while count < lines {
        let mut buf = Vec::new();
        let n = reader.read_until(b'\n', &mut buf).await?;
        if n == 0 {
            break;
        }
        let buf = strip_line_terminator(buf);
        stdout.write_all(&buf).await?;
        stdout.write_all(b"\n").await?;
        count += 1;
    }
    stdout.flush().await?;
    Ok(())
}

/// Same line-limited read loop as [`head_stream_async`], but over a
/// synchronous `BufRead` -- used for the gzip-decoded path, where the
/// whole (typically small) compressed object has already been buffered in
/// memory and run through `flate2::read::MultiGzDecoder`.
fn head_stream_sync<R: std::io::BufRead>(reader: &mut R, lines: i64) -> Result<()> {
    use std::io::Write;
    let mut stdout = std::io::stdout();
    let mut count = 0i64;
    while count < lines {
        let mut buf = Vec::new();
        let n = reader.read_until(b'\n', &mut buf)?;
        if n == 0 {
            break;
        }
        let buf = strip_line_terminator(buf);
        stdout.write_all(&buf)?;
        stdout.write_all(b"\n")?;
        count += 1;
    }
    stdout.flush()?;
    Ok(())
}

/// `head`: full un-ranged GET per target, then a client-side first-N-lines
/// loop that stops early and drops the stream ([SEM] §4). `--json` has no
/// effect on this command's payload ([OUT] gotcha 14: `head`/`cat` sit
/// entirely outside the message/printMsg system). `--rewind`,
/// `--version-id`, and `--zip` are not implemented -- rather than accept
/// and hard-error on them, they are simply absent from `HeadArgs`, so
/// clap itself rejects them with its normal "unexpected argument" error;
/// simpler than a bespoke refusal message for flags this command never
/// otherwise touches. Likewise, a stdin/`-` fallback for "no targets" is
/// out of scope here (that's pipe's territory in a later task) -- with no
/// targets clap's own "required" error fires instead.
async fn head(args: HeadArgs) -> Result<()> {
    // mc's own defensive reset: an explicit negative -n silently becomes
    // the default of 10 (the flag's own default already covers "unset").
    let lines = if args.lines < 0 { 10 } else { args.lines };
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
        let content_type = resp.content_type().unwrap_or_default().to_ascii_lowercase();
        if content_type.contains("bzip") {
            return Err(anyhow!(
                "head: bzip2-compressed objects are not supported yet"
            ));
        }
        if content_type.contains("gzip") {
            // No streaming gzip bridge between the SDK's async body and a
            // sync flate2 decoder here -- buffer the whole (head-sized,
            // typically small) compressed object in memory first. Fine for
            // `head`'s use case; would need revisiting for arbitrarily
            // large gzip objects.
            let mut reader = resp.body.into_async_read();
            let mut compressed = Vec::new();
            reader.read_to_end(&mut compressed).await?;
            let decoder = flate2::read::MultiGzDecoder::new(std::io::Cursor::new(compressed));
            let mut buf_reader = std::io::BufReader::new(decoder);
            head_stream_sync(&mut buf_reader, lines)?;
        } else {
            let mut reader = tokio::io::BufReader::new(resp.body.into_async_read());
            head_stream_async(&mut reader, lines).await?;
        }
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

/// `stat`: prints a full metadata block per target. `--recursive` walks the
/// target as a prefix (like `ls -r`) and prints one block per object under
/// it instead of stat'ing a single key ([SEM] §13); an empty listing is a
/// quiet success (matches `ls`'s own empty-prefix behavior), not an error --
/// only the non-recursive single-key path treats a miss as fatal.
async fn stat(args: StatArgs) -> Result<()> {
    for target in args.targets {
        let parsed = parse_s3_url(&target)?;
        let bucket = parsed
            .bucket
            .ok_or_else(|| anyhow!("bucket is required in target `{target}`"))?;
        let (client, _) = client_for_alias(&parsed.alias).await?;
        if args.recursive {
            let prefix = parsed.key.unwrap_or_default();
            let objects = list::collect_objects(&client, &bucket, &prefix).await?;
            for obj in objects {
                stat_one(&client, &parsed.alias, &bucket, &obj.key).await?;
            }
        } else {
            let key = parsed
                .key
                .ok_or_else(|| anyhow!("object key is required in target `{target}`"))?;
            stat_one(&client, &parsed.alias, &bucket, &key).await?;
        }
    }
    Ok(())
}

/// Shared `HeadObject` + [`StatMessage`] print used by both the single-key
/// and `--recursive` paths of `stat`.
async fn stat_one(client: &Client, alias: &str, bucket: &str, key: &str) -> Result<()> {
    let resp = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .with_context(|| format!("stat `{alias}/{bucket}/{key}`"))?;
    let date = resp
        .last_modified()
        .map(smithy_to_chrono)
        .unwrap_or_else(epoch);
    let metadata: BTreeMap<String, String> = resp
        .metadata()
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    print_msg(&StatMessage {
        key: key.to_string(),
        date,
        size: resp.content_length().unwrap_or_default() as u64,
        etag: strip_etag_quotes(resp.e_tag().unwrap_or_default()),
        content_type: resp.content_type().map(str::to_string),
        cache_control: resp.cache_control().map(str::to_string),
        metadata,
    });
    Ok(())
}

/// `du`: recursive disk-usage summary matching mc's exact depth semantics
/// ([SEM] §5, `mainDu`). Depth resolution: an explicit non-zero `-d` wins;
/// otherwise `-r` means unlimited (`-1`); otherwise the default is `1` (a
/// single flat total for the whole target). mc's `-d 0` is indistinguishable
/// from "unset" (`cliCtx.Int("depth") == 0` can't tell "the user typed `-d
/// 0`" from "the flag was never given"), so it falls through to the same
/// default here too, rather than being treated as its own case.
async fn du(args: DuArgs) -> Result<()> {
    if args.targets.is_empty() {
        return Err(anyhow!("du: this command requires at least one argument"));
    }
    let effective_depth = if args.depth != 0 {
        args.depth
    } else if args.recursive {
        -1
    } else {
        1
    };
    let mut first_err = None;
    for target in &args.targets {
        let parsed = parse_s3_url(target)?;
        let bucket = parsed.bucket.clone().ok_or_else(|| {
            anyhow!("du: `{target}` is not a folder. Only folders are supported by 'du' command.")
        })?;
        let (client, _) = client_for_alias(&parsed.alias).await?;
        let prefix = parsed.key.unwrap_or_default();
        if let Err(e) = du_walk(&client, &parsed.alias, &bucket, &prefix, effective_depth).await
            && first_err.is_none()
        {
            first_err = Some(e);
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Recursive walk mirroring mc's `du()` (`cmd/du-main.go`, [SEM] §5): a
/// `depth` of exactly `1` does one flat recursive listing and sums it into a
/// single number -- the terminal/leaf case, "no more per-level breakdown
/// below here". Any other depth does a non-recursive (delimiter) listing,
/// sums the files found directly at this level, and recurses into every
/// subdirectory with `depth - 1` (a positive depth counts down; `-1` --
/// unlimited -- and an already-exhausted `0` both stay put, since mc only
/// decrements `depth` while it's `> 0`). A line is printed for every level
/// *except* when the current call's `depth` is exactly `0`: that level's
/// total still folds into its parent's sum via the return value, it just
/// isn't printed on its own (mc's `if depth != 0 { printMsg(...) }`).
///
/// Boxed because an `async fn` can't recurse into itself directly (its
/// future would need to contain itself, which isn't a statically sized
/// type).
fn du_walk<'a>(
    client: &'a Client,
    alias: &'a str,
    bucket: &'a str,
    prefix: &'a str,
    depth: i32,
) -> Pin<Box<dyn Future<Output = Result<(u64, u64)>> + 'a>> {
    Box::pin(async move {
        let mut size = 0u64;
        let mut objects = 0u64;
        if depth == 1 {
            for obj in list::collect_objects(client, bucket, prefix).await? {
                size += obj.size;
                objects += 1;
            }
        } else {
            let mut token: Option<String> = None;
            let mut subdirs: Vec<String> = Vec::new();
            loop {
                let resp = client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(prefix)
                    .delimiter("/")
                    .set_continuation_token(token.take())
                    .send()
                    .await?;
                for obj in resp.contents() {
                    let key = obj.key().unwrap_or_default();
                    let obj_size = obj.size().unwrap_or_default() as u64;
                    if key.ends_with('/') && obj_size == 0 {
                        continue; // folder marker
                    }
                    size += obj_size;
                    objects += 1;
                }
                for p in resp.common_prefixes() {
                    if let Some(full) = p.prefix() {
                        subdirs.push(full.to_string());
                    }
                }
                if resp.is_truncated().unwrap_or(false) {
                    token = resp.next_continuation_token().map(String::from);
                    if token.is_none() {
                        break;
                    }
                } else {
                    break;
                }
            }
            let child_depth = if depth > 0 { depth - 1 } else { depth };
            for subdir in subdirs {
                let (sub_size, sub_objects) =
                    du_walk(client, alias, bucket, &subdir, child_depth).await?;
                size += sub_size;
                objects += sub_objects;
            }
        }
        if depth != 0 {
            print_msg(&DuMessage {
                prefix: du_display_prefix(alias, bucket, prefix),
                size,
                objects,
            });
        }
        Ok((size, objects))
    })
}

/// `alias/bucket[/prefix]` with any trailing slash trimmed off -- the
/// user-facing display form for a `du` line's prefix, matching how the
/// target argument itself is styled (rather than mc's own host-relative
/// `bucket/prefix` form, which drops the alias entirely).
fn du_display_prefix(alias: &str, bucket: &str, prefix: &str) -> String {
    let trimmed = prefix.trim_end_matches('/');
    if trimmed.is_empty() {
        format!("{alias}/{bucket}")
    } else {
        format!("{alias}/{bucket}/{trimmed}")
    }
}

/// `tree` glyph constants ([SEM] §6 / [OUT] §2's `treeEntry`/`treeLastEntry`/
/// `treeNext`/`treeLevel`). `TREE_OPEN`/`TREE_CLOSED` are the two-part
/// continuation columns (`treeNext+treeLevel` / `" "+treeLevel`) prepended to
/// every descendant of a still-open vs. already-closed ancestor branch.
const TREE_ENTRY: &str = "├─ ";
const TREE_LAST_ENTRY: &str = "└─ ";
const TREE_OPEN: &str = "│  ";
const TREE_CLOSED: &str = "   ";

/// One child in a `tree` directory listing.
struct TreeEntry {
    name: String,
    is_dir: bool,
}

/// A `tree` traversal position: either the alias root (whose children are
/// bucket names) or a prefix inside a specific bucket (whose children are a
/// delimiter listing). Verified against real `mc tree` ([task 11] ground
/// truth run): unlike `ls`, `tree` never `HeadObject`-probes a bare target --
/// it always lists the target as a directory prefix (appending `/` when
/// missing), so a target that names a plain object simply yields zero
/// children and prints nothing at all, not even the root line.
enum TreeNode {
    AliasRoot,
    Path { bucket: String, prefix: String },
}

impl TreeNode {
    fn child(&self, name: &str) -> TreeNode {
        match self {
            TreeNode::AliasRoot => TreeNode::Path {
                bucket: name.to_string(),
                prefix: String::new(),
            },
            TreeNode::Path { bucket, prefix } => TreeNode::Path {
                bucket: bucket.clone(),
                prefix: format!("{prefix}{name}/"),
            },
        }
    }
}

/// List the immediate children of `node`: for [`TreeNode::AliasRoot`], every
/// bucket (always a "directory"); for [`TreeNode::Path`], a non-recursive
/// delimiter listing split into files (only included when `files`) followed
/// by directories -- matching real `mc tree`'s observed per-directory order
/// (files sorted, then directories sorted; *not* a single lexicographic
/// merge across both -- confirmed against real `mc tree --files` on a
/// directory containing both a file and a directory that would sort earlier
/// than it, e.g. `0dir/` after `1.txt`/`ab.txt`).
async fn tree_list_children(
    client: &Client,
    node: &TreeNode,
    files: bool,
) -> Result<Vec<TreeEntry>> {
    match node {
        TreeNode::AliasRoot => {
            let resp = client.list_buckets().send().await?;
            let mut names: Vec<String> = resp
                .buckets()
                .iter()
                .filter_map(|b| b.name().map(String::from))
                .collect();
            names.sort();
            Ok(names
                .into_iter()
                .map(|name| TreeEntry { name, is_dir: true })
                .collect())
        }
        TreeNode::Path { bucket, prefix } => {
            let mut file_names: Vec<String> = Vec::new();
            let mut dir_names: Vec<String> = Vec::new();
            let mut token: Option<String> = None;
            loop {
                let resp = client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(prefix.as_str())
                    .delimiter("/")
                    .set_continuation_token(token.take())
                    .send()
                    .await?;
                for obj in resp.contents() {
                    let key = obj.key().unwrap_or_default();
                    let Some(name) = key.strip_prefix(prefix.as_str()) else {
                        continue;
                    };
                    if name.is_empty() {
                        continue; // folder-marker object exactly at this prefix
                    }
                    file_names.push(name.to_string());
                }
                for p in resp.common_prefixes() {
                    let Some(full) = p.prefix() else { continue };
                    let name = full
                        .strip_prefix(prefix.as_str())
                        .unwrap_or(full)
                        .trim_end_matches('/');
                    if name.is_empty() {
                        continue;
                    }
                    dir_names.push(name.to_string());
                }
                if resp.is_truncated().unwrap_or(false) {
                    token = resp.next_continuation_token().map(String::from);
                    if token.is_none() {
                        break;
                    }
                } else {
                    break;
                }
            }
            let mut entries = Vec::new();
            if files {
                entries.extend(file_names.into_iter().map(|name| TreeEntry {
                    name,
                    is_dir: false,
                }));
            }
            entries.extend(
                dir_names
                    .into_iter()
                    .map(|name| TreeEntry { name, is_dir: true }),
            );
            Ok(entries)
        }
    }
}

/// Print `entries` (the children of `node`, whose own line -- root or an
/// ancestor entry -- has already been printed) at tree `level`, recursing
/// into directory children while `depth == -1 || level <= depth` ([SEM] §6).
/// `continuation` is the already-built prefix of `│  `/`   ` columns
/// inherited from every still-open/closed ancestor branch; each entry here
/// prepends its own `├─ `/`└─ ` glyph to it.
///
/// Boxed for the same reason as `du_walk`: async recursion needs a
/// heap-allocated, pinned future.
fn tree_print_entries<'a>(
    client: &'a Client,
    node: TreeNode,
    entries: Vec<TreeEntry>,
    level: i32,
    depth: i32,
    files: bool,
    continuation: String,
) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
    Box::pin(async move {
        let last_idx = entries.len().checked_sub(1);
        for (i, entry) in entries.into_iter().enumerate() {
            let is_last = Some(i) == last_idx;
            let glyph = if is_last { TREE_LAST_ENTRY } else { TREE_ENTRY };
            print_msg(&TreeMessage {
                entry: entry.name.clone(),
                is_dir: entry.is_dir,
                branch_string: format!("{continuation}{glyph}"),
            });
            if entry.is_dir && (depth == -1 || level <= depth) {
                let child = node.child(&entry.name);
                let child_entries = tree_list_children(client, &child, files).await?;
                let child_continuation = format!(
                    "{continuation}{}",
                    if is_last { TREE_CLOSED } else { TREE_OPEN }
                );
                tree_print_entries(
                    client,
                    child,
                    child_entries,
                    level + 1,
                    depth,
                    files,
                    child_continuation,
                )
                .await?;
            }
        }
        Ok(())
    })
}

/// Walk one `tree` target: prints the root line (the target exactly as
/// typed) followed by its descendants, but only if the target has at least
/// one (post-`--files`-filter) child -- an empty target prints *nothing*,
/// not even the root line (confirmed against real `mc tree` on an empty
/// bucket and on a bucket containing only files with `--files` unset).
async fn tree_walk(
    client: &Client,
    node: TreeNode,
    root_label: &str,
    depth: i32,
    files: bool,
) -> Result<()> {
    let entries = tree_list_children(client, &node, files).await?;
    if entries.is_empty() {
        return Ok(());
    }
    print_msg(&TreeMessage {
        entry: root_label.to_string(),
        is_dir: true,
        branch_string: String::new(),
    });
    tree_print_entries(client, node, entries, 1, depth, files, String::new()).await
}

/// `mc tree` ([SEM] §6): draws a directory tree of buckets/objects.
/// `--json` bypasses tree drawing entirely and aliases the exact `ls
/// --recursive --json` code path ([OUT] §2 gotcha 9) -- but only *after* the
/// depth validation below, which real `mc --json tree -d 0 ...` still
/// enforces before rerouting (verified against the real binary).
async fn tree(args: TreeArgs) -> Result<()> {
    if args.depth == 0 || args.depth < -1 {
        return Err(anyhow!(
            "please set a proper depth, for example: '--depth 1' to limit the tree output, default (-1) output displays everything"
        ));
    }
    if out().json {
        return ls(LsArgs {
            rewind: None,
            versions: false,
            recursive: true,
            incomplete: false,
            summarize: false,
            storage_class: None,
            zip: false,
            targets: args.targets,
        })
        .await;
    }
    if args.targets.is_empty() {
        return Err(anyhow!("tree: this command requires at least one argument"));
    }
    for target in &args.targets {
        let parsed = parse_s3_url(target)?;
        let (client, _) = client_for_alias(&parsed.alias).await?;
        let node = match parsed.bucket {
            None => TreeNode::AliasRoot,
            Some(bucket) => {
                let key = parsed.key.unwrap_or_default();
                let prefix = if key.is_empty() || key.ends_with('/') {
                    key
                } else {
                    format!("{key}/")
                };
                TreeNode::Path { bucket, prefix }
            }
        };
        tree_walk(&client, node, target, args.depth, args.files).await?;
    }
    Ok(())
}
