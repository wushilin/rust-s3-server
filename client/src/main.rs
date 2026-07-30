mod config;
mod list;
mod transfer;
mod urls;

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::Client;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use clap::{Args, Parser, Subcommand};
use humansize::{BINARY, format_size};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use config::{Alias, client_for_alias, load_config, save_config};
use list::{ObjectPaginator, collect_objects};
use transfer::{download_key_to_path, download_object, transfer_object_between_s3, upload_file};
use urls::{is_s3_url, join_key, join_s3_target, parse_s3_url, parse_size};

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
struct MirrorArgs {
    #[arg(short = 'P', long, default_value_t = 4)]
    parallel: usize,
    #[arg(short = 's', long, default_value = "256MiB", help = "each part size")]
    part_size: String,
    #[arg(long)]
    overwrite: bool,
    #[arg(long)]
    remove: bool,
    #[arg(long)]
    dry_run: bool,
    #[arg(long, alias = "fake")]
    fake: bool,
    #[arg(short = 'w', long)]
    watch: bool,
    #[arg(long)]
    disable_multipart: bool,
    source: String,
    target: String,
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
async fn main() -> Result<()> {
    let cli = Cli::parse();
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
                    println!("{}", bucket.name().unwrap_or_default());
                }
            }
            (Some(bucket), key) => {
                let prefix = key.unwrap_or_default();
                let delimiter = if args.recursive { None } else { Some("/") };
                let mut token = None;
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
                        println!("[DIR] {}", p.prefix().unwrap_or_default());
                    }
                    for obj in resp.contents() {
                        let size = obj.size().unwrap_or_default() as u64;
                        let key = obj.key().unwrap_or_default();
                        let modified = obj
                            .last_modified()
                            .map(|d| d.to_string())
                            .unwrap_or_else(|| "-".into());
                        println!("{modified} {:>12} {key}", format_size(size, BINARY));
                    }
                    if resp.is_truncated().unwrap_or(false) {
                        token = resp.next_continuation_token().map(String::from);
                    } else {
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn mb(args: MbArgs) -> Result<()> {
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
            Ok(_) => println!("Bucket created successfully `{target}`."),
            Err(err) => {
                let svc = err.as_service_error();
                let already_exists = svc.is_some_and(|e| {
                    e.is_bucket_already_owned_by_you() || e.is_bucket_already_exists()
                });
                if args.ignore_existing && already_exists {
                    println!("Bucket `{target}` already exists.");
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
                    for bucket in resp.buckets() {
                        let name = bucket.name().unwrap_or_default();
                        remove_bucket(&client, &parsed.alias, name, args.force).await?;
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
        remove_prefix(client, alias, bucket, "", false).await?;
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
    println!("Removed `{alias}/{bucket}` successfully.");
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
        } else {
            return Ok(());
        }
    }
}

async fn put(args: PutArgs) -> Result<()> {
    let part_size = parse_size(&args.part_size)?;
    upload_file(
        &args.source,
        &args.target,
        part_size,
        args.parallel,
        args.disable_multipart,
        args.storage_class.as_deref(),
    )
    .await
}

async fn cp(args: CpArgs) -> Result<()> {
    if args.paths.len() < 2 {
        return Err(anyhow!("cp requires SOURCE [SOURCE...] TARGET"));
    }
    let part_size = parse_size(&args.part_size)?;
    let target = args.paths.last().unwrap().clone();
    for source in &args.paths[..args.paths.len() - 1] {
        if is_s3_url(source) && is_s3_url(&target) {
            if args.recursive {
                mirror_s3_to_s3(
                    source,
                    &target,
                    part_size,
                    args.disable_multipart,
                    args.parallel,
                )
                .await?;
            } else {
                copy_s3_object_to_s3(
                    source,
                    &target,
                    part_size,
                    args.disable_multipart,
                    args.parallel,
                )
                .await?;
            }
        } else if is_s3_url(source) && !is_s3_url(&target) {
            if args.recursive {
                mirror_s3_to_local(source, Path::new(&target)).await?;
            } else {
                download_object(source, Some(PathBuf::from(&target))).await?;
            }
        } else if !is_s3_url(source) && is_s3_url(&target) {
            let source_path = Path::new(source);
            if source_path.is_dir() {
                if !args.recursive {
                    return Err(anyhow!(
                        "source `{source}` is a directory; use --recursive to copy it"
                    ));
                }
                mirror_local_to_s3(
                    source_path,
                    &target,
                    part_size,
                    args.disable_multipart,
                    args.parallel,
                )
                .await?;
            } else {
                upload_file(
                    source_path,
                    &target,
                    part_size,
                    args.parallel,
                    args.disable_multipart,
                    args.storage_class.as_deref(),
                )
                .await?;
            }
        } else {
            copy_local_path(Path::new(source), Path::new(&target), args.recursive).await?;
        }
    }
    Ok(())
}

async fn mirror(args: MirrorArgs) -> Result<()> {
    let part_size = parse_size(&args.part_size)?;
    if args.watch {
        return Err(anyhow!("mirror --watch is not implemented yet"));
    }
    if args.remove {
        eprintln!(
            "mirror --remove is accepted, but removal of extraneous target objects is not implemented yet"
        );
    }
    if args.dry_run || args.fake {
        println!("mirror dry run: {} -> {}", args.source, args.target);
        return Ok(());
    }

    let source_path = Path::new(&args.source);
    if source_path.exists() && is_s3_url(&args.target) {
        mirror_local_to_s3(
            source_path,
            &args.target,
            part_size,
            args.disable_multipart,
            args.parallel,
        )
        .await
    } else if is_s3_url(&args.source) && !is_s3_url(&args.target) {
        mirror_s3_to_local(&args.source, Path::new(&args.target)).await
    } else if is_s3_url(&args.source) && is_s3_url(&args.target) {
        mirror_s3_to_s3(
            &args.source,
            &args.target,
            part_size,
            args.disable_multipart,
            args.parallel,
        )
        .await
    } else {
        Err(anyhow!(
            "mirror currently supports local directory -> S3 prefix, S3 prefix -> local directory, and S3 prefix -> S3 prefix"
        ))
    }
}

async fn copy_s3_object_to_s3(
    source: &str,
    target: &str,
    part_size: u64,
    disable_multipart: bool,
    parallel: usize,
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
    let (source_client, _) = client_for_alias(&source_url.alias).await?;
    let (target_client, _) = client_for_alias(&target_url.alias).await?;
    let head = source_client
        .head_object()
        .bucket(&source_bucket)
        .key(&source_key)
        .send()
        .await?;
    transfer_object_between_s3(
        &source_client,
        &source_bucket,
        &source_key,
        &target_client,
        &target_bucket,
        &target_key,
        head.content_length().unwrap_or_default() as u64,
        part_size,
        disable_multipart,
        parallel,
    )
    .await?;
    println!(
        "Copied `{}/{}` to `{}/{}`.",
        source_bucket, source_key, target_bucket, target_key
    );
    Ok(())
}

async fn copy_local_path(source: &Path, target: &Path, recursive: bool) -> Result<()> {
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
                    let rel = path.strip_prefix(source)?;
                    let output = target.join(rel);
                    if let Some(parent) = output.parent() {
                        fs::create_dir_all(parent).await?;
                    }
                    fs::copy(&path, &output).await?;
                    println!("Copied `{}` to `{}`.", path.display(), output.display());
                }
            }
        }
    } else {
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
        println!("Copied `{}` to `{}`.", source.display(), output.display());
    }
    Ok(())
}

async fn mirror_local_to_s3(
    source: &Path,
    target: &str,
    part_size: u64,
    disable_multipart: bool,
    parallel: usize,
) -> Result<()> {
    let metadata = fs::metadata(source)
        .await
        .with_context(|| format!("stat {}", source.display()))?;
    if !metadata.is_dir() {
        return upload_file(source, target, part_size, parallel, disable_multipart, None).await;
    }

    let mut dirs = VecDeque::from([source.to_path_buf()]);
    while let Some(dir) = dirs.pop_front() {
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                dirs.push_back(path);
                continue;
            }
            if !metadata.is_file() {
                continue;
            }
            let rel = path
                .strip_prefix(source)?
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            let object_target = join_s3_target(target, &rel);
            upload_file(
                &path,
                &object_target,
                part_size,
                parallel,
                disable_multipart,
                None,
            )
            .await?;
        }
    }
    Ok(())
}

async fn mirror_s3_to_local(source: &str, target: &Path) -> Result<()> {
    let parsed = parse_s3_url(source)?;
    let bucket = parsed
        .bucket
        .ok_or_else(|| anyhow!("bucket is required in source `{source}`"))?;
    let prefix = parsed.key.unwrap_or_default();
    let (client, _) = client_for_alias(&parsed.alias).await?;
    fs::create_dir_all(target).await?;

    for obj in collect_objects(&client, &bucket, &prefix).await? {
        let rel = obj
            .key
            .strip_prefix(&prefix)
            .unwrap_or(&obj.key)
            .trim_start_matches('/');
        if rel.is_empty() {
            continue;
        }
        let output = target.join(rel);
        download_key_to_path(&client, &bucket, &obj.key, &output).await?;
        println!(
            "Mirrored `{}/{}` to `{}`.",
            bucket,
            obj.key,
            output.display()
        );
    }
    Ok(())
}

async fn mirror_s3_to_s3(
    source: &str,
    target: &str,
    part_size: u64,
    disable_multipart: bool,
    parallel: usize,
) -> Result<()> {
    let source_url = parse_s3_url(source)?;
    let source_bucket = source_url
        .bucket
        .ok_or_else(|| anyhow!("bucket is required in source `{source}`"))?;
    let source_prefix = source_url.key.unwrap_or_default();
    let target_url = parse_s3_url(target)?;
    let target_bucket = target_url
        .bucket
        .ok_or_else(|| anyhow!("bucket is required in target `{target}`"))?;
    let target_prefix = target_url.key.unwrap_or_default();
    let (source_client, _) = client_for_alias(&source_url.alias).await?;
    let (target_client, _) = client_for_alias(&target_url.alias).await?;

    for obj in collect_objects(&source_client, &source_bucket, &source_prefix).await? {
        let rel = obj
            .key
            .strip_prefix(&source_prefix)
            .unwrap_or(&obj.key)
            .trim_start_matches('/');
        if rel.is_empty() {
            continue;
        }
        let target_key = join_key(&target_prefix, rel);
        transfer_object_between_s3(
            &source_client,
            &source_bucket,
            &obj.key,
            &target_client,
            &target_bucket,
            &target_key,
            obj.size,
            part_size,
            disable_multipart,
            parallel,
        )
        .await?;
        println!(
            "Mirrored `{}/{}` to `{}/{}`.",
            source_bucket, obj.key, target_bucket, target_key
        );
    }
    Ok(())
}

async fn get(args: GetArgs) -> Result<()> {
    download_object(&args.source, args.target).await
}

async fn cat(args: CatArgs) -> Result<()> {
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
        let removed = remove_prefix(&client, &parsed.alias, &bucket, &prefix, args.dry_run).await?;
        if removed == 0 {
            println!("Nothing to remove under `{target}`.");
        }
        Ok(())
    } else {
        let key = parsed
            .key
            .ok_or_else(|| anyhow!("object key is required in target `{target}`"))?;
        // DeleteObject succeeds for missing keys; stat first for an mc-like error.
        client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|_| anyhow!("object does not exist"))?;
        if args.dry_run {
            println!("DRY-RUN rm `{target}`.");
            return Ok(());
        }
        client
            .delete_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await?;
        println!("Removed `{target}`.");
        Ok(())
    }
}

async fn remove_prefix(
    client: &Client,
    alias: &str,
    bucket: &str,
    prefix: &str,
    dry_run: bool,
) -> Result<u64> {
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    // S3 prefix matching is a raw string match, so a listing prefix of `p`
    // (no trailing slash) also matches sibling keys like `prefix2/x.txt`.
    // When the prefix doesn't already end at a path boundary, filter each
    // page down to the exact key or its `prefix/`-rooted descendants so
    // `rm -r bucket/p` only ever touches `p` itself and things under `p/`.
    let boundary_safe = prefix.is_empty() || prefix.ends_with('/');
    let descendant_prefix = format!("{prefix}/");
    let mut pager = ObjectPaginator::new(client.clone(), bucket.to_string(), prefix.to_string());
    let mut removed = 0u64;
    while let Some(mut page) = pager.next_page().await? {
        if !boundary_safe {
            page.retain(|obj| obj.key == prefix || obj.key.starts_with(&descendant_prefix));
        }
        if page.is_empty() {
            continue;
        }
        if dry_run {
            for obj in &page {
                println!("DRY-RUN rm `{alias}/{bucket}/{}`.", obj.key);
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
                println!("Removed `{alias}/{bucket}/{}`.", obj.key);
            }
            removed += chunk.len() as u64;
        }
    }
    Ok(removed)
}

async fn stat(args: StatArgs) -> Result<()> {
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
        println!("Name      : {key}");
        println!(
            "Size      : {}",
            format_size(resp.content_length().unwrap_or_default() as u64, BINARY)
        );
        if let Some(etag) = resp.e_tag() {
            println!("ETag      : {etag}");
        }
        if let Some(modified) = resp.last_modified() {
            println!("Modified  : {modified}");
        }
        if let Some(content_type) = resp.content_type() {
            println!("Type      : {content_type}");
        }
    }
    Ok(())
}
