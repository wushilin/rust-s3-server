# rust-s3-server
An S3 compatible Object Storage written in Rust. Ideal for local test environment


# Building

```bash
$ cargo build --release
```

# Running

## Getting help
```bash
$ target/release/rusts3 --help
Usage: rusts3 [OPTIONS]

Options:
  -b, --base-dir <BASE_DIR>          [default: ./rusts3-data]
      --bind-address <BIND_ADDRESS>  Bind IP address [default: 0.0.0.0]
      --bind-port <BIND_PORT>        Bind port number [default: 8000]
      --log-conf <LOG_CONF>          Log4rs config file [default: ]
  -h, --help                         Print help
```

## Example usage:
```bash
$ target/release/rusts3 -b "test-data" --bind-address "192.168.44.172" --bind-port 18000 --log-conf log4rs.yaml
```

### Testing
Creating bucket:
```bash
$ aws --endpoint-url http://192.168.44.172:18000 s3 mb s3://new-bucket
make_bucket: new-bucket
```

Uploading object:
```bash
$ aws --endpoint-url http://192.168.44.172:18000 s3 cp ~/Downloads/zulu.dmg s3://new-bucket/some-path/zulu.dmg
upload: ./zulu.dmg to s3://new-bucket/some-path/zulu.dmg 
```

Downloading object:
```bash
$ aws --endpoint-url http://192.168.44.172:18000 s3 cp s3://new-bucket/some-path/zulu.dmg ./new.dmg
download: s3://new-bucket/some-path/zulu.dmg to ./new.dmg 

# files should be the same
$ diff ~/Downloads/zulu.dmg ./new/dmg
```

Listing object:
```bash
$ aws --endpoint-url http://192.168.44.172:18000 s3 ls s3://new-bucket
                           PRE some-path/
$ aws --endpoint-url http://192.168.44.172:18000 s3 ls s3://new-bucket/some-path/
                           PRE some-path/
2023-08-13 11:45:25     615835 zulu.dmg
```

Deleting object:
```bash
$ aws --endpoint-url http://192.168.44.172:18000 s3 rm s3://new-bucket/some-path/zulu.img
delete: s3://new-bucket/some-path/zulu.dmg
```
