use tokio::fs::File;
use std::error::Error;
use md5::{Md5, Digest};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use std::i64;
use asyncio_utils::UndoReader;


pub async fn copy_chunk_to_raw<D>(input_raw:&mut D, output: &mut File, chunked:bool) -> Result<(usize, String), Box<dyn Error>> 
    where D:AsyncRead + Unpin
{
    let mut input_obj = UndoReader::new(input_raw, None);
    let input = &mut input_obj;
    let mut hasher:Md5 = Md5::new();
    let mut total:usize = 0;
    if chunked {
        loop {
            // it is chunked
            let header = read_chunk_header(input).await?;
            let mut tokens = header.split(";");
            let size = tokens.next().expect("No size info!");
            let size = i64::from_str_radix(size, 16)?;
            if size == 0 {
                break;
            }
            let size = size as usize;
            let copied = copy_fixed_bytes(input, output, size, &mut hasher).await?;
            skip_n_bytes(input, 2).await?;
            total += copied;
        }
        let final_hash = format!("{:x}", hasher.finalize());
        return Ok((total, final_hash));
    } else {
        // raw
        let copied = copy_direct(input, output, &mut hasher).await?;
        let final_hash = format!("{:x}", hasher.finalize());

        return Ok((copied, final_hash));
    }

}

async fn try_read_full<D>(input: &mut UndoReader<D>, buff:&mut[u8]) -> Result<usize, Box<dyn Error>> 
    where D:AsyncRead + Unpin
{
    let target = buff.len();
    let mut nread:usize = 0;
    while nread < target {
        let rr = input.read(&mut buff[nread..]).await?;
        if rr == 0 {
            return Ok(nread);
        }

        nread += rr;
    }
    return Ok(nread);
}
async fn skip_n_bytes<D>(input: &mut UndoReader<D>, how_many:usize) -> Result<(), Box<dyn Error>> 
    where D:AsyncRead + Unpin
{
    let mut buf = vec![0u8; how_many];
    let mut nread:usize = 0;
    while nread < how_many {
        let rr = input.read(&mut buf[nread..]).await?;
        if rr == 0 {
            panic!("Insufficient read!");
        }
        nread += rr;
    }
    Ok(())
}
async fn read_chunk_header<D>(input: &mut UndoReader<D>) -> Result<String, Box<dyn Error>> 
    where D:AsyncRead + Unpin
{
    let mut buf = [0u8; 512];
    let rr = try_read_full(input, &mut buf).await?;
    for i in 0 .. rr - 1 {
        if buf[i] == 0x0d && buf[i+1] == 0x0a {
            if buf.len() > i + 2 {
                input.unread(&buf[i + 2..]);
            }
            // new line found!
            return Ok(std::str::from_utf8(&buf[0..i]).unwrap().to_string());
        }
    }
    if buf[1] == 0x3b && buf[0] == 0x30 {
        // 0 bytes
        return Ok(std::str::from_utf8(&buf[0..rr]).unwrap().to_string());
    }
    panic!("Expecing chunk header but not found!");
}

pub async fn copy_direct<D>(reader:&mut UndoReader<D>, out:&mut tokio::fs::File, hasher:&mut Md5) -> Result<usize, Box<dyn Error>> 
    where D:tokio::io::AsyncRead + Unpin
{
    let mut buf = [0u8; 4096];
    let mut copied: usize = 0;
    loop {
        let rr = reader.read(&mut buf).await?;
        if rr == 0 {
            break;
        }
        out.write_all(&mut buf[..rr]).await?;
        hasher.update(&mut buf[..rr]);
        copied += rr;
    }
    return Ok(copied);
}

async fn copy_fixed_bytes<D>(input: &mut UndoReader<D>, output: &mut File, bytes:usize, hasher:&mut Md5) -> Result<usize, Box<dyn Error>>
    where D:tokio::io::AsyncRead + Unpin
{
    let mut remaining = bytes;
    let mut buffer = [0u8; 4096];
    // Copy as whole chunk
    while remaining > 0 {
        let mut rr = input.read(&mut buffer).await?;
        if rr > remaining {
            // overread, put it back
            let to_put_back = &buffer[remaining..rr];
            input.unread(to_put_back);
            // only write up to remaining bytes
            rr = remaining;
        }
        let wr = output.write(&mut buffer[..rr]).await?;
        if wr != rr {
            panic!("Incomplete write!");
        }
        hasher.update(&buffer[..rr]);
        remaining = remaining - wr;
    }
    Ok(bytes)
}