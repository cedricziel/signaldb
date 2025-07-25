use anyhow::Result;

#[test]
fn test_simple_produce_gzip() -> Result<()> {
    // Just test that the compression libraries work
    let test_data = b"This is test data that should compress well. ".repeat(10);

    // Test Gzip compression
    {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&test_data)?;
        let compressed = encoder.finish()?;

        println!(
            "Gzip: {} bytes -> {} bytes",
            test_data.len(),
            compressed.len()
        );

        // Decompress to verify
        use flate2::read::GzDecoder;
        use std::io::Read;

        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;

        assert_eq!(test_data, decompressed.as_slice());
    }

    // Test LZ4 compression
    {
        let compressed = lz4::block::compress(&test_data, None, true)?;
        println!(
            "LZ4: {} bytes -> {} bytes",
            test_data.len(),
            compressed.len()
        );

        let decompressed = lz4::block::decompress(&compressed, None)?;
        assert_eq!(test_data, decompressed.as_slice());
    }

    // Test Snappy compression
    {
        let mut encoder = snap::raw::Encoder::new();
        let compressed = encoder.compress_vec(&test_data)?;
        println!(
            "Snappy: {} bytes -> {} bytes",
            test_data.len(),
            compressed.len()
        );

        let decompressed = snap::raw::Decoder::new().decompress_vec(&compressed)?;
        assert_eq!(test_data, decompressed.as_slice());
    }

    // Test Zstd compression
    {
        let compressed = zstd::encode_all(&test_data[..], 3)?;
        println!(
            "Zstd: {} bytes -> {} bytes",
            test_data.len(),
            compressed.len()
        );

        let decompressed = zstd::decode_all(&compressed[..])?;
        assert_eq!(test_data, decompressed.as_slice());
    }

    Ok(())
}

fn main() {
    if let Err(e) = test_simple_produce_gzip() {
        eprintln!("Test failed: {e}");
        std::process::exit(1);
    }
    println!("Test passed!");
}
