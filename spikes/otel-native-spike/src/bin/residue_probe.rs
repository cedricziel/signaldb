//! Diagnostic: read a spike-written parquet file DIRECTLY with DataFusion's
//! parquet reader (bypassing datafusion_iceberg) and show the binary residue.
//! Usage: residue_probe <file.parquet>

use anyhow::Result;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .expect("usage: residue_probe <file.parquet>");
    let ctx = SessionContext::new();
    ctx.register_parquet("t", &path, ParquetReadOptions::default())
        .await?;
    for sql in ["SELECT service_name, attributes_residue FROM t"] {
        println!("-- {sql}");
        match ctx.sql(sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    for b in &batches {
                        println!("   schema col0 type: {}", b.column(0).data_type());
                    }
                    println!("{}", pretty_format_batches(&batches)?);
                }
                Err(e) => println!("   collect error: {e}"),
            },
            Err(e) => println!("   plan error: {e}"),
        }
    }
    Ok(())
}
