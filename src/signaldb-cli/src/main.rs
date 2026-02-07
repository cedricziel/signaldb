mod commands;
mod tui;

use clap::Parser;
use commands::Cli;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = cli.run().await {
        eprintln!("Error: {e}");
        for cause in e.chain().skip(1) {
            eprintln!("  caused by: {cause}");
        }
        std::process::exit(1);
    }
}
