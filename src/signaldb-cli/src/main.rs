use clap::{CommandFactory, Parser};
use signaldb_cli::commands::Cli;

fn main() {
    // Handle dynamic shell completion requests (COMPLETE=<shell> signaldb-cli ...)
    // before argument parsing and before the async runtime starts: the tenant-ID
    // completer builds its own single-threaded runtime, which must not nest
    // inside another tokio runtime. Exits the process when completing.
    clap_complete::CompleteEnv::with_factory(Cli::command).complete();

    let cli = Cli::parse();

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("Error: failed to start async runtime: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = runtime.block_on(cli.run()) {
        eprintln!("Error: {e}");
        for cause in e.chain().skip(1) {
            eprintln!("  caused by: {cause}");
        }
        std::process::exit(1);
    }
}
