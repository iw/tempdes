use std::process::ExitCode;

fn main() -> ExitCode {
    match tempdes::cli::main() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(2)
        }
    }
}
