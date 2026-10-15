fn main() {
    if pi_codemode::worker_main().is_err() {
        std::process::exit(1);
    }
}
