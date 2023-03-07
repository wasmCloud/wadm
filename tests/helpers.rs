#![allow(dead_code)]
use tokio::process::Command;

const WASHBOARD_URL: &str = "localhost:4000";

pub struct CleanupGuard {
    already_running: bool,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if !self.already_running {
            match std::process::Command::new("wash").args(["down"]).output() {
                Ok(o) if !o.status.success() => {
                    eprintln!(
                        "Error stopping wasmcloud host: {}",
                        String::from_utf8_lossy(&o.stderr)
                    )
                }
                Err(e) => eprintln!("Error stopping wasmcloud host: {e}"),
                _ => (),
            }
        }
    }
}

// TODO: Make this actually be unique for each test so we can run in parallel
pub async fn setup_test() -> CleanupGuard {
    // Start wasmcloud host if we don't find one running
    let already_running = if tokio::net::TcpStream::connect(WASHBOARD_URL).await.is_err() {
        let output = Command::new("wash")
            .args(["up", "-d"])
            .status()
            .await
            .expect("Unable to run wash up");
        assert!(output.success(), "Error trying to start host",);
        // Make sure we can connect
        wait_for_server(WASHBOARD_URL).await;
        // Give the host just a bit more time to spin up. If we don't wait, sometimes the host isn't
        // totally ready
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        false
    } else {
        true
    };

    CleanupGuard { already_running }
}

pub async fn wait_for_server(url: &str) {
    let mut wait_count = 1;
    loop {
        // Magic number: 10 + 1, since we are starting at 1 for humans
        if wait_count >= 11 {
            panic!("Ran out of retries waiting for host to start");
        }
        match tokio::net::TcpStream::connect(url).await {
            Ok(_) => break,
            Err(e) => {
                eprintln!("Waiting for server {url} to come up, attempt {wait_count}. Will retry in 1 second. Got error {e:?}");
                wait_count += 1;
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

/// Runs wash with the given args and makes sure it runs successfully. Returns the contents of
/// stdout
pub async fn run_wash_command<I, S>(args: I) -> Vec<u8>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = Command::new("wash")
        .args(args)
        .output()
        .await
        .expect("Unable to run wash command");
    if !output.status.success() {
        panic!(
            "wash command didn't exit successfully: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    }
    output.stdout
}
