use std::process::Command;

#[test]
fn exits_with_a_structured_error_when_app_id_is_missing() {
    let output = Command::new(env!("CARGO_BIN_EXE_merge-app"))
        .env_remove("GITHUB_APP_ID")
        .env_remove("GITHUB_APP_PRIVATE_KEY_PATH")
        .env("RUST_LOG", "info")
        .output()
        .expect("run the merge-app binary");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());

    let logs = String::from_utf8_lossy(&output.stderr);
    assert!(logs.contains("\"event\":\"application_failed\""));
    assert!(logs.contains("GITHUB_APP_ID is required"));
}
