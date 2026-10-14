use std::process::Command;

#[test]
fn prints_the_default_greeting() {
    let output = Command::new(env!("CARGO_BIN_EXE_merge-app"))
        .output()
        .expect("run the merge-app binary");

    assert!(output.status.success());
    assert_eq!(output.stdout, b"Hello, world!\n");
}
