//! Build and CI orchestration, invoked as `cargo xtask <command>`.

use clap::{Parser, Subcommand};
use color_eyre::eyre::Result;
use duct::cmd;

#[derive(Debug, Parser)]
#[command(name = "xtask", about = "Build and CI tasks")]
struct Cli {
    #[command(subcommand)]
    command: Task,
}

#[derive(Debug, Subcommand)]
enum Task {
    /// Everything CI runs, in order.
    Ci,
    /// Clippy across all targets, warnings denied.
    Check,
    /// The test suite.
    Test,
    /// Formatting check.
    Fmt,
    /// Apply formatting.
    FmtFix,
    /// Licence and advisory audit.
    Deny,
    /// Unused-dependency check.
    Machete,
    /// Coverage report.
    Coverage,
    /// The subset run by the pre-commit hook.
    #[command(visible_alias = "pc")]
    Precommit,
}

fn main() -> Result<()> {
    color_eyre::install()?;
    match Cli::parse().command {
        Task::Ci => {
            fmt()?;
            check()?;
            test()?;
            deny()?;
            machete()
        }
        Task::Check => check(),
        Task::Test => test(),
        Task::Fmt => fmt(),
        Task::FmtFix => run("cargo", &["fmt", "--all"]),
        Task::Deny => deny(),
        Task::Machete => machete(),
        Task::Coverage => run(
            "cargo",
            &[
                "llvm-cov",
                "--workspace",
                "--lcov",
                "--output-path",
                "lcov.info",
            ],
        ),
        // Kept fast: the pre-commit hook runs on every commit, so the slower
        // audits (deny, machete, coverage) are left to CI.
        Task::Precommit => {
            fmt()?;
            check()
        }
    }
}

fn check() -> Result<()> {
    run(
        "cargo",
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
    )
}

fn test() -> Result<()> {
    run("cargo", &["test", "--workspace", "--all-targets"])
}

fn fmt() -> Result<()> {
    run("cargo", &["fmt", "--all", "--check"])
}

fn deny() -> Result<()> {
    run("cargo", &["deny", "check"])
}

fn machete() -> Result<()> {
    // Invoked as the binary directly rather than as `cargo machete`: the
    // cargo subcommand shim passes "machete" through as a path argument,
    // which it then fails to stat.
    run("cargo-machete", &[])
}

fn run(program: &str, args: &[&str]) -> Result<()> {
    println!("$ {program} {}", args.join(" "));
    cmd(program, args).run()?;
    Ok(())
}
