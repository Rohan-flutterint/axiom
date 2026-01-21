use std::fs;

use anyhow::Result;
use clap::Parser;
use serde::Serialize;

use axiom_kernel::adapters::iceberg::IcebergMetadata;
use axiom_kernel::invariants::InvariantEngine;
use axiom_kernel::log::{InMemoryLogStore, MetadataLog, TableEvent};
use axiom_kernel::simulate::{simulate_table, SimulationResult};
use axiom_kernel::state::policy_config::PolicyConfig;

/// Axiom Control Plane CLI
#[derive(Parser, Debug)]
#[command(name = "axiom")]
#[command(about = "Axiom data control plane (dry-run)", long_about = None)]
struct Cli {
    /// Path to policy config JSON
    #[arg(long)]
    policy: Option<String>,

    /// Path to metadata log JSON
    #[arg(long)]
    log: String,

    /// Path to Iceberg metadata JSON
    #[arg(long)]
    iceberg: String,
}

/// Wrapper for JSON output
#[derive(Debug, Serialize)]
struct CliOutput {
    expected_state: String,
    drift_report: serde_json::Value,
    decision_plan: serde_json::Value,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // ----------------------------
    // Load metadata log
    // ----------------------------
    let log_data = fs::read_to_string(&cli.log)?;
    let events: Vec<TableEvent> = serde_json::from_str(&log_data)?;

    let store = InMemoryLogStore::default();
    let mut log = MetadataLog::new(store);

    for event in events {
        log.append(event)?;
    }

    // ----------------------------
    // Load Policy
    // ----------------------------
    let policy = if let Some(path) = cli.policy {
        let data = fs::read_to_string(path)?;
        serde_json::from_str::<PolicyConfig>(&data)?
    } else {
        PolicyConfig::default_policy()
    };

    // ----------------------------
    // Load Iceberg metadata
    // ----------------------------
    let iceberg_data = fs::read_to_string(&cli.iceberg)?;
    let iceberg_meta: IcebergMetadata = serde_json::from_str(&iceberg_data)?;
    let iceberg_state = iceberg_meta.into_table_state();

    // ----------------------------
    // Invariants (empty for now)
    // ----------------------------
    let invariants = InvariantEngine::new();

    // ----------------------------
    // Run simulation
    // ----------------------------
    let SimulationResult {
        expected_state,
        drift_report,
        decision_plan,
    } = simulate_table(&log, &invariants, &iceberg_state, &policy)?;

    // ----------------------------
    // Output
    // ----------------------------
    let output = CliOutput {
        expected_state: format!("{expected_state:?}"),
        drift_report: serde_json::to_value(&drift_report)?,
        decision_plan: serde_json::to_value(&decision_plan)?,
    };

    println!("{}", serde_json::to_string_pretty(&output)?);

    Ok(())
}
