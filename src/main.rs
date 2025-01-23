use blitzkv::database::{Database, DatabaseError};
use std::path::PathBuf;
use tracing::{error, info, instrument};
use tracing_subscriber;

fn run() -> Result<(), DatabaseError> {
    info!("Initializing BlitzKV database");

    // Create data directory if it doesn't exist
    let data_dir = PathBuf::from("data");
    std::fs::create_dir_all(&data_dir).expect("Failed to create data directory");

    // Create a new database instance
    let mut db = Database::new(data_dir.join("blitzkv.db"))?;

    // Insert some key-value pairs
    info!("Inserting initial key-value pairs");
    db.set(b"name", b"BlitzKV")?;
    db.set(b"type", b"Key-Value Store")?;
    db.set(b"language", b"Rust")?;
    info!("Initial key-value pairs inserted successfully");

    // Retrieve values
    println!("Name: {}", String::from_utf8_lossy(&db.get(b"name")?));
    println!("Type: {}", String::from_utf8_lossy(&db.get(b"type")?));
    println!(
        "Language: {}",
        String::from_utf8_lossy(&db.get(b"language")?)
    );

    // Delete a key
    info!("Deleting key 'type'");
    db.delete(b"type")?;
    println!("After deleting 'type', exists: {:?}", db.get(b"type"));

    // Print some statistics
    println!("\nDatabase Statistics:");
    println!("Number of key-value pairs: {}", db.len());
    println!("Total size: {} bytes", db.total_size());
    println!("Total capacity: {} bytes", db.total_capacity());
    println!("Space amplification: {:.2}x", db.space_amplification());

    // List all keys
    println!("\nAll keys:");
    for key in db.keys() {
        println!("- {}", String::from_utf8_lossy(&key));
    }

    Ok(())
}

#[instrument]
fn main() {
    // Initialize tracing subscriber
    tracing_subscriber::fmt::init();

    if let Err(e) = run() {
        error!("Database error: {:?}", e);
        std::process::exit(1);
    }
}
