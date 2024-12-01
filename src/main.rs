use blitzkv::database::Database;

fn main() {
    // Create a new database instance
    let mut db = Database::new();

    // Insert some key-value pairs
    db.set(b"name", b"BlitzKV").unwrap();
    db.set(b"type", b"Key-Value Store").unwrap();
    db.set(b"language", b"Rust").unwrap();

    // Retrieve values
    println!(
        "Name: {}",
        String::from_utf8_lossy(&db.get(b"name").unwrap())
    );
    println!(
        "Type: {}",
        String::from_utf8_lossy(&db.get(b"type").unwrap())
    );
    println!(
        "Language: {}",
        String::from_utf8_lossy(&db.get(b"language").unwrap())
    );

    // Delete a key
    db.delete(b"type").unwrap();
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
}
