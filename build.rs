fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile Cap'n Proto schema
    capnpc::CompilerCommand::new()
        .file("src/transport/capnp/raft.capnp")
        .default_parent_module(vec!["transport".to_string(), "capnp".to_string()])
        .run()
        .expect("capnp schema compilation failed");

    Ok(())
}