fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile protobuf files
    tonic_build::compile_protos("proto/ping.proto")?;
    tonic_build::compile_protos("proto/raft.proto")?;
    tonic_build::compile_protos("proto/datastore.proto")?;

    // Compile Cap'n Proto schema
    capnpc::CompilerCommand::new()
        .file("src/transport/capnp/raft.capnp")
        .default_parent_module(vec!["transport".to_string(), "capnp".to_string()])
        .run()
        .expect("capnp schema compilation failed");

    Ok(())
}