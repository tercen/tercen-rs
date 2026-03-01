fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile proto files from tercen_grpc_api submodule
    // This ensures we stay in sync with the canonical API definitions
    tonic_prost_build::configure()
        .build_server(false) // Client only, no server code generation
        .build_transport(false) // Don't generate transport code (avoid naming conflicts)
        .compile_protos(
            &[
                "tercen_grpc_api/protos/tercen.proto",
                "tercen_grpc_api/protos/tercen_model.proto",
            ],
            &["tercen_grpc_api/protos"],
        )?;

    Ok(())
}
