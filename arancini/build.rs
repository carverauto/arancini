extern crate capnpc;

fn main() {
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR is set by cargo");

    capnpc::CompilerCommand::new()
        .output_path(&out_dir)
        .src_prefix("schemas")
        .file("schemas/update.capnp")
        .run()
        .expect("capnp compiles");
}
