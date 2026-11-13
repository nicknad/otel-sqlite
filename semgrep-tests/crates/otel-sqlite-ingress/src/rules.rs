// Fixture: std::net inside the rule's include path. Not compiled.
fn canary() {
    // ruleid: rust.std-net-in-pipeline
    let _ = std::net::TcpStream::connect("127.0.0.1:9");

    // ruleid: rust.std-net-in-pipeline
    let _ = std::net::TcpListener::bind("127.0.0.1:0");

    // ok: rust.std-net-in-pipeline
    let _ = tokio::net::TcpStream::connect("127.0.0.1:9");
}
