// Fixture: same calls OUTSIDE the rule's include path. Expects no findings at
// all, so it carries no annotations. Not compiled.
fn canary() {
    // ruleid: rust.std-net-in-pipeline
    let _ = std::net::TcpStream::connect("127.0.0.1:9");
    let _ = std::net::TcpListener::bind("127.0.0.1:0");
}
