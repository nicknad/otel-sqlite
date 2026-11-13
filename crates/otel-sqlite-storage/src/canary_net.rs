// Canary: same content, OUTSIDE the include path. Not compiled.
fn canary() {
    let _ = std::net::TcpStream::connect("127.0.0.1:9");
    let _ = std::net::TcpListener::bind("127.0.0.1:0");
}
