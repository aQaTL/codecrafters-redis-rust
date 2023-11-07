use std::borrow::Cow;
use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;

fn main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	println!("Logs from your program will appear here!");

	let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

	for stream in listener.incoming() {
		match stream {
			Ok(stream) => {
				std::thread::spawn(move || handle_connection(stream));
			}
			Err(e) => {
				println!("error: {}", e);
			}
		}
	}
}

fn handle_connection(mut stream: TcpStream) {
	let mut input = vec![0_u8; 256];
	let read_bytes = stream.read(&mut input).unwrap();
	unsafe {
		input.set_len(read_bytes);
	}

	let input_str = std::str::from_utf8(&input).unwrap();

	println!("input({read_bytes}): \"{input_str}\"");
	PONG_RESPONSE.write_to(&mut stream).unwrap();
	stream.flush().unwrap();
	println!("closing connection");
}

struct SimpleString<'a>(Cow<'a, str>);

impl SimpleString<'_> {
	fn write_to(&self, w: &mut impl Write) -> io::Result<usize> {
		w.write_all(b"+")?;
		w.write_all(self.0.as_bytes())?;
		w.write_all(b"\r\n")?;
		Ok(1 + self.0.len() + 2)
	}
}

const PONG_RESPONSE: SimpleString<'static> = SimpleString(Cow::Borrowed("PONG"));
