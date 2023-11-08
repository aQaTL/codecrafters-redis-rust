use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::{Arc, RwLock};

fn main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	println!("Logs from your program will appear here!");

	let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

	let db: Arc<Db> = Arc::default();

	for stream in listener.incoming() {
		match stream {
			Ok(stream) => {
				let db = Arc::clone(&db);
				std::thread::spawn(move || handle_connection(stream, db));
			}
			Err(e) => {
				println!("error: {}", e);
			}
		}
	}
}

#[derive(Default)]
struct Db {
	hm: RwLock<HashMap<String, RESPMsg<'static>>>,
}

fn handle_connection(mut stream: TcpStream, db: Arc<Db>) {
	let mut input = vec![0_u8; 256];
	loop {
		unsafe {
			input.set_len(256);
		};
		let read_bytes = stream.read(&mut input).unwrap();
		if read_bytes == 0 {
			break;
		}
		unsafe {
			input.set_len(read_bytes);
		}

		// println!("input({read_bytes}): \"{input_str}\"");
		let message = RESPMsg::from_slice(input.as_slice());
		println!("input: {message:#?}");

		match message {
			RESPMsg::Array(RESPArray(arr)) => match &arr[0] {
				RESPMsg::BulkString(BulkString(str)) => {
					let command = str.to_ascii_lowercase();
					match command.as_str() {
						"echo" => handle_echo(&mut stream, &arr[1]),
						"ping" => handle_ping(&mut stream),
						"set" => handle_set(&mut stream, &db, &arr[1..]),
						"get" => handle_get(&mut stream, &db, &arr[1..]),
						_ => panic!("unknown command: {command}"),
					}
				}
				RESPMsg::SimpleString(SimpleString(str)) if str.eq_ignore_ascii_case("ping") => {
					handle_ping(&mut stream);
				}
				_ => panic!(),
			},
			_ => unimplemented!(),
		}

		stream.flush().unwrap();
	}
	println!("closing connection");
}

fn handle_ping(stream: &mut TcpStream) {
	PONG_RESPONSE.write_to(stream).unwrap();
}

fn handle_echo(stream: &mut TcpStream, payload: &RESPMsg<'_>) {
	let RESPMsg::BulkString(payload) = payload else {
		panic!();
	};

	// Echo back the same thing
	payload.write_to(stream).unwrap();
}

fn handle_set(stream: &mut TcpStream, db: &Arc<Db>, payload: &[RESPMsg<'_>]) {
	if payload.len() > 2 {
		eprintln!("[WARN] Received more than <key> <value>. Ignoring that.");
	}
	let RESPMsg::BulkString(BulkString(key)) = &payload[0] else {
		panic!();
	};
	let value = payload[1].clone();

	{
		db.hm.write().unwrap().insert(key.to_string(), value);
	}

	let response = SimpleString("OK".into());
	response.write_to(stream).unwrap();
}

fn handle_get(stream: &mut TcpStream, db: &Arc<Db>, payload: &[RESPMsg<'_>]) {
	if payload.len() > 1 {
		eprintln!("[WARN] Received more than <key>. Ignoring that.");
	}
	let RESPMsg::BulkString(BulkString(key)) = &payload[0] else {
		panic!();
	};

	let db = db.hm.read().unwrap();
	let value = db.get(key as &str);
	let response = match value {
		Some(
			RESPMsg::BulkString(BulkString(value)) | RESPMsg::SimpleString(SimpleString(value)),
		) => RESPMsg::BulkString(BulkString(Cow::Borrowed(value))),
		None => RESPMsg::Null,
		_ => panic!(),
	};

	response.write_to(stream).unwrap();
	drop(db);
}

const PONG_RESPONSE: SimpleString<'static> = SimpleString(Cow::Borrowed("PONG"));

#[derive(Debug, Eq, PartialEq)]
enum RESPMsg<'a> {
	SimpleString(SimpleString<'a>),
	BulkString(BulkString<'a>),
	Array(RESPArray<'a>),
	Null,
}

impl RESPMsg<'_> {
	#[allow(dead_code)]
	fn write_to(&self, w: &mut impl Write) -> io::Result<usize> {
		match self {
			RESPMsg::SimpleString(v) => v.write_to(w),
			RESPMsg::BulkString(v) => v.write_to(w),
			RESPMsg::Array(v) => v.write_to(w),
			RESPMsg::Null => {
				w.write_all(b"_\r\n")?;
				Ok(3)
			}
		}
	}

	fn from_slice<'a>(mut input: &'a [u8]) -> RESPMsg<'a> {
		let slice: &mut &'a [u8] = &mut input;
		Self::decode(slice)
	}

	fn decode<'a, 'b>(input: &'b mut &'a [u8]) -> RESPMsg<'a>
	where
		'a: 'b,
	{
		match (*input)[0] {
			b'*' => RESPMsg::Array(RESPArray::decode(input)),
			b'$' => RESPMsg::BulkString(BulkString::decode(input)),
			b'+' => RESPMsg::SimpleString(SimpleString::decode(input)),
			b'_' => RESPMsg::Null,
			v => panic!("unimplemented type `{v}`"),
		}
	}

	fn clone(&self) -> RESPMsg<'static> {
		match self {
			RESPMsg::SimpleString(v) => RESPMsg::SimpleString(v.clone()),
			RESPMsg::BulkString(v) => RESPMsg::BulkString(v.clone()),
			RESPMsg::Array(v) => {
				let arr: RESPArray<'static> = v.clone_manually();
				RESPMsg::Array(arr)
			}
			RESPMsg::Null => RESPMsg::Null,
		}
	}
}

#[derive(Debug, Eq, PartialEq)]
struct SimpleString<'a>(Cow<'a, str>);

impl SimpleString<'_> {
	fn write_to(&self, w: &mut impl Write) -> io::Result<usize> {
		w.write_all(b"+")?;
		w.write_all(self.0.as_bytes())?;
		w.write_all(b"\r\n")?;
		Ok(1 + self.0.len() + 2)
	}

	fn decode<'a, 'b>(input: &'b mut &'a [u8]) -> SimpleString<'a>
	where
		'a: 'b,
	{
		let str_end_idx = find_crlf_idx(&(*input)[1..]).unwrap() + 1;
		let str = std::str::from_utf8(&(*input)[1..str_end_idx]).unwrap();
		*input = &((*input)[(str_end_idx + 2)..]);
		SimpleString(str.into())
	}

	fn clone(&self) -> SimpleString<'static> {
		SimpleString(Cow::Owned(self.0.to_string()))
	}
}

#[derive(Debug, Eq, PartialEq)]
struct BulkString<'a>(Cow<'a, str>);

impl BulkString<'_> {
	fn write_to(&self, w: &mut impl Write) -> io::Result<usize> {
		let header = format!("${}\r\n", self.0.len());
		w.write_all(header.as_bytes())?;
		w.write_all(self.0.as_bytes())?;
		w.write_all(b"\r\n")?;
		Ok(header.len() + self.0.len() + 2)
	}

	fn decode<'a, 'b>(input: &'b mut &'a [u8]) -> BulkString<'a>
	where
		'a: 'b,
	{
		let len_end_idx = find_crlf_idx(&(*input)[1..]).unwrap() + 1;
		let length: usize = std::str::from_utf8(&(*input)[1..len_end_idx])
			.unwrap()
			.parse()
			.unwrap();
		let data =
			std::str::from_utf8(&((*input)[len_end_idx + 2..(len_end_idx + 2 + length)])).unwrap();
		assert_eq!(
			&((*input)[(len_end_idx + 2 + length)..(len_end_idx + 2 + length + 2)]),
			&[b'\r', b'\n']
		);
		*input = &(*input)[(len_end_idx + 2 + length + 2)..];
		BulkString(data.into())
	}

	fn clone(&self) -> BulkString<'static> {
		BulkString(Cow::Owned(self.0.to_string()))
	}
}

#[derive(Debug, Eq, PartialEq)]
struct RESPArray<'a>(Vec<RESPMsg<'a>>);

impl RESPArray<'_> {
	#[allow(dead_code)]
	fn write_to(&self, w: &mut impl Write) -> io::Result<usize> {
		let header = format!("*{}\r\n", self.0.len());
		w.write_all(header.as_bytes())?;
		let mut written = 0;
		for msg in &self.0 {
			written += msg.write_to(w)?;
		}
		if self.0.is_empty() {
			w.write_all(b"\r\n")?;
			written += 2;
		}
		Ok(header.len() + written)
	}

	fn decode<'a, 'b>(input: &'b mut &'a [u8]) -> RESPArray<'a>
	where
		'a: 'b,
	{
		let input_len_end_idx = find_crlf_idx(&(*input)[1..]).unwrap() + 1;
		let length: usize = std::str::from_utf8(&(*input)[1..input_len_end_idx])
			.unwrap()
			.parse()
			.unwrap();
		let mut vec: Vec<RESPMsg<'a>> = Vec::with_capacity(length);
		*input = &(*input)[input_len_end_idx + 2..];
		for _ in 0..length {
			let msg: RESPMsg<'a> = RESPMsg::decode(input);
			vec.push(msg);
		}
		RESPArray(vec)
	}

	fn clone_manually(&self) -> RESPArray<'static> {
		let mut vec: Vec<RESPMsg<'static>> = Vec::with_capacity(self.0.len());
		for x in self.0.iter() {
			let msg: RESPMsg<'static> = x.clone();
			vec.push(msg);
		}
		RESPArray(vec)
	}
}

fn find_crlf_idx(input: &[u8]) -> Option<usize> {
	input.windows(2).position(|window| window == [b'\r', b'\n'])
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_find_crlf() {
		assert_eq!(find_crlf_idx(b"\r\nfoobar"), Some(0));
		assert_eq!(find_crlf_idx(b"f\r\nfoobar"), Some(1));
		assert_eq!(
			find_crlf_idx(b"ala ma\rko\n\rta\n, a kot ma ale\r\n."),
			Some(28)
		);
		assert_eq!(
			find_crlf_idx(b"ala ma\rko\n\rta\n, a kot ma ale\r\n. Foobar."),
			Some(28)
		);
		assert_eq!(find_crlf_idx(b"ala ma\rko\n\rta\n, a kot ma ale"), None);
	}

	#[test]
	fn test_decode() {
		let msg = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
		let expected = RESPMsg::Array(RESPArray(vec![
			RESPMsg::BulkString(BulkString("ECHO".into())),
			RESPMsg::BulkString(BulkString("hey".into())),
		]));
		let result = RESPMsg::from_slice(msg);
		assert_eq!(result, expected);
	}
}
