use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

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
	hm: RwLock<HashMap<String, Value>>,
}

fn handle_connection(stream: TcpStream, db: Arc<Db>) {
	let mut stream = RESPMsgReader::new(stream);
	while let Some(message) = stream.next_msg().unwrap() {
		match message {
			RESPMsg::Array(RESPArray(arr)) => match &arr[0] {
				RESPMsg::BulkString(BulkString(str)) => {
					let command = str.to_ascii_lowercase();
					match command.as_str() {
						"echo" => handle_echo(stream.get_mut(), &arr[1]),
						"ping" => handle_ping(stream.get_mut()),
						"set" => handle_set(stream.get_mut(), &db, arr.into_iter().skip(1)),
						"get" => handle_get(stream.get_mut(), &db, &arr[1..]),
						_ => panic!("unknown command: {command}"),
					}
				}
				RESPMsg::SimpleString(SimpleString(str)) if str.eq_ignore_ascii_case("ping") => {
					handle_ping(stream.get_mut());
				}
				_ => panic!(),
			},
			_ => unimplemented!(),
		}

		stream.get_mut().flush().unwrap();
	}
	println!("closing connection");
}

struct BufReader<R> {
	stream: R,
	buffer: Vec<u8>,
}

impl<R: Read> BufReader<R> {
	fn new(stream: R) -> Self {
		BufReader {
			stream,
			buffer: Vec::new(),
		}
	}

	fn read_more(&mut self) -> io::Result<()> {
		const MSG_LEN: usize = 1024;

		let current_len = self.buffer.len();
		self.buffer.resize(current_len + MSG_LEN, 0_u8);

		let new_buf_space = &mut self.buffer[current_len..(current_len + MSG_LEN)];
		let bytes_read = self.stream.read(new_buf_space)?;

		unsafe {
			self.buffer.set_len(current_len + bytes_read);
		}

		Ok(())
	}

	fn fill_buf(&mut self) -> &[u8] {
		&self.buffer
	}

	fn consume(&mut self, amt: usize) {
		if amt == 0 {
			return;
		}
		let buf_ptr = self.buffer.as_mut_ptr();
		let buf_to_copy = &self.buffer[amt..];
		unsafe {
			for (idx, b) in buf_to_copy.iter().copied().enumerate() {
				*buf_ptr.add(idx) = b;
			}

			self.buffer.set_len(buf_to_copy.len());
		}
	}
}

struct RESPMsgReader<R> {
	stream: BufReader<R>,
	current_position: usize,
}

impl<R: Read> RESPMsgReader<R> {
	pub fn new(stream: R) -> Self {
		RESPMsgReader {
			stream: BufReader::new(stream),
			current_position: 0,
		}
	}

	pub fn get_mut(&mut self) -> &mut R {
		&mut self.stream.stream
	}

	pub fn next_msg(&mut self) -> io::Result<Option<RESPMsg<'static>>> {
		let msg = self.next_msg_();
		self.stream.consume(self.current_position);
		self.current_position = 0;
		msg
	}

	fn next_msg_(&mut self) -> io::Result<Option<RESPMsg<'static>>> {
		if self.stream.fill_buf().is_empty()
			|| self.stream.fill_buf().len() == self.current_position
		{
			self.stream.read_more()?;
		}
		let Some(msg_type) = self.stream.fill_buf().get(self.current_position) else {
			return Ok(None);
		};
		match msg_type {
			b'*' => Ok(Some(RESPMsg::Array(self.decode_array()?))),
			b'$' => Ok(Some(RESPMsg::BulkString(self.decode_bulk_string()?))),
			b'+' => Ok(Some(RESPMsg::SimpleString(self.decode_simple_string()?))),
			b'_' => Ok(Some(self.decode_null()?)),
			v => panic!("unimplemented type `{}`", *v as char),
		}
	}

	fn decode_array(&mut self) -> io::Result<RESPArray<'static>> {
		loop {
			let Some(len_end_idx) = find_crlf_idx(&self.stream.fill_buf()[self.current_position..])
			else {
				self.stream.read_more()?;
				continue;
			};
			let length: usize = std::str::from_utf8(
				&self.stream.fill_buf()
					[(self.current_position + 1)..(self.current_position + len_end_idx)],
			)
			.unwrap()
			.parse()
			.unwrap();

			self.current_position += len_end_idx + 2;

			let mut vec: Vec<RESPMsg<'static>> = Vec::with_capacity(length);
			for _ in 0..length {
				let msg: RESPMsg<'static> = self.next_msg_()?.unwrap();
				vec.push(msg);
			}
			return Ok(RESPArray(vec));
		}
	}

	fn decode_bulk_string(&mut self) -> io::Result<BulkString<'static>> {
		loop {
			let Some(len_end_idx) = find_crlf_idx(&self.stream.fill_buf()[self.current_position..])
			else {
				self.stream.read_more()?;
				continue;
			};
			let length: usize = std::str::from_utf8(
				&self.stream.fill_buf()
					[(self.current_position + 1)..(self.current_position + len_end_idx)],
			)
			.unwrap()
			.parse()
			.unwrap();

			let data_start_idx = len_end_idx + 2;
			let Some(data_end_idx) =
				find_crlf_idx(&self.stream.fill_buf()[(self.current_position + data_start_idx)..])
			else {
				self.stream.read_more()?;
				continue;
			};
			let actual_data_len = data_end_idx;
			assert_eq!(actual_data_len, length);
			let data_end_idx = data_start_idx + data_end_idx;

			let data = &self.stream.fill_buf()
				[(self.current_position + data_start_idx)..(self.current_position + data_end_idx)];
			let str: String = std::str::from_utf8(data).unwrap().to_string();

			self.current_position += data_end_idx + 2;
			return Ok(BulkString(Cow::Owned(str)));
		}
	}

	fn decode_simple_string(&mut self) -> io::Result<SimpleString<'static>> {
		loop {
			let Some(str_end_idx) = find_crlf_idx(&self.stream.fill_buf()[self.current_position..])
			else {
				self.stream.read_more()?;
				continue;
			};

			let str: String = std::str::from_utf8(
				&self.stream.fill_buf()
					[(self.current_position + 1)..(self.current_position + str_end_idx)],
			)
			.unwrap()
			.to_string();

			self.current_position += str_end_idx + 2;

			return Ok(SimpleString(Cow::Owned(str)));
		}
	}

	fn decode_null(&mut self) -> io::Result<RESPMsg<'static>> {
		while (self.stream.fill_buf().len() - self.current_position) < 3 {
			self.stream.read_more()?;
		}
		let Some(msg) = (&self.stream.fill_buf()
			[(self.current_position + 1)..(self.current_position + 3)]
			== b"\r\n")
			.then_some(RESPMsg::Null)
		else {
			panic!()
		};

		self.current_position += 3;

		Ok(msg)
	}
}

fn handle_ping<W: Write>(stream: &mut W) {
	PONG_RESPONSE.write_to(stream).unwrap();
}

fn handle_echo<W: Write>(stream: &mut W, payload: &RESPMsg<'_>) {
	let RESPMsg::BulkString(payload) = payload else {
		panic!();
	};

	// Echo back the same thing
	payload.write_to(stream).unwrap();
}

fn handle_set<W, I>(stream: &mut W, db: &Arc<Db>, payload: I)
where
	W: Write,
	I: Iterator<Item = RESPMsg<'static>>,
{
	let options = SetCommand::new(payload);

	{
		println!("Expiry: {:?}", options.expiry);
		db.hm.write().unwrap().insert(
			options.key.0.into_owned(),
			Value {
				value: options.value,
				expires_at: options.expiry.map(|expiry| Instant::now() + expiry),
			},
		);
	}

	let response = SimpleString("OK".into());
	response.write_to(stream).unwrap();
}

struct Value {
	value: RESPMsg<'static>,
	expires_at: Option<Instant>,
}

struct SetCommand<'a> {
	key: BulkString<'a>,
	value: RESPMsg<'a>,
	expiry: Option<Duration>,
}

impl SetCommand<'_> {
	fn new(mut payload: impl Iterator<Item = RESPMsg<'static>>) -> Self {
		let RESPMsg::BulkString(key) = payload.next().unwrap() else {
			panic!();
		};
		let value = payload.next().unwrap().clone();

		let mut expiry = None;

		while let Some(msg) = payload.next() {
			match msg {
				RESPMsg::BulkString(BulkString(str)) if str.eq_ignore_ascii_case("PX") => {
					let expiry_millis: u64 = match payload.next().unwrap() {
						RESPMsg::BulkString(BulkString(expiry)) => expiry.parse().unwrap(),
						msg => panic!("{msg:?}"),
					};
					expiry = Some(Duration::from_millis(expiry_millis));
				}
				_ => (),
			}
		}

		SetCommand { key, value, expiry }
	}
}

fn handle_get<W: Write>(stream: &mut W, db: &Arc<Db>, payload: &[RESPMsg<'_>]) {
	if payload.len() > 1 {
		eprintln!("[WARN] Received more than <key>. Ignoring that.");
	}
	let RESPMsg::BulkString(BulkString(key)) = &payload[0] else {
		panic!();
	};

	let db = db.hm.read().unwrap();
	let value = db.get(key as &str);
	let response = match value {
		Some(Value {
			value:
				RESPMsg::BulkString(BulkString(value)) | RESPMsg::SimpleString(SimpleString(value)),
			expires_at,
		}) => {
			if expires_at
				.map(|expires_at| Instant::now() > expires_at)
				.unwrap_or_default()
			{
				RESPMsg::NullBulkString
			} else {
				RESPMsg::BulkString(BulkString(Cow::Borrowed(value)))
			}
		}
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
	NullBulkString,
	Array(RESPArray<'a>),
	Null,
}

impl RESPMsg<'_> {
	#[allow(dead_code)]
	fn write_to(&self, w: &mut impl Write) -> io::Result<usize> {
		match self {
			RESPMsg::SimpleString(v) => v.write_to(w),
			RESPMsg::BulkString(v) => v.write_to(w),
			RESPMsg::NullBulkString => {
				w.write_all(b"$-1\r\n")?;
				Ok(5)
			}
			RESPMsg::Array(v) => v.write_to(w),
			RESPMsg::Null => {
				w.write_all(b"_\r\n")?;
				Ok(3)
			}
		}
	}

	#[allow(dead_code)]
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
			RESPMsg::NullBulkString => RESPMsg::NullBulkString,
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

	#[allow(dead_code)]
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

	#[allow(dead_code)]
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

	#[test]
	fn test_decode_from_reader() {
		let msg = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
		let expected = RESPMsg::Array(RESPArray(vec![
			RESPMsg::BulkString(BulkString("ECHO".into())),
			RESPMsg::BulkString(BulkString("hey".into())),
		]));
		let result = RESPMsgReader::new(msg.as_slice())
			.next_msg()
			.unwrap()
			.unwrap();
		assert_eq!(result, expected);
	}

	#[test]
	fn test_decode_multiple_messages_from_reader() {
		let msg1 = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
		let msg2 = b"*2\r\n$15\r\nAla nie ma kota\r\n$3\r\nhey\r\n";
		let msg3 = b"*2\r\n+FOOBAR\r\n$3\r\nhey\r\n";
		let msg4 = b"*3\r\n$4\r\nECHO\r\n_\r\n$3\r\nhey\r\n";
		let expected1 = RESPMsg::Array(RESPArray(vec![
			RESPMsg::BulkString(BulkString("ECHO".into())),
			RESPMsg::BulkString(BulkString("hey".into())),
		]));
		let expected2 = RESPMsg::Array(RESPArray(vec![
			RESPMsg::BulkString(BulkString("Ala nie ma kota".into())),
			RESPMsg::BulkString(BulkString("hey".into())),
		]));
		let expected3 = RESPMsg::Array(RESPArray(vec![
			RESPMsg::SimpleString(SimpleString("FOOBAR".into())),
			RESPMsg::BulkString(BulkString("hey".into())),
		]));
		let expected4 = RESPMsg::Array(RESPArray(vec![
			RESPMsg::BulkString(BulkString("ECHO".into())),
			RESPMsg::Null,
			RESPMsg::BulkString(BulkString("hey".into())),
		]));

		let result = RESPMsgReader::new(msg1.as_slice())
			.next_msg()
			.unwrap()
			.unwrap();
		assert_eq!(result, expected1);

		let result = RESPMsgReader::new(msg2.as_slice())
			.next_msg()
			.unwrap()
			.unwrap();
		assert_eq!(result, expected2);

		let result = RESPMsgReader::new(msg3.as_slice())
			.next_msg()
			.unwrap()
			.unwrap();
		assert_eq!(result, expected3);

		let result = RESPMsgReader::new(msg4.as_slice())
			.next_msg()
			.unwrap()
			.unwrap();
		assert_eq!(result, expected4);
	}

	#[test]
	fn test_decode_simple_message_from_reader() {
		let msg = b"+ala ma kota\r\n";
		let expected = RESPMsg::SimpleString(SimpleString("ala ma kota".into()));
		let result = RESPMsgReader::new(msg.as_slice())
			.next_msg()
			.unwrap()
			.unwrap();
		assert_eq!(result, expected);
	}

	#[test]
	fn test_decode_empty_array_from_reader() {
		let msg = b"*0\r\n";
		let expected = RESPMsg::Array(RESPArray(Vec::new()));
		let result = RESPMsgReader::new(msg.as_slice())
			.next_msg()
			.unwrap()
			.unwrap();
		assert_eq!(result, expected);
	}
}
