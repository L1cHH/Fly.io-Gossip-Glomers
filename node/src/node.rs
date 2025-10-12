use std::error::Error;
use std::io;
use std::io::Write;
use crate::message::{MaelstromMessage, Message, MessageBody};
use crate::id_generator::IdGenerator;

pub struct Node {
    id: Option<String>,
    node_ids: Option<Vec<String>>,
    id_generator: IdGenerator,
    handlers: std::collections::HashMap<String, Box<dyn FnMut(Message)>>,
    saved_messages: Vec<u32>
}

impl Node {

    fn node_id(&self) -> Option<&String> {
        match &self.id {
            Some(id) => Some(id),
            None => None
        }
    }

    pub fn new() -> Self {
        Node {
            id: None,
            node_ids: None,
            id_generator: IdGenerator::new(),
            handlers: std::collections::HashMap::new(),
            saved_messages: Vec::new()
        }
    }

    fn save_message(&mut self, msg: u32) {
        self.saved_messages.push(msg)
    }

    fn init_node(&mut self, node_id: String, node_ids: Vec<String>) {
        self.id = Some(node_id);
        self.node_ids = Some(node_ids);
    }

    pub fn handle<H: FnMut(Message) + 'static>(&mut self, type_message: String, handler: H) -> Result<(), String>{
        if self.handlers.contains_key(&type_message) {
            Err(String::from("handler for this message type already exists..."))
        } else {
            self.handlers.insert(type_message, Box::new(handler));
            Ok(())
        }
    }

    fn use_default_handler(&mut self, msg: Message) -> Result<(), Box<dyn Error>> {
        let msg_id = msg.msg_id().unwrap();
        match msg.typ().as_str() {
            "init" => {
                self.init_node(msg.node_id().unwrap(), msg.node_ids().unwrap());
                let msg = Message {
                    src: String::from(&msg.dest),
                    dest: String::from(&msg.src),
                    body: MessageBody::InitOk {
                        in_reply_to: *msg_id
                    }
                };
                self.reply(msg)
            },

            "echo" => {
                let echo = msg.echo().unwrap();
                let msg = Message {
                    src: String::from(&msg.dest),
                    dest: String::from(&msg.src),
                    body: MessageBody::EchoOk {
                        msg_id: *msg_id,
                        in_reply_to: *msg_id,
                        echo: String::from(echo)
                    }
                };
                self.reply(msg)
            },

            "generate" => {
                let node_id = self.node_id().unwrap().to_string();
                let id = &self.id_generator.generate(&node_id);
                let msg = Message {
                    src: String::from(&msg.dest),
                    dest: String::from(&msg.src),
                    body: MessageBody::GenerateOk {
                        msg_id: *msg_id,
                        in_reply_to: *msg_id,
                        id: *id
                    }
                };
                self.reply(msg)
            },

            "broadcast" => {
                if let MessageBody::Broadcast {message, msg_id} = msg.body {
                    self.save_message(message);
                    let msg = Message {
                        src: String::from(&msg.dest),
                        dest: String::from(&msg.src),
                        body: MessageBody::BroadcastOk {
                            in_reply_to: msg_id,
                        }
                    };
                    self.reply(msg)
                } else {
                    unreachable!()
                }
            },

            "read" => {
                let msg = Message {
                    src: String::from(&msg.dest),
                    dest: String::from(&msg.src),
                    body: MessageBody::ReadOk {
                        messages: self.saved_messages.clone(),
                        in_reply_to: *msg_id,
                    }
                };
                self.reply(msg)
            },

            "topology" => {
                let msg = Message {
                    src: String::from(&msg.dest),
                    dest: String::from(&msg.src),
                    body: MessageBody::TopologyOk {
                        in_reply_to: *msg_id,
                    }
                };

                self.reply(msg)
            }
            _ => {unimplemented!()}
        }
    }

    pub fn reply(&self, msg_to_send: Message) -> Result<(), Box<dyn Error>> {
        let output = MaelstromMessage::from_deserialized_msg(msg_to_send)?;

        println!("{}", output);
        io::stdout().flush()?;
        Ok(())
    }

    fn handle_message(&mut self, msg: Message) -> Result<(), Box<dyn Error>> {

        let msg_type = msg.typ();

        if !self.handlers.contains_key(&msg_type) {
            return self.use_default_handler(msg)
        }

        let handler = self.handlers.get_mut(&msg_type).unwrap();

        Ok((*handler)(msg))

    }

    pub fn run(&mut self)  {
        loop {
            let mut maelstrom_msg: MaelstromMessage = String::new().into();
            if let Err(e) = io::stdin().read_line(&mut maelstrom_msg.0) {
                eprintln!("error occur while taking an output... {e}");
                continue
            }

            let des_message = match maelstrom_msg.to_deserialized_msg() {
                Ok(des_msg) => des_msg,
                Err(_) => {
                    eprintln!("error occur while deserializing msg...");
                    continue
                }
            };

            if let Err(e) = self.handle_message(des_message) {
                eprintln!("error occur while handling a message: {e}")
            }

        }
    }
}

