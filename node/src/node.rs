use std::error::Error;
use std::io;
use std::io::Write;
use crate::message::{MaelstromMessage, Message};

pub struct Node {
    id: Option<String>,
    node_ids: Option<Vec<String>>,
    handlers: std::collections::HashMap<String, Box<dyn FnMut(Message)>>
}

impl Node {

    pub fn new() -> Self {
        Node {
            id: None,
            node_ids: None,
            handlers: std::collections::HashMap::new(),
        }
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

    fn use_default_handler(&mut self, init_msg: Message) -> Result<(), Box<dyn Error>> {
        match init_msg.typ() {
            "init" => {
                self.init_node(init_msg.node_id().unwrap(), init_msg.node_ids().unwrap());
                let msg = init_msg.generate_reply_msg();
                self.reply(msg)
            },

            _ => {
                let msg = init_msg.generate_reply_msg();
                self.reply(msg)
            }
        }
    }

    pub fn reply(&self, msg_to_send: Message) -> Result<(), Box<dyn Error>> {
        let output = MaelstromMessage::from_deserialized_msg(msg_to_send)?;

        println!("{}", output);
        io::stdout().flush()?;
        Ok(())
    }

    fn handle_message(&mut self, msg: Message) -> Result<(), Box<dyn Error>> {

        if !self.handlers.contains_key(msg.typ()) {
            return self.use_default_handler(msg)
        }

        let handler = self.handlers.get_mut(msg.typ()).unwrap();

        Ok((*handler)(msg))

    }

    pub fn run(&mut self)  {
        loop {
            let mut maelstrom_msg: MaelstromMessage = String::new().into();
            match io::stdin().read_line(&mut maelstrom_msg.0) {
                Err(e) => {
                    eprintln!("error occur while taking an output... {e}");
                    continue
                }
                Ok(_) => {
                    eprintln!("Received: {}", maelstrom_msg.0);
                }
            }

            let des_message = match maelstrom_msg.to_deserialized_msg() {
                Ok(des_msg) => des_msg,
                Err(_) => {
                    eprintln!("error occur while deserializing msg...");
                    continue
                }
            };

            eprintln!("{:?}", des_message);

            match self.handle_message(des_message) {
                Ok(()) => {
                    eprintln!("message was handled and sent")
                },
                Err(e) => {
                    eprintln!("error occur while handling a message: {e}")
                }
            }

        }
    }
}

