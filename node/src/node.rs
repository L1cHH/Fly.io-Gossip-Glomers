use std::error::Error;
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, oneshot, mpsc};
use crate::message::{MaelstromMessage, Message, MessageBody};
use crate::id_generator::IdGenerator;
//TODO: изменить pending_responses на hashmap<msg_id, tx> (то есть без самого отправителя)
pub struct Node {
    id: Option<String>,
    node_ids: Option<Vec<String>>,
    id_generator: IdGenerator,
    handlers: HashMap<String, Box<dyn FnMut(Message)>>,
    saved_messages: HashSet<u32>,
    pending_responses: Arc<Mutex<HashMap<u32, oneshot::Sender<()>>>>,
    output_sender: mpsc::UnboundedSender<Message>
}

impl Node {

    fn node_id(&self) -> Option<&String> {
        match &self.id {
            Some(id) => Some(id),
            None => None
        }
    }

    pub async fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<Message>();
        //init output writer
        tokio::spawn(async {
            Node::stdout_writer(rx).await
        });

        Node {
            id: None,
            node_ids: None,
            id_generator: IdGenerator::new(),
            handlers: HashMap::new(),
            saved_messages: HashSet::new(),
            pending_responses: Arc::new(Mutex::new(HashMap::new())),
            output_sender: tx
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

    fn handle_init(&mut self, msg: Message) -> Result<(), Box<dyn Error>> {
        self.init_node(msg.node_id().unwrap(), msg.node_ids().unwrap());
        let msg = Message {
            src: String::from(&msg.dest),
            dest: String::from(&msg.src),
            body: MessageBody::InitOk {
                in_reply_to: *msg.msg_id().unwrap()
            }
        };
        self.output_sender.send(msg).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn handle_echo(&self, msg: Message) -> Result<(), Box<dyn Error>> {
        let msg_id = msg.msg_id().unwrap();
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
        self.output_sender.send(msg).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn handle_generate(&mut self, msg: Message) -> Result<(), Box<dyn Error>> {
        let msg_id = msg.msg_id().unwrap();
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
        self.output_sender.send(msg).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn handle_topology(&self, msg: Message) -> Result<(), Box<dyn Error>> {
        let msg_id = msg.msg_id().unwrap();
        let msg = Message {
            src: String::from(&msg.dest),
            dest: String::from(&msg.src),
            body: MessageBody::TopologyOk {
                in_reply_to: *msg_id,
            }
        };
        self.output_sender.send(msg).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn handle_read(&self, msg: Message) -> Result<(), Box<dyn Error>> {
        let msg_id = msg.msg_id().unwrap();
        let msg = Message {
            src: String::from(&msg.dest),
            dest: String::from(&msg.src),
            body: MessageBody::ReadOk {
                messages: self.saved_messages.clone(),
                in_reply_to: *msg_id,
            }
        };

        self.output_sender.send(msg).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    async fn handle_broadcast(&mut self, msg: Message) -> Result<(), Box<dyn Error>> {
        let (message, msg_id) = match msg.body {
            MessageBody::Broadcast {message, msg_id} => (message, msg_id),
            _ => unreachable!()
        };

        if self.saved_messages.insert(message) {

            self.replicate_to_peers(message).await;
            let msg = Message {
                src: String::from(&msg.dest),
                dest: String::from(&msg.src),
                body: MessageBody::BroadcastOk {
                    in_reply_to: msg_id,
                }
            };
            self.output_sender.send(msg).map_err(|e| Box::new(e) as Box<dyn Error>)
        } else {
            let msg = Message {
                src: String::from(&msg.dest),
                dest: String::from(&msg.src),
                body: MessageBody::BroadcastOk {
                    in_reply_to: msg_id,
                }
            };

            self.output_sender.send(msg).map_err(|e| Box::new(e) as Box<dyn Error>)
        }
    }

    async fn replicate_to_peers(&mut self, message: u32) {
        let nodes = self.node_ids.as_ref().unwrap();
        // let mut fut_set = tokio::task::JoinSet::new();
        let cur_node = self.id.as_ref().unwrap().to_string();

        for node in nodes {
            //sending only to other nodes
            if cur_node.ne(node) {
                let generated_id = self.id_generator.generate(self.id.as_ref().unwrap());
                let pending_res = self.pending_responses.clone();
                let src = cur_node.clone();
                let dest = String::from(node);
                let output_sender = self.output_sender.clone();

                let msg = Message {
                    src,
                    dest,
                    body: MessageBody::Broadcast {
                        msg_id: generated_id,
                        message
                    }
                };

                let _  = output_sender.send(msg.clone());

                tokio::spawn(async move {

                    let msg = msg.clone();

                    let (tx, mut rx) = oneshot::channel::<()>();

                    let mut guard = pending_res.lock().await;

                    if let None = guard.get_mut(&generated_id) {
                        guard.insert(generated_id, tx);
                    }

                    let mut attempts = 5;
                    loop {

                        let _ = output_sender.send(msg.clone());

                        tokio::select! {
                            _ = &mut rx => break,
                            _ = tokio::time::sleep(tokio::time::Duration::from_millis(300)) => {
                                attempts -= 1;
                                if attempts == 0 {
                                    eprintln!("no response for msg_id:{} and value:{}", generated_id, message);
                                    break
                                }
                            }
                        }
                    }

                });
            }
        }

    }

    async fn handle_broadcast_ok(&mut self, msg: Message) -> Result<(), Box<dyn Error>> {

        let MessageBody::BroadcastOk { in_reply_to } = msg.body else {
            return Ok(());
        };

        let mut guard = self.pending_responses.lock().await;

        if let Some(sender) = guard.remove(&in_reply_to) {
            let _ = sender.send(());
        }

        Ok(())
    }

    async fn handle_message(&mut self, msg: Message) -> Result<(), Box<dyn Error>> {

        let msg_type = msg.typ();

        match msg_type.as_str() {
            "init" => self.handle_init(msg)?,
            "echo" => self.handle_echo(msg)?,
            "generate" => self.handle_generate(msg)?,
            "read" => self.handle_read(msg)?,
            "topology" => self.handle_topology(msg)?,
            "broadcast" => self.handle_broadcast(msg).await?,
            "broadcast_ok" => self.handle_broadcast_ok(msg).await?,
            _ => unimplemented!()
        }


        Ok(())
    }

    pub async fn stdout_writer(mut rx: mpsc::UnboundedReceiver<Message>) {
        let mut stdout = tokio::io::stdout();
        while let Some(msg) = rx.recv().await {
            let output = match MaelstromMessage::from_deserialized_msg(msg) {
                Ok(ser_msg) => ser_msg,
                Err(_) => {
                    eprintln!("error occur while serializing msg...");
                    continue
                }
            };

            let mut bytes = output.into_bytes();
            bytes.push(b'\n');

            if let Err(e) = stdout.write_all(&bytes).await {
                eprintln!("error writing to stdout: {e}");
                continue;
            }

            // flush обязательно обрабатываем
            if let Err(e) = stdout.flush().await {
                eprintln!("error flushing stdout: {e}");
            }
        }
    }

    pub async fn run(&mut self) -> tokio::io::Result<()> {

        let buf = tokio::io::BufReader::new(tokio::io::stdin());
        let mut lines = buf.lines();

        while let Some(line) = lines.next_line().await? {
            let maelstrom_msg = MaelstromMessage::from(line);
            let des_message = match maelstrom_msg.to_deserialized_msg() {
                Ok(des_msg) => des_msg,
                Err(_) => {
                    eprintln!("error occur while deserializing msg...");
                    continue
                }
            };

            if let Err(e) = self.handle_message(des_message).await {
                eprintln!("error occur while handling a message: {e}")
            }

        };
        Ok(())
    }
}

