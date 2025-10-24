use std::error::Error;
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, oneshot, mpsc};
use crate::counter::Counter;
use crate::message::{MaelstromMessage, Message, MessageBody, MessageForm};
use crate::id_generator::IdGenerator;
use crate::kafka::Kafka;

pub struct Node {
    id: Option<String>,
    node_ids: Option<Vec<String>>,
    id_generator: Option<IdGenerator>,
    saved_messages: HashSet<u32>,
    broadcast_pending: Arc<Mutex<HashMap<u32, oneshot::Sender<()>>>>,
    output_sender: mpsc::UnboundedSender<MessageForm>,
    counter: Counter,
    kafka: Kafka
}

impl Node {
    fn id_generator(&mut self) -> &mut IdGenerator {
        self.id_generator.as_mut().unwrap()
    }

    pub async fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<MessageForm>();
        //init output writer
        tokio::spawn(async {
            Node::stdout_writer(rx).await
        });


        Node {
            id: None,
            node_ids: None,
            id_generator: None,
            saved_messages: HashSet::new(),
            broadcast_pending: Arc::new(Mutex::new(HashMap::new())),
            output_sender: tx,
            counter: Counter::new(),
            kafka: Kafka::new(),
        }
    }

    async fn init_node(&mut self, node_id: String, node_ids: Vec<String>) {
        let id_generator = IdGenerator::new(node_id.clone());
        self.id_generator = Some(id_generator);
        self.id = Some(node_id.clone());
        self.node_ids = Some(node_ids.clone());
        self.counter.init_counter_replication(node_id, node_ids, self.output_sender.clone())
    }

    async fn handle_init(&mut self, msg: Message<MessageBody>) -> Result<(), Box<dyn Error>> {
        self.init_node(msg.node_id().unwrap(), msg.node_ids().unwrap()).await;
        let msg = Message {
            src: String::from(&msg.dest),
            dest: String::from(&msg.src),
            body: MessageBody::InitOk {
                in_reply_to: *msg.msg_id().unwrap()
            }
        };
        self.output_sender.send(msg.into()).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn handle_echo(&self, msg: Message<MessageBody>) -> Result<(), Box<dyn Error>> {
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
        self.output_sender.send(msg.into()).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    async fn handle_generate(&mut self, msg: Message<MessageBody>) -> Result<(), Box<dyn Error>> {
        let msg_id = msg.msg_id().unwrap();
        let id = &self.id_generator().generate();
        let msg = Message {
            src: String::from(&msg.dest),
            dest: String::from(&msg.src),
            body: MessageBody::GenerateOk {
                msg_id: *msg_id,
                in_reply_to: *msg_id,
                id: *id
            }
        };
        self.output_sender.send(msg.into()).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn handle_topology(&self, msg: Message<MessageBody>) -> Result<(), Box<dyn Error>> {
        let msg_id = msg.msg_id().unwrap();
        let msg = Message {
            src: String::from(&msg.dest),
            dest: String::from(&msg.src),
            body: MessageBody::TopologyOk {
                in_reply_to: *msg_id,
            }
        };
        self.output_sender.send(msg.into()).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    async fn handle_broadcast(&mut self, msg: Message<MessageBody>) -> Result<(), Box<dyn Error>> {
        let (message, msg_id) = match msg.body {
            MessageBody::Broadcast {message, msg_id} => (message, msg_id),
            _ => unreachable!()
        };

        if self.saved_messages.insert(message) {
            self.replicate_to_peers(message).await;
        }

        let msg = Message {
            src: String::from(&msg.dest),
            dest: String::from(&msg.src),
            body: MessageBody::BroadcastOk {
                in_reply_to: msg_id,
            }
        };

        self.output_sender.send(msg.into()).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    async fn replicate_to_peers(&mut self, message: u32) {
        let nodes = self.node_ids.as_ref().unwrap().clone();
        let cur_node = self.id.as_ref().unwrap().to_string();

        for node in &nodes {
            //sending only to other nodes
            if cur_node.ne(node) {
                let generated_id = self.id_generator().generate();
                let pending_res = self.broadcast_pending.clone();
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

                let _  = output_sender.send(msg.clone().into());

                tokio::spawn(async move {

                    let msg = msg.clone();

                    let (tx, mut rx) = oneshot::channel::<()>();

                    let mut guard = pending_res.lock().await;

                    if let None = guard.get_mut(&generated_id) {
                        guard.insert(generated_id, tx);
                    }

                    //dropping mutex guard before sending msg attempts
                    drop(guard);

                    let mut attempts = 5;
                    loop {

                        let _ = output_sender.send(msg.clone().into());

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

    async fn handle_broadcast_ok(&mut self, msg: Message<MessageBody>) -> Result<(), Box<dyn Error>> {

        let MessageBody::BroadcastOk { in_reply_to } = msg.body else {
            return Ok(());
        };

        let mut guard = self.broadcast_pending.lock().await;

        if let Some(sender) = guard.remove(&in_reply_to) {
            let _ = sender.send(());
        }

        Ok(())
    }

    async fn handle_read(&self, msg: Message<MessageBody>) -> Result<(), Box<dyn Error>> {
        let msg_id = msg.msg_id().copied().unwrap();

        let v =  self.counter.read().await;

        let msg = Message {
            src: String::from(&msg.dest),
            dest: String::from(&msg.src),
            body: MessageBody::ReadOk {
                in_reply_to: msg_id,
                value: v
            }
        };

        Ok(self.output_sender.send(msg.into()).map_err(|e| Box::new(e) as Box<dyn Error>)?)
    }

    async fn handle_add(&mut self, msg: Message<MessageBody>) -> Result<(), Box<dyn Error>> {
        let MessageBody::Add {msg_id, delta} = msg.body else {
            return Ok(())
        };


        self.counter.add(&msg.dest, delta, false).await;

        let res = Message {
            src: String::from(&msg.dest),
            dest: String::from(&msg.src),
            body: MessageBody::AddOk {
                in_reply_to: msg_id,
            }
        };

        Ok(self.output_sender.send(res.into()).map_err(|e| Box::new(e) as Box<dyn Error>)?)
    }

    async fn handle_share_counter_state(&mut self, msg: Message<MessageBody>) {
        let MessageBody::ShareCounterState {value } = msg.body else { return };

        self.counter.add(&msg.src, value, true).await;
    }

    fn handle_send(&mut self, node_msg: Message<MessageBody>) -> Result<(), Box<dyn Error>>{
        let MessageBody::Send {msg_id, key, msg} = node_msg.body else {
            return Ok(())
        };

        let offset = self.kafka.write_log(key, msg);

        let msg = Message {
            src: String::from(&node_msg.dest),
            dest: String::from(&node_msg.src),
            body: MessageBody::SendOk {
                in_reply_to: msg_id,
                offset,
            }
        };

        Ok(self.output_sender.send(msg.into()).map_err(|e| Box::new(e) as Box<dyn Error>)?)
    }

    fn handle_poll(&self, msg: Message<MessageBody>) -> Result<(), Box<dyn Error>> {
        let MessageBody::Poll {msg_id, offsets} = msg.body else {
            return Ok(())
        };

        let msgs = self.kafka.read_logs(offsets);

        let msg = Message {
            src: String::from(&msg.dest),
            dest: String::from(&msg.src),
            body: MessageBody::PollOk {
                in_reply_to: msg_id,
                msgs,
            }
        };

        Ok(self.output_sender.send(msg.into()).map_err(|e| Box::new(e) as Box<dyn Error>)?)
    }

    fn handle_commit_offsets(&mut self, node_msg: Message<MessageBody>) -> Result<(), Box<dyn Error>> {
        let MessageBody::CommitOffsets {msg_id, offsets} = node_msg.body else {
            return Ok(())
        };

        let msg = Message {
            src: String::from(&node_msg.dest),
            dest: String::from(&node_msg.src),
            body: MessageBody::CommitOffsetsOk {
                in_reply_to: msg_id,
            }
        };

        self.kafka.commit_offsets(node_msg.src, offsets);



        Ok(self.output_sender.send(msg.into()).map_err(|e| Box::new(e) as Box<dyn Error>)?)
    }

    fn handle_list_commited_offsets(&self, msg: Message<MessageBody>) -> Result<(), Box<dyn Error>> {
        let MessageBody::ListCommittedOffsets {msg_id, keys} = msg.body else {
            return Ok(())
        };

        let commited_offsets = self.kafka.get_commited_offsets(&msg.src, keys);

        let msg = Message {
            src: String::from(&msg.dest),
            dest: String::from(&msg.src),
            body: MessageBody::ListCommittedOffsetsOk {
                in_reply_to: msg_id,
                offsets: commited_offsets
            }
        };

        Ok(self.output_sender.send(msg.into()).map_err(|e| Box::new(e) as Box<dyn Error>)?)
    }

    async fn handle_message(&mut self, msg_form: MessageForm) -> Result<(), Box<dyn Error>> {

        match msg_form {
            MessageForm::NodeMessage(node_msg) => {
                let msg_type = node_msg.typ();
                match msg_type.as_str() {
                    "init" => self.handle_init(node_msg).await?,
                    "echo" => self.handle_echo(node_msg)?,
                    "generate" => self.handle_generate(node_msg).await?,
                    "topology" => self.handle_topology(node_msg)?,
                    "broadcast" => self.handle_broadcast(node_msg).await?,
                    "broadcast_ok" => self.handle_broadcast_ok(node_msg).await?,
                    "read" => self.handle_read(node_msg).await?,
                    "add" => self.handle_add(node_msg).await?,
                    "share_counter_state" => self.handle_share_counter_state(node_msg).await,
                    "send" => self.handle_send(node_msg)?,
                    "poll" => self.handle_poll(node_msg)?,
                    "commit_offsets" => self.handle_commit_offsets(node_msg)?,
                    "list_committed_offsets" => self.handle_list_commited_offsets(node_msg)?,
                    _ => unimplemented!()
                }
            },
        }

        Ok(())
    }

    pub async fn stdout_writer(mut rx: mpsc::UnboundedReceiver<MessageForm>) {
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



