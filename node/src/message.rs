use std::fmt::Debug;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message<Body> {
    pub src: String,
    pub dest: String,
    pub body: Body
}


impl Message<MessageBody> {
    pub fn typ(&self) -> String {
        match &self.body() {
            MessageBody::Init {..} => String::from("init"),
            MessageBody::InitOk {..} => unreachable!(),
            MessageBody::Echo {..} => String::from("echo"),
            MessageBody::EchoOk {..} => String::from("echo_ok"),
            MessageBody::Generate {..} => String::from("generate"),
            MessageBody::GenerateOk {..} => String::from("generate_ok"),
            MessageBody::Broadcast {..} => String::from("broadcast"),
            MessageBody::BroadcastOk {..} => String::from("broadcast_ok"),
            MessageBody::Read {..} => String::from("read"),
            MessageBody::ReadOk {..} => String::from("read_ok"),
            MessageBody::Topology {..} => String::from("topology"),
            MessageBody::TopologyOk {..} => String::from("topology_ok"),
            MessageBody::Add {..} => String::from("add"),
            MessageBody::AddOk {..} => String::from("add_ok"),
            MessageBody::ShareCounterState {..} => String::from("share_counter_state")
        }
    }

    fn body(&self) -> &MessageBody {
        &self.body
    }

    pub fn msg_id(&self) -> Option<&u32> {
        match &self.body {
            MessageBody::Echo{msg_id, ..} => {
                Some(msg_id)
            },
            MessageBody::Init {msg_id, ..} => {
                Some(msg_id)
            },
            MessageBody::EchoOk {msg_id, ..} => {
                Some(msg_id)
            },
            MessageBody::Broadcast {msg_id, ..} => {
                Some(msg_id)
            },
            MessageBody::Generate {msg_id, ..} => {
                Some(msg_id)
            },
            MessageBody::Read {msg_id, ..} => {
                Some(msg_id)
            },
            MessageBody::Topology {msg_id, ..} => {
                Some(msg_id)
            }
            _ => None
        }
    }

    pub fn echo(&self) -> Option<&String> {
        match &self.body {
            MessageBody::Echo {echo, ..} => {
                Some(echo)
            },
            _ => None
        }
    }

    pub fn node_id(&self) -> Option<String> {
        match &self.body {
            MessageBody::Init {node_id, ..} => {
                Some(node_id.to_string())
            },
            _ => None
        }
    }

    pub fn node_ids(&self) -> Option<Vec<String>> {
        match &self.body {
            MessageBody::Init {node_ids, ..} => {
                Some(node_ids.clone())
            },
            _ => None
        }
    }
}

impl Into<MessageForm> for Message<MessageBody> {
    fn into(self) -> MessageForm {
        MessageForm::NodeMessage(self)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageBody {
    Echo {msg_id: u32, echo: String},
    Init {msg_id: u32, node_id: String, node_ids: Vec<String>},
    InitOk {in_reply_to: u32},
    EchoOk {msg_id: u32, in_reply_to: u32, echo: String},
    Generate {msg_id: u32},
    GenerateOk {msg_id: u32, id: u32, in_reply_to: u32},
    Broadcast {msg_id: u32, message: u32},
    BroadcastOk {in_reply_to: u32},
    Topology {msg_id: u32, topology: HashMap<String, Vec<String>>},
    TopologyOk {in_reply_to: u32},
    Add {msg_id: u32, delta: u32},
    AddOk {in_reply_to: u32},
    Read {msg_id: u32},
    ReadOk {in_reply_to: u32, value: u32},
    ShareCounterState {value: u32}
}

pub enum MessageForm {
    NodeMessage(Message<MessageBody>)
}


#[derive(Deserialize)]
pub struct RawMessage {
    src: String,
    dest: String,
    body: serde_json::Value
}

pub struct MaelstromMessage(pub String);
impl MaelstromMessage {
    pub fn to_deserialized_msg(&self) -> serde_json::Result<MessageForm> {
        let raw_msg: RawMessage = serde_json::from_str(&self.0)?;
        let body: MessageBody = serde_json::from_value(raw_msg.body)?;
        Ok(MessageForm::NodeMessage(Message {
            src: raw_msg.src,
            dest: raw_msg.dest,
            body
        }))
    }

    pub fn from_deserialized_msg(des_msg: MessageForm) -> serde_json::Result<String> {
        let ser_message = match des_msg {
            MessageForm::NodeMessage(node_msg) => serde_json::to_string(&node_msg)?,
        };

        Ok(ser_message)
    }
}

impl From<String> for MaelstromMessage {
    fn from(value: String) -> Self {
        MaelstromMessage(value)
    }
}


