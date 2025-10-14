use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: MessageBody
}

impl Message {
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

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum MessageBody {
    #[serde(rename = "echo")]
    Echo {msg_id: u32, echo: String},
    #[serde(rename = "init")]
    Init {msg_id: u32, node_id: String, node_ids: Vec<String>},
    #[serde(rename = "init_ok")]
    InitOk {in_reply_to: u32},
    #[serde(rename = "echo_ok")]
    EchoOk {msg_id: u32, in_reply_to: u32, echo: String},
    #[serde(rename = "generate")]
    Generate {msg_id: u32},
    #[serde(rename = "generate_ok")]
    GenerateOk {msg_id: u32, id: u32, in_reply_to: u32},
    #[serde(rename = "broadcast")]
    Broadcast {msg_id: u32, message: u32},
    #[serde(rename = "broadcast_ok")]
    BroadcastOk {in_reply_to: u32},
    #[serde(rename = "read")]
    Read {msg_id: u32},
    #[serde(rename = "read_ok")]
    ReadOk {in_reply_to: u32, messages: std::collections::HashSet<u32>},
    #[serde(rename = "topology")]
    Topology {msg_id: u32, topology: std::collections::HashMap<String, Vec<String>>},
    #[serde(rename = "topology_ok")]
    TopologyOk {in_reply_to: u32}
}


pub struct MaelstromMessage(pub String);
impl MaelstromMessage {
    pub fn to_deserialized_msg(&self) -> serde_json::Result<Message> {
        let des_message: Message = serde_json::from_str(&self.0)?;
        Ok(des_message)
    }

    pub fn from_deserialized_msg(des_msg: Message) -> serde_json::Result<String> {
        let ser_message = serde_json::to_string(&des_msg)?;
        Ok(ser_message)
    }
}

impl From<String> for MaelstromMessage {
    fn from(value: String) -> Self {
        MaelstromMessage(value)
    }
}


