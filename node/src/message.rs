use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    src: String,
    dest: String,
    body: MessageBody
}

impl Message {
    pub fn typ(&self) -> &str {
        &self.body().msg_type
    }

    fn body(&self) -> &MessageBody {
        &self.body
    }

    fn msg_id(&self) -> Option<&i32> {
        self.body().msg_id.as_ref()
    }

    fn echo(&self) -> Option<&String> {
        self.body().echo.as_ref()
    }

    pub fn generate_reply_msg(self) -> Self {
        match self.typ() {
            "init" => {
                let msg_id = self.msg_id().unwrap();
                Message {
                    src: String::from(&self.dest),
                    dest: String::from(&self.src),
                    body: MessageBody {
                        msg_type: String::from("init_ok"),
                        msg_id: None,
                        echo: None,
                        in_reply_to: Some(msg_id).copied(),
                        node_ids: None,
                        node_id: None
                    }
                }
            },
            "echo" => {
                let msg_id = self.msg_id().unwrap();
                let echo = self.echo().unwrap();
                Message {
                    src: String::from(&self.dest),
                    dest: String::from(&self.src),
                    body: MessageBody {
                        msg_type: String::from("echo_ok"),
                        msg_id: Some(msg_id).copied(),
                        in_reply_to: Some(msg_id).copied(),
                        echo: Some(String::from(echo)),
                        node_ids: None,
                        node_id: None
                    }
                }
            },
            _ => {unimplemented!()}
        }

    }

    pub fn node_id(&self) -> Option<String> {
        if let Some(node_id) = &self.body().node_id {
            Some(node_id.to_string())
        } else {
            None
        }

    }

    pub fn node_ids(&self) -> Option<Vec<String>> {
        if let Some(node_ids) = &self.body().node_ids {
            Some(node_ids.clone())
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct MessageBody {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    echo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_ids: Option<Vec<String>>
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


