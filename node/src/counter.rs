use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use crate::message::{Message, MessageBody, MessageForm};

pub struct Counter {
    value: Arc<Mutex<HashMap<String, u32>>>
}


impl Counter {

    pub fn new() -> Self {
        Counter {
            value: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub async fn add(&mut self, node_id: &String, value: u32, with_replace: bool) {
        let mut guard = self.value.lock().await;

        match guard.get_mut(node_id) {
            None => { guard.insert(node_id.clone(), value); },
            Some(v) => {
                if with_replace {
                    *v = value
                } else {
                    *v += value
                }
            }
        }
    }

    pub async fn read(&self) -> u32 {
        let guard = self.value.lock().await;
        guard.values().map(|v| v).sum()
    }

    pub fn init_counter_replication(&mut self, cur_node: String, other_nodes: Vec<String>, out: mpsc::UnboundedSender<MessageForm>) {
        let counter_v = self.value.clone();
        tokio::spawn(async move {
           loop {

               let guard = counter_v.lock().await;

               if guard.is_empty() {
                   drop(guard);
                   tokio::time::sleep(Duration::from_millis(500)).await;
                   continue
               }

               for node in &other_nodes {
                   if cur_node.eq(node) { continue }

                   let v = guard.get(&cur_node).unwrap_or(&0);

                   let msg = Message {
                       src: String::from(&cur_node),
                       dest: String::from(node),
                       body: MessageBody::ShareCounterState {
                           value: *v
                       }
                   };

                   let _ = out.send(msg.into());
               }
               drop(guard);

               tokio::time::sleep(Duration::from_millis(300)).await;
           }
        });
    }
}