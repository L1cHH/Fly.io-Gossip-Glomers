use std::collections::HashMap;

pub struct Kafka {
    // storage for logs that client have seen last time
    // client_id -> log_key & last seen offset for its log_key
    last_seen_logs: HashMap<String, HashMap<String, usize>>,
    // storage for logs
    // log_key & logs for its log_key
    logs: HashMap<String, Vec<u32>>
}

impl Kafka {

    pub fn new() -> Self {
        Kafka {
            last_seen_logs: HashMap::new(),
            logs: HashMap::new()
        }
    }

    pub fn write_log(&mut self, key: String, msg: u32) -> usize {
        match self.logs.get_mut(&key) {
            Some(logs) => {
                let logs_len = logs.len();
                logs.push(msg);
                logs_len
            },
            None => {
                self.logs.insert(key, vec![msg]);
                0
            }
        }
    }

    fn get_logs_from_offset(&self, log_key: &String, offset: usize) -> Option<Vec<(usize, u32)>> {
        match self.logs.get(log_key) {
            Some(logs) => {
                let logs = logs.iter().enumerate().skip(offset).map(|(index, el)| (index, *el)).collect::<Vec<(usize, u32)>>();
                Some(logs)
            },
            None => None
        }

    }

    pub fn read_logs(&self, offsets: HashMap<String, usize>) -> HashMap<String, Vec<(usize, u32)>> {
        let mut logs = HashMap::new();

        for (log_k, offset) in offsets {
            if let Some(logs_from_offset) = self.get_logs_from_offset(&log_k, offset) {
                logs.insert(log_k, logs_from_offset);
            }

        }

        logs
    }

    pub fn commit_offsets(&mut self, client: String, offsets: HashMap<String, usize>) {
        self.last_seen_logs.insert(client, offsets);
    }

    pub fn get_commited_offsets(&self, client: &String, log_keys: Vec<String>) -> HashMap<String, usize> {
        match self.last_seen_logs.get(client) {
            Some(offsets) => {
                log_keys
                    .into_iter()
                    .filter_map(|key| {
                        offsets.get(&key).map(|&offset| (key, offset))
                    })
                    .collect()
            },
            None => {
                HashMap::new()
            }
        }
    }
}