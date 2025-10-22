use std::ops::Add;

pub struct IdGenerator {
    counter: u32,
    node_id: String
}

impl IdGenerator {
    pub fn new(node_id: String) -> Self {
        IdGenerator {
            counter: 0,
            node_id
        }
    }

    pub fn generate(&mut self) -> u32 {
        let counter_str = self.counter.to_string();
        let id = counter_str.add(&self.node_id[1..]).parse::<u32>().expect("error occur while generating unique id");
        self.counter += 1;
        id
    }
}