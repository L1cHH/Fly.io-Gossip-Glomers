use std::ops::Add;

pub struct IdGenerator {
    counter: u32
}

impl IdGenerator {
    pub fn new() -> Self {
        IdGenerator {
            counter: 0
        }
    }

    pub fn generate(&mut self, node_id: &str) -> u32 {
        let counter_str = self.counter.to_string();
        let id = counter_str.add(&node_id[1..]).parse::<u32>().expect("error occur while generating unique id");
        self.counter += 1;
        id
    }
}