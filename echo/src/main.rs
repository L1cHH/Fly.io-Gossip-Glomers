use node::node::Node;

#[tokio::main]
async fn main() {
    let mut node = Node::new().await;
    let _ = node.run().await;
}

