use serde::{Deserialize, Serialize};
use whirpool::{Message, MessageBody, MsgId, run, send_messages};

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageFields {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Add {
        delta: usize,
    },
    AddOk,
    Read,
    ReadOk {
        value: usize,
    },
    Gossip {
        counters: std::collections::HashMap<String, usize>,
    },
    Error {
        code: usize,
        text: String,
    },
}

pub struct GrowCounterNode {
    msg_id: MsgId,
    counters: std::collections::HashMap<String, usize>,
    node_id: String,
    nodes: Vec<String>,
    last_gossip_ts: std::time::Instant,
}

impl GrowCounterNode {
    const GOSSIP_TRANSMISSION_INTERVAL_MS: std::time::Duration =
        std::time::Duration::from_millis(25);

    pub fn respond(&mut self, msg: Message<MessageFields>) -> Vec<Message<MessageFields>> {
        self.msg_id += 1;

        let mut out: Vec<Message<MessageFields>> = Vec::new();

        if std::time::Instant::now().duration_since(self.last_gossip_ts)
            >= GrowCounterNode::GOSSIP_TRANSMISSION_INTERVAL_MS
        {
            self.nodes
                .iter()
                .filter(|s| **s != self.node_id)
                .for_each(|dest| {
                    out.push(Message::new(
                        msg.dest.clone(),
                        dest.clone(),
                        MessageBody {
                            msg_id: self.msg_id.into(),
                            in_reply_to: None,
                            fields: MessageFields::Gossip {
                                counters: self.counters.clone(),
                            },
                        },
                    ))
                });

            self.last_gossip_ts = std::time::Instant::now();
        }

        let reply = match &msg.body.fields {
            MessageFields::Init { node_id, node_ids } => {
                self.nodes = node_ids.clone();
                self.node_id = node_id.clone();

                self.nodes.iter().for_each(|n| {
                    self.counters.insert(n.clone(), 0);
                });

                Some(Message::new(
                    msg.dest,
                    msg.src,
                    MessageBody {
                        msg_id: None,
                        in_reply_to: msg.body.msg_id,
                        fields: MessageFields::InitOk,
                    },
                ))
            }
            MessageFields::Add { delta } => {
                let current_local_counter = self.counters.get_mut(&self.node_id).unwrap();
                *current_local_counter += *delta;

                Some(Message::new(
                    msg.dest,
                    msg.src,
                    MessageBody {
                        msg_id: None,
                        in_reply_to: msg.body.msg_id,
                        fields: MessageFields::AddOk,
                    },
                ))
            }
            MessageFields::Read => Some(Message::new(
                msg.dest,
                msg.src,
                MessageBody {
                    msg_id: None,
                    in_reply_to: msg.body.msg_id,
                    fields: MessageFields::ReadOk {
                        value: self.counters.values().sum(),
                    },
                },
            )),
            MessageFields::Gossip { counters } => {
                for (node, &counter) in counters {
                    let ent = self.counters.get_mut(node).unwrap();
                    *ent = (*ent).max(counter);
                }

                None
            }
            _ => None,
        };

        if let Some(reply) = reply {
            out.push(reply);
        }

        out
    }
}

fn main() -> anyhow::Result<()> {
    let mut state_machine = GrowCounterNode {
        msg_id: 0,
        counters: std::collections::HashMap::new(),
        node_id: "".to_owned(),
        nodes: Vec::new(),
        last_gossip_ts: std::time::Instant::now(),
    };
    run(|stdout, msg| send_messages(stdout, state_machine.respond(msg)))
}
