use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use whirpool::{Message, MessageBody, MsgId, run, send_messages};

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageFields {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Broadcast {
        message: MsgId,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<MsgId>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        seen: Vec<MsgId>,
    },
    Error {
        code: usize,
        text: String,
    },
}

pub struct BroadcastNode {
    msg_id: MsgId,
    seen_msgs: HashMap<String, HashSet<MsgId>>,
    topology: HashMap<String, Vec<String>>,
    node_id: String,
    last_gossip_ts: std::time::Instant,
}

impl BroadcastNode {
    const GOSSIP_TRANSMISSION_INTERVAL_MS: std::time::Duration =
        std::time::Duration::from_millis(50);

    pub fn respond(&mut self, msg: Message<MessageFields>) -> Vec<Message<MessageFields>> {
        self.msg_id += 1;

        let mut out: Vec<Message<MessageFields>> = Vec::new();

        if std::time::Instant::now().duration_since(self.last_gossip_ts)
            >= BroadcastNode::GOSSIP_TRANSMISSION_INTERVAL_MS
        {
            let seen_msgs: Vec<_> = self.seen_msgs[&self.node_id].iter().copied().collect();

            if let Some(neighbors) = self.topology.get(&self.node_id) {
                neighbors
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
                                    seen: seen_msgs.clone(),
                                },
                            },
                        ))
                    });
            }

            self.last_gossip_ts = std::time::Instant::now();
        }

        let reply = match &msg.body.fields {
            MessageFields::Init { node_id, .. } => {
                self.node_id = node_id.clone();
                self.seen_msgs.insert(node_id.clone(), HashSet::new());

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
            MessageFields::Broadcast { message } => {
                self.seen_msgs
                    .get_mut(&self.node_id)
                    .unwrap()
                    .insert(*message);

                Some(Message::new(
                    msg.dest,
                    msg.src,
                    MessageBody {
                        msg_id: self.msg_id.into(),
                        in_reply_to: msg.body.msg_id,
                        fields: MessageFields::BroadcastOk,
                    },
                ))
            }
            MessageFields::Read => Some(Message::new(
                msg.dest,
                msg.src,
                MessageBody {
                    msg_id: self.msg_id.into(),
                    in_reply_to: msg.body.msg_id,
                    fields: MessageFields::ReadOk {
                        messages: self.seen_msgs[&self.node_id].iter().copied().collect(),
                    },
                },
            )),
            MessageFields::Topology { topology } => {
                topology.iter().for_each(|(k, v)| {
                    self.topology.insert(k.clone(), v.clone());
                });

                Some(Message::new(
                    msg.dest,
                    msg.src,
                    MessageBody {
                        msg_id: self.msg_id.into(),
                        in_reply_to: msg.body.msg_id,
                        fields: MessageFields::TopologyOk,
                    },
                ))
            }
            MessageFields::Gossip { seen } => {
                self.seen_msgs
                    .get_mut(&self.node_id)
                    .unwrap()
                    .extend(seen.iter().cloned());

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
    let mut state_machine = BroadcastNode {
        msg_id: 0,
        node_id: "".to_owned(),
        seen_msgs: HashMap::new(),
        topology: HashMap::new(),
        last_gossip_ts: std::time::Instant::now(),
    };
    run(|stdout, msg| send_messages(stdout, state_machine.respond(msg)))
}
