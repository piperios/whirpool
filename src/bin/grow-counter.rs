use serde::{Deserialize, Serialize};
use whirpool::{Message, MessageBody, MsgId, make_message, run, send_message};

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

    pub fn build_response(
        &mut self,
        pipe: &mut impl std::io::Write,
        msg: Message<MessageFields>,
    ) -> anyhow::Result<()> {
        self.msg_id += 1;

        if std::time::Instant::now().duration_since(self.last_gossip_ts)
            >= GrowCounterNode::GOSSIP_TRANSMISSION_INTERVAL_MS
        {
            self.nodes.iter().try_for_each(|n| -> anyhow::Result<()> {
                if n == &self.node_id {
                    return Ok(());
                }

                send_message(
                    &mut *pipe,
                    make_message(
                        msg.dest.clone(),
                        n.clone(),
                        MessageBody {
                            msg_id: self.msg_id.into(),
                            in_reply_to: None,
                            fields: MessageFields::Gossip {
                                counters: self.counters.clone(),
                            },
                        },
                    ),
                )?;

                Ok(())
            })?;

            self.last_gossip_ts = std::time::Instant::now();
        }

        if let Some(msg) = match &msg.body.fields {
            MessageFields::Init { node_id, node_ids } => {
                self.nodes = node_ids.clone();
                self.node_id = node_id.clone();

                self.nodes.iter().for_each(|n| {
                    self.counters.insert(n.clone(), 0);
                });

                Some(make_message(
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

                Some(make_message(
                    msg.dest,
                    msg.src,
                    MessageBody {
                        msg_id: None,
                        in_reply_to: msg.body.msg_id,
                        fields: MessageFields::AddOk,
                    },
                ))
            }
            MessageFields::Read => Some(make_message(
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
        } {
            send_message(&mut *pipe, msg)?;
        }

        Ok(())
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
    run(|stdout, msg| state_machine.build_response(stdout, msg))
}
