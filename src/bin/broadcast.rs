use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use whirpool::{Message, MessageBody, MsgId, make_message, run, send_message};

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
    nodes: Vec<String>,
}

impl BroadcastNode {
    pub fn build_response(
        &mut self,
        pipe: &mut impl std::io::Write,
        msg: Message<MessageFields>,
    ) -> anyhow::Result<()> {
        self.msg_id += 1;

        if let Some(msg) = match &msg.body.fields {
            MessageFields::Init { node_id, node_ids } => {
                self.nodes = node_ids.clone();
                self.node_id = node_id.clone();
                self.seen_msgs.insert(node_id.clone(), HashSet::new());

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
            MessageFields::Broadcast { message } => {
                if self
                    .seen_msgs
                    .get_mut(&self.node_id)
                    .unwrap()
                    .insert(*message)
                {
                    self.topology
                        .get(&self.node_id)
                        .into_iter()
                        .flatten()
                        .try_for_each(|n| -> anyhow::Result<()> {
                            let dest = n.clone();
                            if *dest == msg.src {
                                return Ok(());
                            }

                            send_message(
                                &mut *pipe,
                                make_message(
                                    msg.dest.clone(),
                                    dest.clone(),
                                    MessageBody {
                                        msg_id: self.msg_id.into(),
                                        in_reply_to: None,
                                        fields: MessageFields::Broadcast { message: *message },
                                    },
                                ),
                            )?;

                            Ok(())
                        })?;
                }

                Some(make_message(
                    msg.dest,
                    msg.src,
                    MessageBody {
                        msg_id: self.msg_id.into(),
                        in_reply_to: msg.body.msg_id,
                        fields: MessageFields::BroadcastOk,
                    },
                ))
            }
            MessageFields::Read => Some(make_message(
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

                Some(make_message(
                    msg.dest,
                    msg.src,
                    MessageBody {
                        msg_id: self.msg_id.into(),
                        in_reply_to: msg.body.msg_id,
                        fields: MessageFields::TopologyOk,
                    },
                ))
            }
            _ => None,
        } {
            send_message(&mut *pipe, msg)?;
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut state_machine = BroadcastNode {
        msg_id: 0,
        node_id: "".to_owned(),
        seen_msgs: HashMap::new(),
        topology: HashMap::new(),
        nodes: Vec::new(),
    };
    run(|stdout, msg| state_machine.build_response(stdout, msg))
}
