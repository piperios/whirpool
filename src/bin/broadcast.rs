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
    id: MsgId,
    seen_msgs: HashSet<MsgId>,
    topology: HashMap<String, Vec<String>>,
}

impl BroadcastNode {
    pub fn build_response(
        &mut self,
        pipe: &mut impl std::io::Write,
        msg: Message<MessageFields>,
    ) -> anyhow::Result<()> {
        self.id += 1;

        if let Some(msg) = match &msg.body.fields {
            MessageFields::Init {
                node_id: _,
                node_ids: _,
            } => Some(make_message(
                msg.dest,
                msg.src,
                MessageBody {
                    msg_id: None,
                    in_reply_to: msg.body.msg_id,
                    fields: MessageFields::InitOk,
                },
            )),
            MessageFields::Broadcast { message } => {
                if self.seen_msgs.insert(*message) {
                    self.topology
                        .values()
                        .try_for_each(|v| -> anyhow::Result<()> {
                            v.iter().filter(|n| n.as_str() != msg.src).try_for_each(
                                |n| -> anyhow::Result<()> {
                                    send_message(
                                        &mut *pipe,
                                        make_message(
                                            msg.dest.clone(),
                                            n.clone(),
                                            MessageBody {
                                                msg_id: self.id.into(),
                                                in_reply_to: None,
                                                fields: MessageFields::Broadcast {
                                                    message: *message,
                                                },
                                            },
                                        ),
                                    )?;
                                    Ok(())
                                },
                            )?;
                            Ok(())
                        })?;
                }

                Some(make_message(
                    msg.dest,
                    msg.src,
                    MessageBody {
                        msg_id: self.id.into(),
                        in_reply_to: msg.body.msg_id,
                        fields: MessageFields::BroadcastOk,
                    },
                ))
            }
            MessageFields::Read => Some(make_message(
                msg.dest,
                msg.src,
                MessageBody {
                    msg_id: self.id.into(),
                    in_reply_to: msg.body.msg_id,
                    fields: MessageFields::ReadOk {
                        messages: self.seen_msgs.iter().copied().collect(),
                    },
                },
            )),
            MessageFields::Topology { topology } => {
                self.topology = topology.to_owned();
                Some(make_message(
                    msg.dest,
                    msg.src,
                    MessageBody {
                        msg_id: self.id.into(),
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
        id: 0,
        seen_msgs: HashSet::new(),
        topology: HashMap::new(),
    };
    run(|stdout, msg| state_machine.build_response(stdout, msg))
}
