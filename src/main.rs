use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

type MsgId = u64;

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageFields {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Generate,
    GenerateOk {
        id: MsgId,
    },
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

#[derive(Debug, Deserialize, Serialize)]
pub struct MessageBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<MsgId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<MsgId>,
    #[serde(flatten)]
    fields: MessageFields,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Message {
    src: String,
    dest: String,
    body: MessageBody,
}

pub struct StateMachine {
    id: MsgId,
    seen_msgs: HashSet<MsgId>,
    topology: HashMap<String, Vec<String>>,
}

pub fn make_message(src: String, dest: String, body: MessageBody) -> Message {
    Message { src, dest, body }
}

pub fn send_message(pipe: &mut impl std::io::Write, msg: Message) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *pipe, &msg).context("Failed to serialize response to stdout")?;
    pipe.write_all(b"\n").context("Failed to write newline")?;
    Ok(())
}

impl StateMachine {
    pub fn build_response(
        &mut self,
        pipe: &mut impl std::io::Write,
        msg: Message,
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
            MessageFields::Echo { echo } => Some(make_message(
                msg.dest,
                msg.src,
                MessageBody {
                    msg_id: self.id.into(),
                    in_reply_to: msg.body.msg_id,
                    fields: MessageFields::EchoOk { echo: echo.clone() },
                },
            )),
            MessageFields::Generate => Some(make_message(
                msg.dest,
                msg.src,
                MessageBody {
                    msg_id: self.id.into(),
                    in_reply_to: msg.body.msg_id,
                    fields: MessageFields::GenerateOk {
                        id: SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_secs()
                            + rand::random::<u64>(),
                    },
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
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let mut state_machine = StateMachine {
        id: 0,
        seen_msgs: HashSet::new(),
        topology: HashMap::new(),
    };

    serde_json::Deserializer::from_reader(stdin)
        .into_iter::<Message>()
        .try_for_each(|msg| -> anyhow::Result<()> {
            let msg = msg.context("Failed to deserialize message from stdin")?;
            state_machine.build_response(&mut stdout, msg)?;
            Ok(())
        })?;

    Ok(())
}
