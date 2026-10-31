use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
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
    seen_msgs: Vec<MsgId>,
    topology: HashMap<String, Vec<String>>,
}

impl StateMachine {
    pub fn step(&mut self, msg: Message) -> Message {
        self.id += 1;

        match &msg.body.fields {
            MessageFields::Init {
                node_id: _,
                node_ids: _,
            } => Message {
                src: msg.dest,
                dest: msg.src,
                body: MessageBody {
                    msg_id: None,
                    in_reply_to: msg.body.msg_id,
                    fields: MessageFields::InitOk,
                },
            },
            MessageFields::Echo { echo } => Message {
                src: msg.dest,
                dest: msg.src,
                body: MessageBody {
                    msg_id: self.id.into(),
                    in_reply_to: msg.body.msg_id,
                    fields: MessageFields::EchoOk { echo: echo.clone() },
                },
            },
            MessageFields::Generate => Message {
                src: msg.dest,
                dest: msg.src,
                body: MessageBody {
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
            },
            MessageFields::Broadcast { message } => {
                self.seen_msgs.push(*message);
                Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: MessageBody {
                        msg_id: self.id.into(),
                        in_reply_to: msg.body.msg_id,
                        fields: MessageFields::BroadcastOk,
                    },
                }
            }
            MessageFields::Read => Message {
                src: msg.dest,
                dest: msg.src,
                body: MessageBody {
                    msg_id: self.id.into(),
                    in_reply_to: msg.body.msg_id,
                    fields: MessageFields::ReadOk {
                        messages: self.seen_msgs.clone(),
                    },
                },
            },
            MessageFields::Topology { topology } => {
                self.topology = topology.to_owned();
                Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: MessageBody {
                        msg_id: self.id.into(),
                        in_reply_to: msg.body.msg_id,
                        fields: MessageFields::TopologyOk,
                    },
                }
            }
            _ => panic!("Received response message as request!!!"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let mut state_machine = StateMachine {
        id: 0,
        seen_msgs: Vec::new(),
        topology: HashMap::new(),
    };

    serde_json::Deserializer::from_reader(stdin)
        .into_iter::<Message>()
        .try_for_each(|msg| -> anyhow::Result<()> {
            let msg = msg.context("Maelstrom input from STDIN could not be deserialized")?;
            serde_json::to_writer(&mut stdout, &state_machine.step(msg))
                .context("Failed to serialize response message")?;
            stdout.write_all(b"\n").context("Failed to write newline")?;
            Ok(())
        })?;

    Ok(())
}
