use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use whirpool::{Message, MessageBody, MsgId, run, send_messages};

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageFields {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Generate,
    GenerateOk {
        id: MsgId,
    },
}

pub struct UniqueIdsNode {
    id: MsgId,
}

impl UniqueIdsNode {
    pub fn respond(&mut self, msg: Message<MessageFields>) -> Option<Message<MessageFields>> {
        self.id += 1;

        match &msg.body.fields {
            MessageFields::Init {
                node_id: _,
                node_ids: _,
            } => Some(Message::new(
                msg.dest,
                msg.src,
                MessageBody {
                    msg_id: None,
                    in_reply_to: msg.body.msg_id,
                    fields: MessageFields::InitOk,
                },
            )),
            MessageFields::Generate => Some(Message::new(
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
            _ => None,
        }
    }
}

fn main() -> anyhow::Result<()> {
    let mut state_machine = UniqueIdsNode { id: 0 };
    run(|stdout, msg| send_messages(stdout, state_machine.respond(msg)))
}
