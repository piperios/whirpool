use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use whirpool::{Message, MessageBody, MsgId, make_message, run, send_message};

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
            _ => None,
        } {
            send_message(&mut *pipe, msg)?;
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut state_machine = UniqueIdsNode { id: 0 };
    run(|stdout, msg| state_machine.build_response(stdout, msg))
}
