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
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}

pub struct EchoNode {
    id: MsgId,
}

impl EchoNode {
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
            MessageFields::Echo { echo } => Some(Message::new(
                msg.dest,
                msg.src,
                MessageBody {
                    msg_id: self.id.into(),
                    in_reply_to: msg.body.msg_id,
                    fields: MessageFields::EchoOk { echo: echo.clone() },
                },
            )),
            _ => None,
        }
    }
}

fn main() -> anyhow::Result<()> {
    let mut state_machine = EchoNode { id: 0 };
    run(|stdout, msg| send_messages(stdout, state_machine.respond(msg)))
}
