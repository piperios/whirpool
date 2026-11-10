use anyhow::Context;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub type MsgId = u64;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MessageBody<T> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<MsgId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<MsgId>,
    #[serde(flatten)]
    pub fields: T,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message<T> {
    pub src: String,
    pub dest: String,
    pub body: MessageBody<T>,
}

impl<T> Message<T> {
    pub fn new(src: String, dest: String, body: MessageBody<T>) -> Self {
        Message { src, dest, body }
    }
}

pub fn send_message<T, W>(pipe: &mut W, msg: Message<T>) -> anyhow::Result<()>
where
    T: Serialize,
    W: std::io::Write,
{
    serde_json::to_writer(&mut *pipe, &msg).context("Failed to serialize message to stdout")?;
    pipe.write_all(b"\n").context("Failed to write newline")?;
    Ok(())
}

pub fn send_messages<T, W, M>(pipe: &mut W, msgs: M) -> anyhow::Result<()>
where
    T: Serialize,
    W: std::io::Write,
    M: IntoIterator<Item = Message<T>>,
{
    for msg in msgs {
        send_message(pipe, msg)?;
    }
    Ok(())
}

pub fn run<T, F>(mut handle: F) -> anyhow::Result<()>
where
    T: DeserializeOwned,
    F: FnMut(&mut std::io::StdoutLock<'static>, Message<T>) -> anyhow::Result<()>,
{
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    serde_json::Deserializer::from_reader(stdin)
        .into_iter::<Message<T>>()
        .try_for_each(|msg| -> anyhow::Result<()> {
            let msg = msg.context("Failed to deserialize message from stdin")?;
            handle(&mut stdout, msg)
        })
}
