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

pub fn make_message<T>(src: String, dest: String, body: MessageBody<T>) -> Message<T> {
    Message { src, dest, body }
}

pub fn send_message<T: Serialize>(
    pipe: &mut impl std::io::Write,
    msg: Message<T>,
) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *pipe, &msg).context("Failed to serialize response to stdout")?;
    pipe.write_all(b"\n").context("Failed to write newline")?;
    Ok(())
}

pub fn run<T>(
    mut handle: impl FnMut(&mut std::io::StdoutLock<'static>, Message<T>) -> anyhow::Result<()>,
) -> anyhow::Result<()>
where
    T: DeserializeOwned,
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
