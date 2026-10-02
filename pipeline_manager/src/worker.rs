use config_analyzer_core::{At, SpanSink};
use tokio::sync::{mpsc, oneshot};

use crate::instance::{Pending, PipelineState, Update};

#[derive(Debug)]
pub(crate) enum WorkerMsg {
    Update(Update),
    // end: Some on End
    Barrier {
        end: Option<At>,
        reply: oneshot::Sender<Vec<Pending>>,
    },
}

pub(crate) async fn run<K>(mut state: PipelineState<K>, mut rx: mpsc::Receiver<WorkerMsg>)
where
    K: SpanSink,
{
    while let Some(msg) = rx.recv().await {
        match msg {
            WorkerMsg::Update(update) => state.apply(update),
            WorkerMsg::Barrier { end, reply } => {
                if let Some(at) = end {
                    state.end(at);
                }
                drop(reply.send(state.pending()));
                if end.is_some() {
                    break;
                }
            }
        }
    }
}
