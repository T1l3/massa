// Copyright (c) 2022 MASSA LABS <info@massa.net>

use super::{
    binders::{ReadBinder, WriteBinder},
    messages::Message,
};
use crossbeam_channel::{bounded, select, tick, Receiver, SendTimeoutError, Sender};
use itertools::Itertools;
use massa_logging::massa_trace;
use massa_models::{node::NodeId, wrapped::Id};
use massa_network_exports::{
    ConnectionClosureReason, NetworkConfig, NetworkError, NodeCommand, NodeEvent, NodeEventType,
};
use massa_time::MassaTime;
use tokio::runtime::Handle;
use tokio::{sync::mpsc, task, time::timeout};
use tracing::{debug, trace, warn};

/// Manages connections
/// One worker per node.
pub struct NodeWorker {
    /// Protocol configuration.
    cfg: NetworkConfig,
    /// Node id associated to that worker.
    node_id: NodeId,
    /// Reader for incoming data.
    socket_reader: ReadBinder,
    /// Optional writer to send data.
    socket_writer_opt: Option<WriteBinder>,
    /// Channel to send node commands.
    node_command_tx: Sender<NodeCommand>,
    /// Channel to receive node commands.
    node_command_rx: Receiver<NodeCommand>,
    /// Channel to send node events.
    node_event_tx: Sender<NodeEvent>,
    /// Handle to the networking runtime.
    /// TODO: move to read/write binders.
    runtime_handle: Handle,
}

impl NodeWorker {
    /// Creates a new node worker
    ///
    /// # Arguments
    /// * `cfg`: Network configuration.
    /// * `node_id`: Node id associated to that worker.
    /// * `socket_reader`: Reader for incoming data.
    /// * `socket_writer`: Writer for sending data.
    /// * `node_command_rx`: Channel to receive node commands.
    /// * `node_event_tx`: Channel to send node events.
    /// * `storage`: Shared storage.
    pub fn new(
        cfg: NetworkConfig,
        node_id: NodeId,
        socket_reader: ReadBinder,
        socket_writer: WriteBinder,
        node_command_tx: Sender<NodeCommand>,
        node_command_rx: Receiver<NodeCommand>,
        node_event_tx: Sender<NodeEvent>,
        runtime_handle: Handle,
    ) -> NodeWorker {
        NodeWorker {
            cfg,
            node_id,
            socket_reader,
            socket_writer_opt: Some(socket_writer),
            node_command_tx,
            node_command_rx,
            node_event_tx,
            runtime_handle,
        }
    }

    /// Send a node event upstream.
    fn send_node_event(&self, event: NodeEvent) {
        let result = self
            .node_event_tx
            .send_timeout(event, self.cfg.max_send_wait_node_event.to_duration());
        match result {
            Ok(()) => {}
            Err(SendTimeoutError::Disconnected(event)) => {
                warn!(
                    "Failed to send NodeEvent due to channel closure: {:?}.",
                    event
                );
            }
            Err(SendTimeoutError::Timeout(event)) => {
                warn!("Failed to send NodeEvent due to timeout: {:?}.", event);
            }
        }
    }

    /// Handle an incoming message from the node.
    fn handle_message(mut self, msg: Message) -> Result<(), NetworkError> {
        match msg {
            Message::BlockHeader(header) => {
                massa_trace!(
                    "node_worker.run_loop. receive Message::BlockHeader",
                    {"block_id": header.id.get_hash(), "header": header, "node": self.node_id}
                );
                let event = NodeEvent(self.node_id, NodeEventType::ReceivedBlockHeader(header));
                self.send_node_event(event);
            }
            Message::AskForBlocks(list) => {
                massa_trace!("node_worker.run_loop. receive Message::AskForBlocks", {"hashlist": list, "node": self.node_id});
                let event = NodeEvent(self.node_id, NodeEventType::ReceivedAskForBlocks(list));
                self.send_node_event(event);
            }
            Message::ReplyForBlocks(list) => {
                massa_trace!("node_worker.run_loop. receive Message::AskForBlocks", {"hashlist": list, "node": self.node_id});
                let event = NodeEvent(self.node_id, NodeEventType::ReceivedReplyForBlocks(list));
                self.send_node_event(event);
            }
            Message::PeerList(pl) => {
                massa_trace!("node_worker.run_loop. receive Message::PeerList", {"peerlist": pl, "node": self.node_id});
                let event = NodeEvent(self.node_id, NodeEventType::ReceivedPeerList(pl));
                self.send_node_event(event);
            }
            Message::AskPeerList => {
                let event = NodeEvent(self.node_id, NodeEventType::AskedPeerList);
                self.send_node_event(event);
            }
            Message::Operations(operations) => {
                massa_trace!(
                    "node_worker.run_loop. receive Message::Operations: ",
                    {"node": self.node_id, "operations": operations}
                );
                //massa_trace!("node_worker.run_loop. receive Message::Operations", {"node": self.node_id, "operations": operations});
                let event = NodeEvent(self.node_id, NodeEventType::ReceivedOperations(operations));
                self.send_node_event(event);
            }
            Message::AskForOperations(operation_prefix_ids) => {
                massa_trace!(
                    "node_worker.run_loop. receive Message::AskForOperations: ",
                    {"node": self.node_id, "operation_ids": operation_prefix_ids}
                );
                //massa_trace!("node_worker.run_loop. receive Message::AskForOperations", {"node": self.node_id, "operations": operation_ids});
                let event = NodeEvent(
                    self.node_id,
                    NodeEventType::ReceivedAskForOperations(operation_prefix_ids),
                );
                self.send_node_event(event);
            }
            Message::OperationsAnnouncement(operation_prefix_ids) => {
                massa_trace!("node_worker.run_loop. receive Message::OperationsBatch", {"node": self.node_id, "operation_prefix_ids": operation_prefix_ids});
                let event = NodeEvent(
                    self.node_id,
                    NodeEventType::ReceivedOperationAnnouncements(operation_prefix_ids),
                );
                self.send_node_event(event);
            }
            Message::Endorsements(endorsements) => {
                massa_trace!("node_worker.run_loop. receive Message::Endorsement", {"node": self.node_id, "endorsements": endorsements});
                let event = NodeEvent(
                    self.node_id,
                    NodeEventType::ReceivedEndorsements(endorsements),
                );
                self.send_node_event(event);
            }
            _ => {
                // TODO: Write a more user-friendly warning/logout after several consecutive fails? see #1082
                massa_trace!(
                    "node_worker.run_loop.self.socket_reader.next(). Unexpected message Warning",
                    {}
                );
            }
        }
        Ok(())
    }

    /// node event loop. Consumes self.
    pub fn run_loop(mut self) -> Result<ConnectionClosureReason, NetworkError> {
        let mut socket_writer = self.socket_writer_opt.take().ok_or_else(|| {
            NetworkError::GeneralProtocolError(
                "NodeWorker call run_loop more than once".to_string(),
            )
        })?;

        let (message_tx, message_rx) = bounded::<Message>(self.cfg.node_command_channel_size); // TODO: config
        let node_reader_handle = self.runtime_handle.spawn(async move {
            node_reader_handle(
                &mut self.socket_reader,
                &mut self.node_event_tx,
                self.node_id,
                self.cfg.max_send_wait_node_event,
                message_tx,
            )
            .await
        });
        tokio::pin!(node_reader_handle);
        let mut reader_joined = false;

        let mut ask_peer_list_interval = tick(self.cfg.ask_peer_list_interval.to_duration());
        let mut exit_reason = ConnectionClosureReason::Normal;
        let mut _exit_reason_reader = ConnectionClosureReason::Normal;

        'select_loop: loop {
            /*
                select! without the "biased" modifier will randomly select the 1st branch to check,
                then will check the next ones in the order they are written.
                We choose this order:
                    * node_writer_handle (rare) to immediately register a stop and avoid wasting resources
                    * incoming socket data (high frequency): forward incoming data in priority to avoid contention
                    * node commands (high frequency): try to send, fail on contention
                    * ask peers: low frequency, non-critical
            */
            select! {
                recv(self.node_command_rx) -> cmd => {
                    node_writer_handle(
                        &mut socket_writer,
                        cmd.ok(),
                        self.cfg.message_timeout,
                        self.node_id,
                        self.cfg.max_ask_blocks,
                        self.cfg.max_operations_per_message,
                        self.cfg.max_endorsements_per_message,
                        &self.runtime_handle,
                    );
                }
                recv(ask_peer_list_interval) -> _ => {
                    debug!("timer-based asking node_id={} for peer list", self.node_id);
                    massa_trace!("node_worker.run_loop. timer_ask_peer_list", {"node_id": self.node_id});
                    massa_trace!("node_worker.run_loop.select.timer send Message::AskPeerList", {"node": self.node_id});
                    if let Err(e) = self.node_command_tx.send(NodeCommand::AskPeerList) {
                        debug!("Node worker {}: unable to send ask peer list: {}", self.node_id, e);
                        break 'select_loop;
                    }

                    trace!("after sending Message::AskPeerList from writer_command_tx in node_worker run_loop");
                }
            }
        }

        // 3- Stop node_reader_handle
        if !reader_joined {
            // Abort the task otherwise socket_reader.next() is stuck, waiting for some data to read
            debug!(
                "node_worker.run_loop.cleanup.node_reader_handle.abort, node_id: {}",
                self.node_id
            );
            node_reader_handle.abort();
        }

        Ok(exit_reason)
    }
}

/// Handle incoming node command, convert to message(s) and write that to socket
fn node_writer_handle(
    socket_writer: &mut WriteBinder,
    node_command: Option<NodeCommand>,
    write_timeout: MassaTime,
    node_id: NodeId,
    max_ask_blocks: u32,
    max_operations_per_message: u32,
    max_endorsements_per_message: u32,
    runtime_handle: &Handle,
) -> ConnectionClosureReason {
    let mut exit_reason = ConnectionClosureReason::Normal;
    let messages_: Option<Vec<Message>> = match node_command {
        Some(NodeCommand::Close(r)) => {
            exit_reason = r;
            None
        }
        Some(NodeCommand::SendPeerList(ip_vec)) => {
            massa_trace!("node_worker.run_loop. send Message::PeerList", {"peerlist": ip_vec, "node": node_id});
            Some(vec![Message::PeerList(ip_vec)])
        }
        Some(NodeCommand::SendBlockHeader(header)) => {
            massa_trace!("node_worker.run_loop. send Message::BlockHeader", {"hash": header.id, "node": node_id});
            Some(vec![Message::BlockHeader(header)])
        }
        Some(NodeCommand::AskForBlocks(list)) => {
            // cut hash list on sub list if exceed max_ask_blocks_per_message
            massa_trace!("node_worker.run_loop. send Message::AskForBlocks", {"hashlist": list, "node": node_id});
            let messages = list
                .chunks(max_ask_blocks as usize)
                .map(|to_send| Message::AskForBlocks(to_send.to_vec()))
                .collect();
            Some(messages)
        }
        Some(NodeCommand::ReplyForBlocks(list)) => {
            // cut hash list on sub list if exceed max_ask_blocks_per_message
            massa_trace!("node_worker.run_loop. send Message::ReplyForBlocks", {"hashlist": list, "node": node_id});
            let messages = list
                .chunks(max_ask_blocks as usize)
                .map(|to_send| Message::ReplyForBlocks(to_send.to_vec()))
                .collect();
            Some(messages)
        }
        Some(NodeCommand::SendOperations(operations)) => {
            massa_trace!("node_worker.run_loop. send Message::SendOperations", {"node": node_id, "operations": operations});
            let messages = operations
                .chunks(max_operations_per_message as usize)
                .map(|to_send| Message::Operations(to_send.to_vec()))
                .collect();
            Some(messages)
        }
        Some(NodeCommand::SendOperationAnnouncements(operation_prefix_ids)) => {
            massa_trace!("node_worker.run_loop. send Message::OperationsAnnouncement", {"node": node_id, "operation_ids": operation_prefix_ids});
            let messages = operation_prefix_ids
                .into_iter()
                .chunks(max_operations_per_message as usize)
                .into_iter()
                .map(|chunk| chunk.collect())
                .map(Message::OperationsAnnouncement)
                .collect();
            Some(messages)
        }
        Some(NodeCommand::AskForOperations(operation_prefix_ids)) => {
            massa_trace!(
                "node_worker.run_loop. send Message::AskForOperations",
                {"node": node_id, "operation_ids": operation_prefix_ids}
            );
            let messages = operation_prefix_ids
                .into_iter()
                .chunks(max_operations_per_message as usize)
                .into_iter()
                .map(|chunk| chunk.collect())
                .map(Message::AskForOperations)
                .collect();
            Some(messages)
        }
        Some(NodeCommand::SendEndorsements(endorsements)) => {
            massa_trace!("node_worker.run_loop. send Message::SendEndorsements", {"node": node_id, "endorsements": endorsements});
            // cut endorsement list if it exceed max_endorsements_per_message
            let messages = endorsements
                .chunks(max_endorsements_per_message as usize)
                .map(|endos| Message::Endorsements(endos.to_vec()))
                .collect();
            Some(messages)
        }
        Some(NodeCommand::AskPeerList) => Some(vec![Message::AskPeerList]),
        None => {
            // Note: this should never happen,
            // since it implies the network worker dropped its node command sender
            // before having shut-down the node and joined on its handle.
            return ConnectionClosureReason::Failed;
        }
    };

    if messages_.is_none() {
        return exit_reason;
    }
    // safe to unwrap here
    let messages = messages_.unwrap();

    for msg in messages.iter() {
        let res = runtime_handle.block_on(async {
            timeout(write_timeout.to_duration(), socket_writer.send(msg)).await
        });
        match res {
            Err(err) => {
                massa_trace!("node_worker.run_loop.loop.writer_command_rx.recv.send.timeout", {
                    "node": node_id,
                });
                debug!("Node data writing timed out: {}", err);
                return ConnectionClosureReason::Failed;
            }
            Ok(Err(err)) => {
                massa_trace!("node_worker.run_loop.loop.writer_command_rx.recv.send.error", {
                    "node": node_id, "err":  format!("{}", err),
                });
                debug!("Node data writing error: {:?}", err);
                return ConnectionClosureReason::Failed;
            }
            Ok(Ok(id)) => {
                massa_trace!("node_worker.run_loop.loop.writer_command_rx.recv.send.ok", {
                                "node": node_id, "msg_id": id});
            }
        }
    }

    exit_reason
}

/// Handle socket read function until a message is received then send it
// via 'node_event_tx' queue
async fn node_reader_handle(
    socket_reader: &mut ReadBinder,
    node_event_tx: &mut Sender<NodeEvent>,
    node_id: NodeId,
    max_send_wait: MassaTime,
    message_chan: Sender<Message>,
) -> ConnectionClosureReason {
    let mut exit_reason = ConnectionClosureReason::Normal;

    loop {
        match socket_reader.next().await {
            Ok(Some((index, msg))) => {
                massa_trace!("node_worker.run_loop. receive self.socket_reader.next()", {
                    "index": index
                });
                let chan_clone = message_chan.clone();
                Handle::current()
                    .spawn_blocking(move || {
                        chan_clone
                            .send(msg)
                            .expect("Failed to send node message to node worker.");
                    })
                    .await
                    .expect("Failed to run task to send node message to node worker.");
            }
            Ok(None) => {
                massa_trace!(
                    "node_worker.run_loop.self.socket_reader.next(). Ok(None) Error",
                    {}
                );
                break;
            } // peer closed cleanly
            Err(err) => {
                // stream error
                debug!(
                    "node_worker.run_loop.self.socket_reader.next(). receive error: {}",
                    err
                );
                exit_reason = ConnectionClosureReason::Failed;
                break;
            }
        }
    }

    exit_reason
}
