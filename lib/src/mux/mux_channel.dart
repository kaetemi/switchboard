/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Fable 5.1 <noreply@anthropic.com>
  Claude Opus 5.5 <noreply@anthropic.com>
  GPT-6 Astra <noreply@anthropic.com>
  GPT-6 Astra <noreply@openai.com>
*/

import 'dart:async';
import 'dart:typed_data';

import 'package:meta/meta.dart';
import 'package:stream_channel/stream_channel.dart';

import '../status.dart';
import '../status_closable.dart';
import 'frame_mux_channel.dart';
import 'mux_connection.dart';

/// State of a channel as seen by the local side.
///
/// See the wiki page "Polyverse Switchboard Mux", section "State summary per
/// channel". There is no "half closed remote" state: receiving the peer's
/// close on an open channel sends the confirming close at once and moves
/// to [closed].
enum MuxChannelState {
  /// Both sides may send.
  open,

  /// We sent our close and wait for the peer's. What the peer sent before
  /// it saw ours is still delivered.
  halfClosedLocal,

  /// Mutually closed, the connection ended, or the peer did not confirm
  /// our close within [MuxOptions.closeConfirmTimeout].
  closed,
}

/// Output tier of what a side sends on a channel: which of the subframes
/// waiting for the transport goes first. A sender writes the connection's
/// control messages first, then the ordinary channels, then the bulk ones.
/// Local to the sender; nothing about it goes on the wire. The frame mux's
/// output scheduler orders its frames by it; a stream binding maps it onto
/// its library's stream priorities.
///
/// See the wiki page "Polyverse Switchboard Mux", section "Output
/// scheduling".
enum MuxPriority {
  /// Ordinary traffic: requests, responses, messages. Goes before bulk.
  normal,

  /// A bulk transfer: its subframes go after ordinary ones. On the frame
  /// mux they get a turn after [MuxOptions.bulkZipper] ordinary frames
  /// while they wait, so sustained ordinary traffic cannot starve them.
  /// [MuxChannel.send]
  /// splits a payload on such a channel into chunks of at most
  /// [MuxOptions.bulkChunkSize].
  bulk,
}

/// A channel that carries its subframes on a [MuxChannel] it may replace
/// before anything was sent or received on it (a slot channel retried
/// after `MOVED`), and passes flow control on to it: what a layer reading
/// it on the application's behalf (Talk) needs of the mux channel under
/// it. A [MuxChannel] is its own carrier and does not implement this.
@internal
abstract interface class MuxChannelCarrier {
  /// The mux channel the subframes travel on now.
  MuxChannel get channel;

  /// As [MuxChannel.manualCredit], for the current channel and a
  /// replacement.
  set manualCredit(bool value);

  /// As [MuxChannel.consumed], on the current channel.
  void consumed(int bytes, {int subframes = 1});

  /// As [MuxChannel.send]: the future completes when the subframe was
  /// handed to the connection, on the current channel or on the
  /// replacement it waits for, so that a layer forwarding what it reads
  /// can return the credit of what it forwards once it went out.
  Future<void> send(Uint8List subframe);

  /// As [MuxChannel.isWritable], for the current channel; false while a
  /// replacement is awaited.
  bool get isWritable;

  /// As [MuxChannel.whenWritable], for the current channel, or the
  /// replacement it waits for.
  Future<void> get whenWritable;
}

/// One channel of a [MuxConnection]: an ordered, flow-controlled sequence
/// of subframes in each direction, opened by either side with an open
/// payload and closed with a status.
///
/// A [StreamChannel] of subframes: [stream] yields what the peer sends,
/// and [sink] (or [send]) sends. Channels come from [MuxConnection.open],
/// [MuxConnection.incoming] and [openAfter]; applications never construct
/// them. Unlike the general [StreamChannel] guarantee, closing the [sink]
/// does not end [stream] at once: what the peer sent before it saw this
/// side's close is still delivered, and [stream] ends when the peer's
/// close arrives or the connection ends.
///
/// ## The binding contract
///
/// This class is the contract of a channel, whatever carries it: the frame
/// mux ([FrameMuxChannel], frames multiplexed over one ordered byte
/// stream), or a binding that gives each channel a stream of the transport
/// (QUIC, WebTransport), which extends this class. Every binding honours:
///
/// * Identity of a channel: a `u48` channel id ([id]), chosen by the side
///   that opens it (even for the initiator, odd for the acceptor, never a
///   reserved id) and known to both, and an open payload ([openPayload]),
///   given by the opener and dispatched on by the receiver.
/// * Subframes: each side sends subframes, byte strings, possibly empty,
///   delivered whole and in order, each as one event of the peer's
///   [stream] (Talk puts one Talk frame in each; on a [MuxPriority.bulk]
///   channel, a payload [send] takes may arrive as several). Nothing is
///   ordered across channels, nor between a channel and the connection's
///   control messages (`IDENT` included); see [openAfter] for what it
///   promises and what it does not.
/// * Close: each side closes the channel once, with a status ([close]),
///   after everything it handed to [send] before; the channel ends when
///   both sides have closed it. On the peer's close this side closes in
///   turn at once (unless it closed first), drops what it has not sent yet
///   (the futures of those sends fail), and ends [stream] once what arrived
///   before is delivered. A receiver rejects a channel by closing it.
///   [done] completes with the end status: the first non-OK status a side
///   saw or sent, else OK; when the connection ends first, its status as
///   [MuxConnection.done] describes (`GOING_AWAY` or `CONNECTION_LOST`). A
///   status with an application code (256 and above) is never sent: a
///   received one reads as [StatusCode.unknown] with the code in the
///   reason, so that it can be relayed with [close]. A close need not
///   carry a reason: a stream binding may close by a reset that carries
///   the status code only.
/// * Flow control: what a receiver holds for a channel is bounded by what
///   it granted, and it grants more as it consumes (as the [stream]'s
///   listener takes subframes, or as [consumed] reports them under
///   [manualCredit]; [grant] grants more at once), so that a reader that
///   stops stalls its own channel and nothing else. [send] takes
///   [sendCost] of the grant per subframe, waiting in order while it
///   lacks room; [isWritable] and [whenWritable] tell a sender when it has
///   room.
/// * Identity: the channel belongs to its [connection], whose
///   [MuxConnection.peerIdentity] is the peer's.
///
/// See `doc/dart-design.md`, section "Binding seam".
abstract class MuxChannel
    with StreamChannelMixin<Uint8List>
    implements StatusClosable {
  /// For bindings: the channel class of a binding extends this one.
  MuxChannel();

  /// The connection this channel belongs to.
  MuxConnection get connection;

  /// The channel id: a `u48` chosen by the side that opened the channel
  /// (even for the initiator, odd for the acceptor, never one of the
  /// frame mux's reserved ids), the same at both ends, so that either side
  /// can name the channel to the other (as a Talk bulk channel's `_bulk`
  /// open payload names its parent), and unique among the connection's
  /// channels until they end (see [MuxConnection.channelWithId]). Every
  /// binding carries it: the frame mux in every frame, a stream binding in
  /// the channel's open record (a stream id is not visible to every
  /// endpoint, a browser's among them), where an id is used once and
  /// never reused.
  int get id;

  /// Whether the local side opened this channel.
  bool get isLocallyOpened;

  /// The open payload, passed through unchanged.
  Uint8List get openPayload;

  /// Current state.
  MuxChannelState get state;

  /// Whether subframes may be sent: [state] is [MuxChannelState.open].
  bool get canSend;

  /// The output tier of what this side sends on the channel; see
  /// [MuxPriority]. [MuxPriority.normal] by default. Either side sets it
  /// for its own direction, at any time: it applies to what is sent from
  /// then on, and to the splitting of payloads [send] takes from then on.
  MuxPriority get priority;

  set priority(MuxPriority value);

  /// Bytes received and not yet delivered to the [stream]'s listener
  /// (nobody listens yet, or the subscription is paused), at their
  /// flow-control cost. Bounded by the window this side granted.
  int get bufferedBytes;

  /// Incoming subframes, single subscription, buffered until listened to.
  /// Ends when the peer's close arrives or the connection ends; read
  /// [done] for the reason.
  ///
  /// Without [manualCredit] a subframe counts as consumed once the
  /// listener has taken it: delivered, not merely buffered, so a paused
  /// subscription (or none yet) consumes nothing and stalls the channel,
  /// and only it, once the window is used up. Cancelling the subscription
  /// drops further subframes, which count as consumed; it does not close
  /// the channel.
  @override
  Stream<Uint8List> get stream;

  /// Whether [stream] has a listener: something reads the channel.
  bool get hasListener;

  /// Bytes of flow-control cost the peer may still send on the channel
  /// before this side returns credit; sending beyond it is a protocol
  /// error of the peer.
  int get receiveWindow;

  /// Whether a layer that reads the channel on the application's behalf
  /// (Talk, a proxy) reports consumption itself with [consumed]; false by
  /// default.
  ///
  /// While true, a subframe the [stream]'s listener takes does not count
  /// as consumed: it stays held ([heldBytes]) until [consumed] reports it,
  /// and its credit returns to the peer only then. Set it before
  /// listening. Setting it back to false counts what is held as consumed.
  /// What is held is bounded by the window.
  bool get manualCredit;

  set manualCredit(bool value);

  /// With [manualCredit]: the cost of the subframes [stream] delivered
  /// that [consumed] has not reported yet. 0 once the channel is closed.
  int get heldBytes;

  /// With [manualCredit]: reports that [subframes] subframes delivered by
  /// [stream], [bytes] bytes long together, have been consumed, so that
  /// their credit can return to the peer. For one subframe:
  /// `consumed(subframe.length)`. The binding knows what they cost.
  ///
  /// Does nothing once the channel is closed. Throws [ArgumentError] for
  /// negative values and [StateError] when more is reported than was
  /// delivered and not yet reported (always the case without
  /// [manualCredit], where delivery is consumption).
  void consumed(int bytes, {int subframes = 1});

  /// Raises the window the peer may use on this channel by [bytes] at once,
  /// for a transfer this side expects to be large; what this side may have
  /// to hold for the channel grows by as much.
  ///
  /// Does nothing once this side can no longer receive on the channel.
  /// Throws [ArgumentError] if [bytes] is not positive, or if the window
  /// would exceed what the binding can grant.
  void grant(int bytes);

  /// Sink for outgoing subframes.
  ///
  /// `add` sends (silently dropped once the channel can no longer send),
  /// `close` is [close] with [Status.ok] and `done` completes when the
  /// channel is closed. `addError` closes the channel with the error's
  /// status if it is a [SwitchboardException], else with
  /// [StatusCode.internal].
  @override
  StreamSink<Uint8List> get sink;

  /// Completes with the channel's end status once it is closed: see the
  /// class documentation, section "Close". Never completes with an error.
  @override
  Future<Status> get done;

  /// Bytes of flow-control cost this side may still send on the channel
  /// before the peer returns credit, leaving out what [send] has reserved
  /// for subframes not written yet.
  int get sendWindow;

  /// The flow-control cost of sending a subframe of non-negative [length]
  /// bytes: what
  /// [send] takes of [sendWindow] for it, and of the peer's window. The
  /// frame mux charges the length plus 16 (a DATA frame's cost); a binding
  /// whose window counts records charges [length] plus its record overhead.
  /// The cost must be positive even for an empty subframe: otherwise a
  /// peer could make a stopped reader buffer unbounded empty records.
  /// A layer that bounds what it holds in the channel's terms
  /// (a slot channel holding subframes while it reopens) counts with it.
  int sendCost(int length);

  /// The largest subframe the peer accepts, in bytes: what [send] takes
  /// on this channel in one piece, or a negative number when not even an
  /// empty one fits. Talk sends a larger message as a bulk payload.
  /// An accepted ordinary subframe must be able to make progress under
  /// the binding's credit-return policy when the receiver keeps consuming;
  /// a record limit larger than the available receive capacity is not
  /// by itself a safe value for this member.
  ///
  /// On the frame mux, half the peer's initial window less the 16 bytes
  /// of a DATA frame's cost (so that a peer returning credit in batches of
  /// half its window never leaves a subframe waiting for ever), within the
  /// frame limit the peer announced; on a stream binding, the peer's record
  /// limit. Larger payloads go on a channel whose [priority] is
  /// [MuxPriority.bulk], where [send] splits them.
  int get maxSubframeLength;

  /// Sends one subframe, which may be empty.
  ///
  /// It takes [sendCost] of [sendWindow]. When the window takes it, and no
  /// earlier subframe is still waiting, it is handed over at once and the
  /// returned future is already complete. Otherwise it waits, in order
  /// behind the subframes before it, until the peer returns enough credit,
  /// and the future completes when it has been handed over. The caller may
  /// reuse [subframe] once the future completes, not before.
  ///
  /// On a channel whose [priority] is [MuxPriority.bulk] when [send] is
  /// called, a non-empty [subframe] of any length is a run of bytes rather
  /// than one subframe: it goes out in chunks sized to the window, which
  /// the receiver sees as several subframes. The future completes when the
  /// last chunk has been handed over.
  ///
  /// The future fails with a [SwitchboardException]
  /// ([StatusCode.failedPrecondition]) if the channel can no longer send
  /// before the subframe goes out: the peer closed it, the connection
  /// ended, or [close]'s confirmation timed out. Such a failure is never
  /// reported as unhandled. A [close] called meanwhile waits for the
  /// subframes before it.
  ///
  /// Throws [SwitchboardException] with [StatusCode.failedPrecondition] if
  /// [canSend] is false, and with [StatusCode.frameTooLarge] if the
  /// subframe is longer than [maxSubframeLength] (on a bulk channel: if
  /// the peer leaves no room for a byte of payload).
  Future<void> send(Uint8List subframe);

  /// Whether a subframe as large as [send] takes on this channel
  /// ([maxSubframeLength]) would be handed over at once: the channel can
  /// send ([canSend]), no subframe of it waits for credit, and
  /// [sendWindow] has room for that subframe's [sendCost].
  ///
  /// Readiness reflects the receiver's window and this channel's own
  /// backlog, not the binding's output buffers: a subframe sent while
  /// writable is handed over at once, and written when the transport takes
  /// it. The future of [send] is the way to follow a subframe; this is for
  /// deciding whether to produce one: a publisher that sends only while
  /// writable never queues in the channel, and a slow receiver stalls it
  /// at the window.
  bool get isWritable;

  /// Completes once the channel [isWritable]: at once when it is, else
  /// when the peer's credit makes room for a subframe of
  /// [maxSubframeLength] and nothing of this channel waits for credit any
  /// more. Fails with a [SwitchboardException]
  /// ([StatusCode.failedPrecondition]) when the channel can no longer
  /// send: at once if [canSend] is false, else when it closes, the peer
  /// closes it, or the connection ends. Never reports an unhandled error.
  Future<void> get whenWritable;

  /// Opens a channel on the same connection, as [MuxConnection.open] does,
  /// in order with this channel's subframes: its open goes out after the
  /// subframes handed to [send] before the call (those waiting for credit
  /// included) and before the ones after it. The new channel's subframes
  /// and close wait for its open.
  ///
  /// This is how Talk opens a bulk channel for a message, its `_bulk` open
  /// payload naming this channel's [id]: the receiver must not get the bulk
  /// channel long before the message, since it bounds the bulk channels
  /// opened ahead of their messages, which an open sent at once could
  /// overtake while the message waits for credit or for the transport.
  /// The frame mux keeps the order on the wire, so the peer receives the
  /// open before the subframes after the call, and opens after the same
  /// parent in call order (channel ids may wrap). A binding that carries
  /// channels on separate streams cannot order streams: it opens the new
  /// stream once what this channel sent before the call is handed to the
  /// transport, and the peer may then receive the new channel before the
  /// subframes sent before the call or after them, after this channel
  /// ended, or out of the order of the ids of the channels opened after
  /// this one. Talk waits for a bulk channel referenced before it arrived
  /// (`TalkOptions.bulkOpenTimeout`); its check that bulk numbers arrive
  /// in increasing order applies only with
  /// [MuxConnection.hasOrderedChannels]. A bulk open arriving before
  /// its parent is held within a bounded backlog until the parent is
  /// dispatched. Once the parent has ended it is refused: Talk's graceful
  /// close waits for its outgoing bulk transfers before closing the parent.
  ///
  /// The new channel continues work the connection admitted already, so a
  /// GOAWAY, sent or received, does not refuse it, and a binding admits a
  /// `_bulk` channel the peer opens for an open channel after its own
  /// GOAWAY likewise: an answer in flight can still carry a bulk payload
  /// while the connection drains.
  ///
  /// If this channel stops sending before the open goes out (the peer
  /// closed it, the connection ended, or this channel's close confirmation
  /// timed out with the open still waiting), the new channel is never
  /// opened: it ends at once with [StatusCode.cancelled]. Throws like
  /// [MuxConnection.open] (except after a GOAWAY), and with
  /// [StatusCode.failedPrecondition] when [canSend] is false.
  MuxChannel openAfter(Uint8List openPayload);

  /// Sends this side's close carrying [status], unless sent already.
  ///
  /// Subframes still waiting for credit ([send]) go first: the close
  /// follows them, so that what was sent before it arrives before it. If
  /// they are still waiting when the confirmation times out
  /// ([MuxOptions.closeConfirmTimeout]), or the peer closes meanwhile, they
  /// are dropped (their futures fail) and the close goes at once.
  ///
  /// The reason may be shortened on the wire, on a UTF-8 character
  /// boundary (the status payload stays within 1024 bytes), or not sent
  /// at all where a stream binding closes by a reset, which carries the
  /// code only; [done] reports [status] as given. The local-only
  /// `CONNECTION_LOST` code is sent as `UNAVAILABLE`.
  ///
  /// Completes when the channel is mutually closed or the connection is
  /// lost, whichever comes first, or when the peer did not confirm within
  /// [MuxOptions.closeConfirmTimeout] (the channel is then closed locally);
  /// at once if the channel is already closed. Never completes with an
  /// error. Throws [ArgumentError] synchronously for application status
  /// codes (256 and above), which must not be used in a close.
  @override
  Future<void> close([Status status = Status.ok]);

  /// Whether what is sent waits for the peer's limits, which a binding
  /// learns after the channel opened (the frame mux's wait for the peer's
  /// LIMITS, [MuxOptions.awaitPeerLimits]): until then [maxSubframeLength]
  /// is provisional. False, the default, for a binding that knows them
  /// from its handshake.
  bool get awaitingLimits => false;

  /// Completes once [awaitingLimits] is false, or the channel ended first.
  Future<void> get limitsKnown => Future<void>.value();
}
