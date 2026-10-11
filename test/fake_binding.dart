/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Opus 5.5 <noreply@anthropic.com>
  GPT-6 Astra <noreply@openai.com>
*/

// A fake binding of the connection contract, for the tests of the seam:
// there is no mux frame anywhere. Each channel is a "stream" whose records
// (its open, its subframes, credit, its close) go to the peer's channel
// object through an in-process wire, in order per stream, the way a QUIC
// stream carries them; the connection's control messages (PING, PONG,
// GOAWAY, NONCE, IDENT) go on a control "stream" of their own, which may
// be slower than the channels ([FakeConnection.pair]'s `controlDelay`), as
// the streams of a QUIC connection are not ordered with each other. Flow
// control counts payload bytes plus one byte per record (empty records
// must be bounded too), the
// windows are the [MuxOptions.initialWindow] of each side, known to the
// other from the start (as QUIC's transport parameters are), and channel
// ids are carried in the open record as the binding contract has it: the
// initiator's even (2, 4, ...), the acceptor's odd (3, 5, ...), each used
// once.
//
// `IDENT` is checked with the core's `verifyIdent`, as a real binding
// would; nothing else of the frame mux is used.

import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:typed_data';

import 'package:switchboard/switchboard.dart';

/// One record on the control stream.
sealed class _Control {}

class _Ping extends _Control {
  _Ping(this.payload);
  final Uint8List payload;
}

class _Pong extends _Control {
  _Pong(this.payload);
  final Uint8List payload;
}

class _GoAway extends _Control {
  _GoAway(this.status);
  final Status status;
}

class _Nonce extends _Control {
  _Nonce(this.nonce);
  final Uint8List nonce;
}

class _Ident extends _Control {
  _Ident(this.payload);
  final Uint8List payload;
}

/// The end of the connection, after everything sent before it.
class _Closed extends _Control {}

/// A connection of the fake binding. Make one with [FakeConnection.pair].
class FakeConnection extends BindingConnection {
  FakeConnection._(
    this.isInitiator,
    this.options,
    this.controlDelay,
    this.channelDelay,
  ) : _nextId = isInitiator ? 2 : 3 {
    _incoming = StreamController<MuxChannel>(
      sync: true,
      onListen: _scheduleIncoming,
      onResume: _scheduleIncoming,
      onCancel: _cancelIncoming,
    );
  }

  /// Two connected sides: the initiator and the acceptor. Control records
  /// take [controlDelay] longer than channel records.
  static (FakeConnection, FakeConnection) pair({
    MuxOptions initiator = const MuxOptions(),
    MuxOptions acceptor = const MuxOptions(),
    Duration controlDelay = Duration.zero,
    Duration Function(int id)? channelDelay,
  }) {
    final a = FakeConnection._(true, initiator, controlDelay, channelDelay);
    final b = FakeConnection._(false, acceptor, controlDelay, channelDelay);
    a._peer = b;
    b._peer = a;
    return (a, b);
  }

  @override
  final bool isInitiator;

  @override
  final MuxOptions options;

  /// How much later than channel records control records arrive.
  final Duration controlDelay;

  /// A fixed delay per channel stream, in both directions. Different
  /// channels may overtake one another; records of one channel cannot.
  final Duration Function(int id)? channelDelay;

  late final FakeConnection _peer;
  late final StreamController<MuxChannel> _incoming;
  final Map<int, FakeChannel> _channels = {};
  final Queue<FakeChannel> _undelivered = Queue<FakeChannel>();
  final Set<FakeChannel> _payloads = {};
  int _openPayloadBytes = 0;
  bool _incomingScheduled = false;
  final Completer<Status> _done = Completer<Status>();
  final Completer<Status> _peerGoAwayStatus = Completer<Status>();
  final List<(Uint8List, Completer<Duration>, Stopwatch)> _pings = [];
  int _nextId;
  int _pingCounter = 0;
  bool _incomingCancelled = false;
  bool _closing = false;
  bool _goAwaySent = false;
  Status? _ownGoAway;
  Status? _peerGoAway;
  Status? _peerGoAwayReceived;
  Status? _closeStatus;
  Completer<void>? _idle;

  // Identity.
  Uint8List? _localNonce;
  Uint8List? _peerNonce;
  Completer<Uint8List>? _nonceWaiter;
  final Completer<void> _identityRequested = Completer<void>();
  PeerIdentity? _peerIdentity;
  final Completer<PeerIdentity> _peerIdentified = Completer<PeerIdentity>();
  Completer<void>? _identityEvent;
  bool _verifying = false;
  final Queue<_Control> _heldControl = Queue<_Control>();

  /// Records of every kind sent so far, by kind: for tests that want to
  /// see what went over the fake wire.
  final Map<String, int> sent = {};

  void _count(String kind) => sent[kind] = (sent[kind] ?? 0) + 1;

  // The wire ----------------------------------------------------------------

  /// Runs [deliver] on the peer's side, in order with earlier records on
  /// channel [id], unless the peer has closed by then.
  void _postData(int id, void Function() deliver) {
    final peer = _peer;
    Timer(channelDelay?.call(id) ?? Duration.zero, () {
      if (!peer._closing) {
        deliver();
      }
    });
  }

  /// Posts [record] on the control stream, [controlDelay] late.
  void _postControl(_Control record) {
    _count(record.runtimeType.toString());
    final peer = _peer;
    Timer(controlDelay, () {
      if (!peer._closing) {
        peer._onControl(record);
      }
    });
  }

  // The contract --------------------------------------------------------------

  @override
  Stream<MuxChannel> get incoming => _incoming.stream;

  @override
  Future<Status> get done => _done.future;

  @override
  bool get isOpen => !_closing && !_goAwaySent;

  @override
  bool get peerGoingAway => _peerGoAway != null;

  @override
  Future<Status> get peerGoAwayStatus => _peerGoAwayStatus.future;

  /// The peer's channel limit, known from the start as QUIC's transport
  /// parameters are.
  @override
  int get peerMaxChannels => _peer.options.maxChannels;

  @override
  int get openChannelCount => _channels.length;

  @override
  int get openPayloadBytes => _openPayloadBytes;

  @override
  MuxChannel? channelWithId(int id) => _channels[id];

  @override
  Iterable<MuxChannel> get channels => List.of(_channels.values);

  @override
  PeerIdentity? get peerIdentity {
    final identity = _peerIdentity;
    return identity == null || identity.isExpired() ? null : identity;
  }

  @override
  Future<PeerIdentity> get peerIdentified => _peerIdentified.future;

  @override
  Future<void> get identityRequested => _identityRequested.future;

  @override
  Future<void> get identityChanged =>
      (_identityEvent ??= Completer<void>()).future;

  @override
  MuxChannel open(Uint8List openPayload) =>
      _openLocal(openPayload, continuation: false).._postOpen();

  FakeChannel _openLocal(Uint8List openPayload, {required bool continuation}) {
    if (_closing || (_goAwaySent && !continuation)) {
      throw SwitchboardException.of(
        StatusCode.failedPrecondition,
        'connection is closing',
      );
    }
    if (_peerGoAway != null && !continuation) {
      throw SwitchboardException.of(
        StatusCode.failedPrecondition,
        'peer is going away',
      );
    }
    final max = peerMaxChannels;
    if (max > 0 && _channels.length >= max) {
      throw SwitchboardException.of(
        StatusCode.resourceExhausted,
        'peer accepts at most $max channels',
      );
    }
    // The shared id vocabulary reserves these values, independent of the
    // binding's encoding. Never wrap or reuse an id on this binding.
    if (_nextId == 0xFFFF || _nextId == 0xFFFF00000000) {
      _nextId += 2;
    }
    if (_nextId >= 0xFFFFFFFFFFFF) {
      throw SwitchboardException.of(
        StatusCode.resourceExhausted,
        'ids used up',
      );
    }
    final id = _nextId;
    _nextId += 2;
    final channel = FakeChannel._(
      this,
      id,
      isLocallyOpened: true,
      openPayload: Uint8List.fromList(openPayload),
      sendWindow: _peer.options.initialWindow,
      receiveWindow: options.initialWindow,
    );
    _channels[id] = channel;
    return channel;
  }

  /// The peer opened [opener].
  void _onOpen(FakeChannel opener) {
    Status? rejection;
    if (_goAwaySent && !_continuesOpenChannel(opener.openPayload)) {
      rejection = Status.of(StatusCode.goingAway);
    } else if (_incomingCancelled) {
      rejection = Status.of(StatusCode.unavailable, 'not accepting channels');
    } else if (options.maxChannels > 0 &&
        (_channels.length >= options.maxChannels ||
            _undelivered.length >= options.maxChannels)) {
      rejection = Status.of(
        StatusCode.resourceExhausted,
        'at most ${options.maxChannels} channels',
      );
    } else if (options.maxOpenPayloadBytes > 0 &&
        _openPayloadBytes + opener.openPayload.length >
            options.maxOpenPayloadBytes) {
      rejection = Status.of(
        StatusCode.resourceExhausted,
        'open payload budget',
      );
    }
    if (rejection != null) {
      final status = rejection;
      _count('close');
      _postData(opener.id, () => opener._onClose(status));
      return;
    }
    final channel = FakeChannel._(
      this,
      opener.id,
      isLocallyOpened: false,
      openPayload: Uint8List.fromList(opener.openPayload),
      sendWindow: _peer.options.initialWindow,
      receiveWindow: options.initialWindow,
    ).._opened = true;
    channel._peerChannel = opener;
    opener._peerChannel = channel;
    _channels[channel.id] = channel;
    _payloads.add(channel);
    _openPayloadBytes += channel.openPayload.length;
    _undelivered.add(channel);
    _scheduleIncoming();
  }

  void _scheduleIncoming() {
    if (_incomingScheduled) return;
    _incomingScheduled = true;
    scheduleMicrotask(() {
      _incomingScheduled = false;
      while (_undelivered.isNotEmpty &&
          _incoming.hasListener &&
          !_incoming.isPaused) {
        final channel = _undelivered.removeFirst();
        if (channel.state == MuxChannelState.closed) _releasePayload(channel);
        _incoming.add(channel);
      }
      if (_closing && _undelivered.isEmpty && !_incoming.isClosed) {
        unawaited(_incoming.close());
      }
    });
  }

  void _cancelIncoming() {
    _incomingCancelled = true;
    final pending = List.of(_undelivered);
    _undelivered.clear();
    for (final channel in pending) {
      _releasePayload(channel);
      unawaited(channel.close(Status.of(StatusCode.unavailable)));
    }
  }

  void _releasePayload(FakeChannel channel) {
    if (_payloads.remove(channel)) {
      _openPayloadBytes -= channel.openPayload.length;
    }
  }

  /// Whether [openPayload] opens a `_bulk` channel for an open channel of
  /// this connection: admitted after this side's GOAWAY.
  bool _continuesOpenChannel(Uint8List openPayload) {
    try {
      final address = ChannelAddress.decode(openPayload);
      if (!TalkBulkOpen.isBulk(address)) {
        return false;
      }
      final parent = _channels[TalkBulkOpen.decode(address).parentId];
      return parent != null && !parent._closeReceived;
    } on ProtocolException {
      return false;
    }
  }

  void _release(FakeChannel channel) {
    if (identical(_channels[channel.id], channel)) {
      _channels.remove(channel.id);
      if (!_undelivered.contains(channel)) _releasePayload(channel);
      scheduleMicrotask(_checkIdle);
    }
  }

  void _checkIdle() {
    if (_closing || _channels.isNotEmpty) {
      return;
    }
    final idle = _idle;
    _idle = null;
    if (idle != null && !idle.isCompleted) {
      idle.complete();
    }
    if (_peerGoAway != null) {
      unawaited(close());
    }
  }

  @override
  Future<Duration> ping([Uint8List? payload]) {
    if (payload != null && payload.length > 125) {
      throw ArgumentError.value(payload.length, 'payload', 'at most 125 bytes');
    }
    if (_closing) {
      return Future.error(
        SwitchboardException.of(
          StatusCode.failedPrecondition,
          'connection is closed',
        ),
      );
    }
    final bytes = payload == null
        ? (ByteData(4)..setUint32(0, _pingCounter++)).buffer.asUint8List()
        : Uint8List.fromList(payload);
    final completer = Completer<Duration>();
    if (_pings.length >= MuxConnection.maxPendingPings) {
      _pings
          .removeAt(0)
          .$2
          .completeError(
            SwitchboardException.of(StatusCode.deadlineExceeded, 'no PONG'),
          );
    }
    _pings.add((bytes, completer, Stopwatch()..start()));
    _postControl(_Ping(bytes));
    return completer.future;
  }

  @override
  Future<void> goAway([Status status = const Status(33)]) {
    checkMuxStatus(status);
    if (_closing) {
      return close();
    }
    if (!_goAwaySent) {
      _goAwaySent = true;
      _ownGoAway = status;
      _postControl(_GoAway(statusFromWire(statusForWire(status))));
    }
    return _goAwayWait();
  }

  Future<void> _goAwayWait() async {
    if (_channels.isNotEmpty && !_closing) {
      final idle = _idle ??= Completer<void>();
      await idle.future.timeout(options.goAwayGrace, onTimeout: () {});
    }
    await close();
  }

  @override
  Future<void> close() => _shutdown(_lostStatus(), null);

  Status _lostStatus() =>
      _peerGoAway ?? _ownGoAway ?? Status.of(StatusCode.connectionLost);

  /// Ends the connection because the peer broke the rules: GOAWAY with
  /// [status], then the end.
  void _fail(Status status) {
    if (_closing) {
      return;
    }
    _postControl(_GoAway(status));
    unawaited(_shutdown(status, _lostStatus()));
  }

  Future<void> _shutdown(Status status, Status? channelStatus) async {
    if (_closing) {
      return;
    }
    _closing = true;
    _closeStatus = status;
    _postControl(_Closed());
    final lost = channelStatus ?? status;
    for (final channel in List.of(_channels.values)) {
      channel._connectionLost(lost);
      if (!_undelivered.contains(channel)) _releasePayload(channel);
    }
    _channels.clear();
    for (final (_, completer, _) in _pings) {
      completer.completeError(SwitchboardException(status));
    }
    _pings.clear();
    _heldControl.clear();
    final waiter = _nonceWaiter;
    _nonceWaiter = null;
    waiter?.completeError(SwitchboardException(status));
    _signalIdentity();
    final idle = _idle;
    _idle = null;
    if (idle != null && !idle.isCompleted) {
      idle.complete();
    }
    _scheduleIncoming();
    if (!_done.isCompleted) {
      _done.complete(status);
    }
  }

  // Identity -------------------------------------------------------------

  @override
  Future<void> identify(
    Credential credential, {
    HolderKey? holderKey,
    Uint8List? intent,
    String? receiver,
    Duration? timeout,
  }) async {
    final holder = credential.holderKey;
    if (holder != null &&
        (holderKey == null || !_equal(holderKey.publicKey, holder))) {
      throw ArgumentError.value(holderKey, 'holderKey', 'not the holder key');
    }
    final intentBytes = intent ?? Uint8List(0);
    final receiverBytes = Uint8List.fromList(
      receiver == null ? const [] : utf8.encode(receiver),
    );
    if (_closing) {
      throw SwitchboardException.of(
        StatusCode.failedPrecondition,
        'connection is closed',
      );
    }
    final limit = timeout ?? options.identityTimeout;
    Future<T> bounded<T>(Future<T> future, String what) =>
        limit <= Duration.zero
        ? future
        : future.timeout(
            limit,
            onTimeout: () => throw SwitchboardException.of(
              StatusCode.deadlineExceeded,
              what,
            ),
          );
    final localNonce = _localNonce ?? _sendNonce();
    final Uint8List peerNonce;
    try {
      peerNonce =
          _peerNonce ??
          await bounded(
            (_nonceWaiter ??= Completer<Uint8List>()..future.ignore()).future,
            'no NONCE from the peer',
          );
    } on SwitchboardException catch (e) {
      throw e.code == StatusCode.deadlineExceeded ? e : _identifyFailure();
    }
    final proof = holder == null
        ? null
        : await holderKey!.sign(
            MuxIdent.proofMessage(
              localNonce,
              peerNonce,
              intentBytes,
              receiverBytes,
            ),
          );
    if (_closing) {
      throw _identifyFailure();
    }
    _postControl(
      _Ident(
        MuxIdent(
          credential: credential.encode(),
          intent: intentBytes,
          receiver: receiverBytes,
          proof: proof,
        ).encode(),
      ),
    );
    try {
      // The control stream is ordered: the PONG means the IDENT was
      // handled, and accepted, since a refusal ends the connection.
      await bounded(ping(), 'IDENT not confirmed');
    } on SwitchboardException catch (e) {
      throw e.code == StatusCode.deadlineExceeded ? e : _identifyFailure();
    }
  }

  SwitchboardException _identifyFailure() {
    if (_peerGoAwayReceived?.known == StatusCode.unauthenticated) {
      return SwitchboardException.of(
        StatusCode.unauthenticated,
        'the peer refused the credential',
      );
    }
    return SwitchboardException(
      _closeStatus ??
          Status.of(StatusCode.failedPrecondition, 'connection is closed'),
    );
  }

  Uint8List _sendNonce() {
    final nonce = Uint8List.fromList(
      List.generate(MuxIdent.nonceLength, (i) => (i * 37 + _nextId) & 0xFF),
    );
    // Not secure, and need not be: the two sides only must differ.
    nonce[0] = isInitiator ? 1 : 2;
    _localNonce = nonce;
    _postControl(_Nonce(nonce));
    return nonce;
  }

  void _signalIdentity() {
    final event = _identityEvent;
    _identityEvent = null;
    event?.complete();
  }

  // Receiving control -------------------------------------------------------

  void _onControl(_Control record) {
    if (_closing) {
      return;
    }
    if (_verifying) {
      if (_heldControl.length >= 1024) {
        _fail(Status.of(StatusCode.resourceExhausted, 'held control budget'));
        return;
      }
      _heldControl.add(record);
      return;
    }
    switch (record) {
      case _Ping(:final payload):
        _postControl(_Pong(payload));
      case _Pong(:final payload):
        final i = _pings.indexWhere((p) => _equal(p.$1, payload));
        if (i >= 0) {
          final (_, completer, watch) = _pings.removeAt(i);
          completer.complete(watch.elapsed);
        }
      case _GoAway(:final status):
        if (_peerGoAway == null) {
          _peerGoAwayReceived = status;
          _peerGoAway = status.known == StatusCode.goingAway
              ? status
              : Status.of(StatusCode.goingAway, status.toString());
          _peerGoAwayStatus.complete(status);
          scheduleMicrotask(_checkIdle);
        }
      case _Nonce(:final nonce):
        if (_peerNonce != null) {
          _fail(Status.of(StatusCode.protocolError, 'second NONCE'));
          return;
        }
        _peerNonce = nonce;
        final requested = _localNonce == null;
        if (requested) {
          _sendNonce();
        }
        final waiter = _nonceWaiter;
        _nonceWaiter = null;
        waiter?.complete(nonce);
        if (requested && !_identityRequested.isCompleted) {
          _identityRequested.complete();
        }
      case _Ident(:final payload):
        _onIdent(payload);
      case _Closed():
        unawaited(_shutdown(_lostStatus(), null));
    }
  }

  void _onIdent(Uint8List payload) {
    final localNonce = _localNonce;
    final peerNonce = _peerNonce;
    if (localNonce == null || peerNonce == null) {
      _fail(Status.of(StatusCode.protocolError, 'IDENT before the nonces'));
      return;
    }
    final MuxIdent ident;
    try {
      ident = MuxIdent.decode(payload);
    } on ProtocolException catch (e) {
      _fail(e.status);
      return;
    }
    final verifier = options.identityVerifier;
    if (verifier == null) {
      return;
    }
    // What follows on the control stream waits for the verification; the
    // channels do not, as with QUIC's streams.
    _verifying = true;
    verifyIdent(
      ident,
      verifier: verifier,
      senderNonce: peerNonce,
      receiverNonce: localNonce,
      localIdentity: localIdentity,
      requireNamedIdent: options.requireNamedIdent,
    ).then(
      (identity) {
        _verifying = false;
        if (_closing) {
          return;
        }
        _peerIdentity = identity;
        if (!_peerIdentified.isCompleted) {
          _peerIdentified.complete(identity);
        }
        _signalIdentity();
        while (_heldControl.isNotEmpty && !_verifying && !_closing) {
          _onControl(_heldControl.removeFirst());
        }
      },
      onError: (Object _) {
        _verifying = false;
        _heldControl.clear();
        _fail(Status.of(StatusCode.unauthenticated, 'identification failed'));
      },
    );
  }

  @override
  String toString() =>
      'FakeConnection(${isInitiator ? 'initiator' : 'acceptor'}, '
      '${_channels.length} channels${_closing ? ', closed' : ''})';
}

/// A channel of the fake binding: a stream of its own.
class FakeChannel extends MuxChannel {
  FakeChannel._(
    this.connection,
    this.id, {
    required this.isLocallyOpened,
    required this.openPayload,
    required int sendWindow,
    required int receiveWindow,
  }) : _initialSendWindow = sendWindow,
       _sendWindow = sendWindow,
       _receiveWindow = receiveWindow,
       _creditWindow = receiveWindow {
    _incoming = StreamController<Uint8List>(
      sync: true,
      onListen: _scheduleDrain,
      onResume: _scheduleDrain,
      onCancel: _discard,
    );
    _sink = _FakeSink(this);
  }

  @override
  final FakeConnection connection;

  @override
  final int id;

  @override
  final bool isLocallyOpened;

  @override
  final Uint8List openPayload;

  FakeChannel? _peerChannel;
  late final StreamController<Uint8List> _incoming;
  // Closed by the application through close().
  // ignore: close_sinks
  late final _FakeSink _sink;
  final Completer<Status> _done = Completer<Status>();
  MuxChannelState _state = MuxChannelState.open;
  MuxPriority _priority = MuxPriority.normal;
  Status _endStatus = Status.ok;
  bool _closeSent = false;
  bool _closeQueued = false;
  bool _closeReceived = false;
  Status? _pendingClose;
  Timer? _confirmTimer;

  // Whether this channel's open went on the wire; until then (a channel of
  // openAfter) what it sends waits in [_preOpen].
  bool _opened = false;
  final List<void Function()> _preOpen = [];
  final Set<FakeChannel> _childrenWaitingOpen = {};

  // Sending.
  final int _initialSendWindow;
  int _sendWindow;
  final Queue<_Send> _sends = Queue<_Send>();
  final List<Completer<void>> _writableWaiters = [];
  bool _admitting = false;

  // Receiving.
  final Queue<Uint8List> _queue = Queue<Uint8List>();
  int _queuedBytes = 0;
  bool _drainScheduled = false;
  bool _discarding = false;
  bool _ended = false;
  int _receiveWindow;
  int _creditWindow;
  int _unreturned = 0;
  int _held = 0;
  bool _manualCredit = false;
  bool _released = false;

  @override
  MuxChannelState get state => _state;

  @override
  MuxPriority get priority => _priority;

  @override
  set priority(MuxPriority value) => _priority = value;

  @override
  bool get canSend => _state == MuxChannelState.open;

  @override
  int get bufferedBytes => _queuedBytes;

  @override
  Stream<Uint8List> get stream => _incoming.stream;

  @override
  bool get hasListener => _incoming.hasListener;

  @override
  int get receiveWindow => _receiveWindow;

  @override
  bool get manualCredit => _manualCredit;

  @override
  set manualCredit(bool value) {
    if (value == _manualCredit) {
      return;
    }
    _manualCredit = value;
    if (!value && _held > 0) {
      final held = _held;
      _held = 0;
      _consume(held);
    }
  }

  @override
  int get heldBytes => _held;

  @override
  void consumed(int bytes, {int subframes = 1}) {
    if (bytes < 0 || subframes < 0) {
      throw ArgumentError('negative bytes ($bytes) or subframes ($subframes)');
    }
    if (_released) {
      return;
    }
    final cost = bytes + subframes;
    if (cost > _held) {
      throw StateError('$cost credit reported consumed, $_held held');
    }
    _held -= cost;
    _consume(cost);
  }

  @override
  void grant(int bytes) {
    if (bytes <= 0) {
      throw ArgumentError.value(bytes, 'bytes', 'must be positive');
    }
    if (!_mayReturnCredit) {
      return;
    }
    if (_creditWindow + bytes > 0xFFFFFFFF) {
      throw ArgumentError.value(bytes, 'bytes', 'window exceeds binding limit');
    }
    _creditWindow += bytes;
    final credit = bytes + _unreturned;
    _unreturned = 0;
    _receiveWindow += credit;
    _toPeer('credit', (peer) => peer._onCredit(credit));
  }

  @override
  StreamSink<Uint8List> get sink => _sink;

  @override
  Future<Status> get done => _done.future;

  @override
  int get sendWindow => _sendWindow;

  /// One byte of record overhead bounds empty subframes too.
  @override
  int sendCost(int length) => length + 1;

  @override
  int get maxSubframeLength => _initialSendWindow < 2
      ? _initialSendWindow - 1
      : _initialSendWindow ~/ 2 - 1;

  @override
  Future<void> send(Uint8List subframe) {
    if (!canSend) {
      throw SwitchboardException.of(
        StatusCode.failedPrecondition,
        'channel $id is ${_state.name}',
      );
    }
    final split = _priority == MuxPriority.bulk && subframe.isNotEmpty;
    if (split &&
        (_initialSendWindow < 2 || connection.options.bulkChunkSize < 1)) {
      throw SwitchboardException.of(
        StatusCode.frameTooLarge,
        'no room for bulk',
      );
    }
    if (!split && subframe.length > maxSubframeLength) {
      throw SwitchboardException.of(
        StatusCode.frameTooLarge,
        'subframe of ${subframe.length} bytes, at most $maxSubframeLength',
      );
    }
    final pending = _Send(Uint8List.fromList(subframe), split: split);
    _sends.add(pending);
    _admit();
    return pending.completer.future;
  }

  @override
  bool get isWritable =>
      canSend &&
      _sends.isEmpty &&
      _sendWindow >= sendCost(maxSubframeLength < 0 ? 0 : maxSubframeLength);

  @override
  Future<void> get whenWritable {
    final waiter = Completer<void>()..future.ignore();
    if (!canSend) {
      waiter.completeError(
        SwitchboardException.of(
          StatusCode.failedPrecondition,
          'channel $id is ${_state.name}',
        ),
      );
    } else if (isWritable) {
      waiter.complete();
    } else {
      _writableWaiters.add(waiter);
    }
    return waiter.future;
  }

  void _wakeWritable() {
    if (_writableWaiters.isEmpty || (canSend && !isWritable)) {
      return;
    }
    final waiters = List.of(_writableWaiters);
    _writableWaiters.clear();
    for (final waiter in waiters) {
      if (canSend) {
        waiter.complete();
      } else {
        waiter.completeError(
          SwitchboardException.of(
            StatusCode.failedPrecondition,
            'channel $id is ${_state.name}',
          ),
        );
      }
    }
  }

  @override
  MuxChannel openAfter(Uint8List openPayload) {
    if (!canSend) {
      throw SwitchboardException.of(
        StatusCode.failedPrecondition,
        'channel $id is ${_state.name}',
      );
    }
    final child = connection._openLocal(openPayload, continuation: true);
    if (_sends.isEmpty) {
      _postChild(child);
    } else {
      _sends.add(_Send.open(child));
    }
    return child;
  }

  @override
  Future<void> close([Status status = Status.ok]) {
    checkMuxStatus(status);
    if (_state == MuxChannelState.open) {
      _closeSent = true;
      _state = MuxChannelState.halfClosedLocal;
      _noteStatus(status);
      if (_opened) {
        _startConfirmTimer();
      }
      if (_sends.isEmpty) {
        _postClose(status);
      } else {
        _pendingClose = status;
      }
      _wakeWritable();
    }
    return _done.future;
  }

  // Sending ---------------------------------------------------------------

  /// Runs [deliver] on the peer's channel, in order with this channel's
  /// other records, once this channel's open went out.
  void _toPeer(String kind, void Function(FakeChannel peer) deliver) {
    if (!_opened) {
      _preOpen.add(() => _toPeer(kind, deliver));
      return;
    }
    connection._count(kind);
    connection._postData(id, () {
      final peer = _peerChannel;
      if (peer != null) {
        deliver(peer);
      }
    });
  }

  /// Puts this channel's open on the wire, and what waited for it.
  void _postOpen() {
    _opened = true;
    connection._count('open');
    connection._postData(id, () => connection._peer._onOpen(this));
    final waiting = List.of(_preOpen);
    _preOpen.clear();
    for (final post in waiting) {
      post();
    }
    if (_state == MuxChannelState.halfClosedLocal) {
      _startConfirmTimer();
    }
  }

  void _postChild(FakeChannel child) {
    if (_opened) {
      child._postOpen();
    } else {
      _childrenWaitingOpen.add(child);
      _preOpen.add(() {
        _childrenWaitingOpen.remove(child);
        child._postOpen();
      });
    }
  }

  void _admit() {
    if (_admitting) {
      return;
    }
    _admitting = true;
    try {
      while (_sends.isNotEmpty) {
        final head = _sends.first;
        final child = head.opens;
        if (child != null) {
          _sends.removeFirst();
          _postChild(child);
          head.completer.complete();
          continue;
        }
        if (head.split) {
          final floor = maxSubframeLength < 1 ? 1 : maxSubframeLength;
          while (head.offset < head.data.length) {
            final rest = head.data.length - head.offset;
            if (_sendWindow < sendCost(rest < floor ? rest : floor)) {
              break;
            }
            var n = connection.options.bulkChunkSize;
            if (rest < n) {
              n = rest;
            }
            if (_sendWindow - 1 < n) {
              n = _sendWindow - 1;
            }
            final start = head.offset;
            head.offset += n;
            _sendWindow -= sendCost(n);
            _postSubframe(Uint8List.sublistView(head.data, start, start + n));
          }
          if (head.offset < head.data.length) {
            break;
          }
        } else {
          if (sendCost(head.data.length) > _sendWindow) {
            break;
          }
          _sendWindow -= sendCost(head.data.length);
          _postSubframe(head.data);
        }
        _sends.removeFirst();
        head.completer.complete();
      }
    } finally {
      _admitting = false;
    }
    final close = _pendingClose;
    if (_sends.isEmpty && close != null) {
      _pendingClose = null;
      _postClose(close);
    }
    _wakeWritable();
  }

  void _postSubframe(Uint8List data) =>
      _toPeer('data', (peer) => peer._receive(data));

  void _postClose(Status status) {
    _closeQueued = true;
    final wire = statusForWire(status);
    _toPeer('close', (peer) => peer._onClose(statusFromWire(wire)));
  }

  void _failSends() {
    final error = SwitchboardException.of(
      StatusCode.failedPrecondition,
      'channel $id closed before the subframe was sent',
    );
    final sends = List.of(_sends);
    _sends.clear();
    for (final send in sends) {
      send.completer.completeError(error);
      final child = send.opens;
      if (child != null) {
        child
          .._noteStatus(
            Status.of(StatusCode.cancelled, 'the open never went out'),
          )
          .._complete();
        connection._release(child);
      }
    }
  }

  void _startConfirmTimer() {
    final timeout = connection.options.closeConfirmTimeout;
    if (timeout <= Duration.zero || _confirmTimer != null) {
      return;
    }
    _confirmTimer = Timer(timeout, () {
      _confirmTimer = null;
      if (_state != MuxChannelState.halfClosedLocal) {
        return;
      }
      final close = _pendingClose;
      if (close != null) {
        _pendingClose = null;
        _failSends();
        _postClose(close.isOk ? Status.of(StatusCode.deadlineExceeded) : close);
      }
      connection._release(this);
      _complete();
    });
  }

  // Receiving -------------------------------------------------------------

  void _receive(Uint8List data) {
    if (_closeReceived || _released) {
      return;
    }
    final cost = sendCost(data.length);
    _receiveWindow -= cost;
    if (_receiveWindow < 0) {
      throw StateError('channel $id: the peer sent beyond its window');
    }
    if (_discarding || _ended) {
      _consume(cost);
      return;
    }
    _queue.add(data);
    _queuedBytes += cost;
    _scheduleDrain();
  }

  void _onCredit(int credit) {
    if (_closeReceived || _closeQueued || _state == MuxChannelState.closed) {
      return;
    }
    _sendWindow += credit;
    _admit();
  }

  void _onClose(Status status) {
    if (_closeReceived || _state == MuxChannelState.closed) {
      return;
    }
    _closeReceived = true;
    _noteStatus(status);
    final confirm = !_closeSent;
    final ours = _pendingClose;
    _pendingClose = null;
    _failSends();
    _closeSent = true;
    _state = MuxChannelState.closed;
    connection._release(this);
    if (confirm) {
      _postClose(Status.ok);
    } else if (ours != null) {
      _postClose(ours);
    }
    _complete();
  }

  bool get _mayReturnCredit => !_released && !_closeReceived && !_closeQueued;

  void _consume(int cost) {
    _unreturned += cost;
    if (_unreturned == 0 || !_mayReturnCredit) {
      return;
    }
    final half = _creditWindow ~/ 2;
    if (_unreturned < (half < 1 ? 1 : half)) {
      return;
    }
    final credit = _unreturned;
    _unreturned = 0;
    _receiveWindow += credit;
    _toPeer('credit', (peer) => peer._onCredit(credit));
  }

  void _discard() {
    _discarding = true;
    final cost = _queuedBytes;
    _queue.clear();
    _queuedBytes = 0;
    if (cost > 0) {
      _consume(cost);
    }
  }

  void _scheduleDrain() {
    if (_drainScheduled) {
      return;
    }
    _drainScheduled = true;
    scheduleMicrotask(_drain);
  }

  void _drain() {
    _drainScheduled = false;
    while (_queue.isNotEmpty && _incoming.hasListener && !_incoming.isPaused) {
      final data = _queue.removeFirst();
      final cost = sendCost(data.length);
      _queuedBytes -= cost;
      if (_manualCredit && !_released) {
        _held += cost;
      } else {
        _unreturned += cost;
      }
      _incoming.add(data);
    }
    _consume(0);
    if (_ended && _queue.isEmpty && !_incoming.isClosed) {
      unawaited(_incoming.close());
    }
  }

  void _noteStatus(Status status) {
    if (_endStatus.isOk && !status.isOk) {
      _endStatus = status;
    }
  }

  void _connectionLost(Status status) {
    _noteStatus(status);
    _complete();
  }

  void _complete() {
    _state = MuxChannelState.closed;
    _confirmTimer?.cancel();
    _confirmTimer = null;
    _pendingClose = null;
    _failSends();
    _preOpen.clear();
    for (final child in _childrenWaitingOpen) {
      child
        .._noteStatus(
          Status.of(StatusCode.cancelled, 'the open never went out'),
        )
        .._complete();
      connection._release(child);
    }
    _childrenWaitingOpen.clear();
    _released = true;
    _held = 0;
    _ended = true;
    _wakeWritable();
    _scheduleDrain();
    if (!_done.isCompleted) {
      _done.complete(_endStatus);
    }
  }

  @override
  String toString() =>
      'FakeChannel($id, ${_state.name}, '
      '${isLocallyOpened ? 'local' : 'remote'})';
}

/// A subframe (or bulk payload) waiting for the window, or the open of a
/// channel of [MuxChannel.openAfter] in order with them.
class _Send {
  _Send(this.data, {required this.split}) : opens = null;

  _Send.open(FakeChannel this.opens) : data = Uint8List(0), split = false;

  final Uint8List data;
  final bool split;
  final FakeChannel? opens;
  int offset = 0;
  final Completer<void> completer = Completer<void>()..future.ignore();
}

class _FakeSink implements StreamSink<Uint8List> {
  _FakeSink(this._channel);

  final FakeChannel _channel;
  bool _closed = false;

  @override
  void add(Uint8List event) {
    if (_closed) {
      throw StateError('sink closed');
    }
    if (_channel.canSend) {
      _channel.send(event).ignore();
    }
  }

  @override
  void addError(Object error, [StackTrace? stackTrace]) {
    _closed = true;
    var status = error is SwitchboardException
        ? error.status
        : Status.of(StatusCode.internal, '$error');
    if (status.isApplicationCode) {
      status = Status.of(StatusCode.unknown, '$status');
    }
    unawaited(_channel.close(status));
  }

  @override
  Future<void> addStream(Stream<Uint8List> stream) async {
    await for (final event in stream) {
      if (_closed) {
        break;
      }
      if (_channel.canSend) {
        try {
          await _channel.send(event);
        } on SwitchboardException catch (e) {
          if (e.code != StatusCode.failedPrecondition) {
            rethrow;
          }
        }
      }
    }
  }

  @override
  Future<void> close() {
    _closed = true;
    return _channel.close();
  }

  @override
  Future<void> get done => _channel.done;
}

bool _equal(List<int> a, List<int> b) {
  if (a.length != b.length) {
    return false;
  }
  for (var i = 0; i < a.length; i++) {
    if (a[i] != b[i]) {
      return false;
    }
  }
  return true;
}

/// A fake listener: a node adopting the connections dialled to it.
class _FakeListener {
  _FakeListener(this.node, this.policy);

  final Switchboard node;
  final ChannelPolicy? policy;
}

/// The [SwitchboardBinding] of `fake://name` endpoints: the listeners of
/// this isolate started with [listen], each adopting the connections
/// dialled to it into its node ([Switchboard.adopt]).
class FakeBinding extends SwitchboardBinding {
  FakeBinding({this.controlDelay = Duration.zero});

  /// The scheme of the fake binding's endpoints.
  static const String scheme = 'fake';

  static final Map<String, _FakeListener> _listeners = {};

  /// How much later than channel records control records arrive on the
  /// connections this binding dials.
  final Duration controlDelay;

  /// Every connection this binding dialled, initiator side.
  final List<FakeConnection> dialled = [];

  /// Has [node] accept the connections dialled to `fake://[name]`, under
  /// [policy], until [stop].
  static Uri listen(Switchboard node, String name, {ChannelPolicy? policy}) {
    _listeners[name] = _FakeListener(node, policy);
    return Uri(scheme: scheme, host: name);
  }

  /// Stops the listener at `fake://[name]`.
  static void stop(String name) => _listeners.remove(name);

  @override
  Set<String> get schemes => const {scheme};

  @override
  Future<MuxConnection> dial(Uri endpoint, BindingDialSettings settings) async {
    final listener = _listeners[endpoint.host];
    if (listener == null) {
      throw SwitchboardException.of(
        StatusCode.unavailable,
        'no fake listener at $endpoint',
      );
    }
    final (local, remote) = FakeConnection.pair(
      initiator: settings.options,
      acceptor: listener.node.muxOptions,
      controlDelay: controlDelay,
    );
    local.localIdentity = settings.localIdentity;
    await listener.node.adopt(
      remote,
      policy: listener.policy,
      remote: 'fake peer',
    );
    dialled.add(local);
    return local;
  }
}
