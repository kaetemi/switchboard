/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Opus 5.5 <noreply@anthropic.com>
  GLM 5.3 Flash <noreply@z.ai>
*/

// Item windows on stream requests (wiki page "Polyverse Switchboard Talk",
// sections "Item windows" and "Timeouts", "Credit"): the window a stream
// request opens with, the credits (EXTEND from the requester) that move
// it, the responder that sends nothing beyond it, the timeouts that stop
// while it is exhausted and the credit bound that runs instead.

import 'dart:async';
import 'dart:typed_data';

import 'package:async/async.dart';
import 'package:fake_async/fake_async.dart';
import 'package:stream_channel/stream_channel.dart';
import 'package:switchboard/core.dart';
import 'package:test/test.dart';

import 'mux_harness.dart' show muxPair;

Uint8List bytes(List<int> values) => Uint8List.fromList(values);

Matcher isStatus(StatusCode code) =>
    isA<SwitchboardException>().having((e) => e.code, 'code', code);

Matcher isRemote(StatusCode code) =>
    isA<TalkAbortException>().having((e) => e.code, 'code', code);

/// A credit for request [id] of [items] items.
TalkFrame credit(int id, int items) => TalkFrame(
  kind: TalkKind.extend,
  requestId: id,
  payload: TalkFrame.creditPayload(items),
);

/// A stream request [id] "LIST" with [window].
TalkFrame streamRequestFrame(int id, int window) => TalkFrame(
  kind: TalkKind.message,
  procedure: Name('LIST'),
  requestId: id,
  stream: true,
  window: window,
);

/// A stream item for request [id].
TalkFrame itemFrame(int id, int value) => TalkFrame(
  kind: TalkKind.streamItem,
  responseId: id,
  payload: bytes([value]),
);

/// Two talk channels joined through a tap that records every frame.
class Pair {
  Pair({
    TalkOptions a = const TalkOptions(),
    TalkOptions b = const TalkOptions(),
  }) {
    final ca = StreamChannelController<Uint8List>();
    final cb = StreamChannelController<Uint8List>();
    ca.foreign.stream.listen((frame) {
      aToB.add(TalkFrame.decode(frame));
      cb.foreign.sink.add(frame);
    }, onDone: () => cb.foreign.sink.close());
    cb.foreign.stream.listen((frame) {
      bToA.add(TalkFrame.decode(frame));
      ca.foreign.sink.add(frame);
    }, onDone: () => ca.foreign.sink.close());
    this.a = TalkChannel(ca.local, options: a);
    this.b = TalkChannel(cb.local, options: b);
  }

  late final TalkChannel a;
  late final TalkChannel b;
  final List<TalkFrame> aToB = [];
  final List<TalkFrame> bToA = [];

  /// The credits [a] sent.
  List<TalkFrame> get credits => [
    for (final f in aToB)
      if (f.isCredit) f,
  ];

  /// The items [b] sent.
  List<TalkFrame> get items => [
    for (final f in bToA)
      if (f.kind == TalkKind.streamItem) f,
  ];

  Future<void> close() => Future.wait([a.close(), b.close()]);
}

/// Stands in for the mux channel: closable with a status, has an end
/// status.
class FakeMuxChannel extends StreamChannelMixin<Uint8List>
    implements StatusClosable {
  FakeMuxChannel(this._inner);

  final StreamChannel<Uint8List> _inner;
  final Completer<Status> _done = Completer<Status>();

  @override
  Stream<Uint8List> get stream => _inner.stream;

  @override
  StreamSink<Uint8List> get sink => _inner.sink;

  @override
  Future<void> close([Status status = Status.ok]) async {
    if (!_done.isCompleted) {
      _done.complete(status);
    }
    _inner.sink.close().ignore();
  }

  @override
  Future<Status> get done => _done.future;

  /// The peer closed the channel with [status]; the stream ends.
  void endFromPeer(StreamChannel<Uint8List> foreign, Status status) {
    _done.complete(status);
    foreign.sink.close().ignore();
  }
}

/// A talk channel whose peer is driven by hand at the frame level.
class RawPeer {
  RawPeer({TalkOptions options = const TalkOptions()}) {
    final c = StreamChannelController<Uint8List>();
    raw = c.foreign;
    mux = FakeMuxChannel(c.local);
    talk = TalkChannel(mux, options: options);
    raw.stream.listen((frame) => received.add(TalkFrame.decode(frame)));
  }

  late final StreamChannel<Uint8List> raw;
  late final FakeMuxChannel mux;
  late final TalkChannel talk;
  final List<TalkFrame> received = [];

  void send(TalkFrame frame) => raw.sink.add(frame.encode());
  void sendHex(String hex) => raw.sink.add(hexBytes(hex));

  /// The peer ends the channel with [status].
  void end(Status status) => mux.endFromPeer(raw, status);

  /// The frames [talk] sent for the peer's request [id].
  List<TalkFrame> repliesTo(int id) => [
    for (final f in received)
      if (f.responseId == id) f,
  ];
}

/// Synchronously observable outcome of a future, for fake_async tests.
class Outcome<T> {
  Outcome(Future<T> future) {
    future.then(
      (v) {
        value = v;
        isDone = true;
      },
      onError: (Object e) {
        error = e;
        isDone = true;
      },
    );
  }

  T? value;
  Object? error;
  bool isDone = false;
}

/// Polls [condition] until it holds, at most 5 s.
Future<void> until(bool Function() condition) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('condition not met');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

/// The single next request [channel] receives.
Future<TalkMessage> nextRequest(TalkChannel channel) => channel.messages.first;

void main() {
  group('the window', () {
    test('a finite window delivers exactly that many items, then nothing '
        'until a grant', () async {
      final p = Pair();
      final served = nextRequest(p.b);
      final s = p.a.streamRequest('LIST', Uint8List(0), window: 3);
      expect(s.window, 3);
      expect(p.aToB, isEmpty, reason: 'not on the wire before a turn');
      final m = await served;
      expect(p.aToB.single.window, 3);
      expect(m.window, 3);
      final sent = [
        for (var i = 0; i < 5; i++) Outcome(m.replyItem(bytes([i]))),
      ];
      final items = <int>[];
      s.items.listen((item) => items.add(item.payload.single));
      await pumpEventQueue();
      expect(items, [0, 1, 2]);
      expect(p.items, hasLength(3), reason: 'nothing beyond the window');
      expect(s.window, 0);
      expect(m.window, 0);
      expect(
        [for (final o in sent) o.isDone],
        [true, true, true, false, false],
      );
      // Nothing is granted on the application's behalf.
      expect(p.credits, isEmpty);
      s.grant(1);
      await pumpEventQueue();
      expect(items, [0, 1, 2, 3]);
      expect(sent[3].isDone, isTrue);
      expect(sent[4].isDone, isFalse);
      expect(p.credits.single.requestId, s.requestId);
      expect(p.credits.single.creditItems, 1);
      expect(hexString(p.credits.single.encode()), '32 01 00 00 01 00 00 00');
      s.grant(5);
      await pumpEventQueue();
      expect(items, [0, 1, 2, 3, 4]);
      expect(sent[4].isDone, isTrue);
      expect(s.window, 4);
      expect(m.window, 4);
      unawaited(m.reply(Uint8List(0)));
      await s.done;
      expect(p.a.outgoingRequestCount, 0);
      expect(p.b.incomingRequestCount, 0);
      await p.close();
    });

    test('grants add up, while exhausted and ahead of time', () async {
      final p = Pair();
      final served = nextRequest(p.b);
      final s = p.a.streamRequest('LIST', Uint8List(0), window: 1);
      final items = <int>[];
      s.items.listen((item) => items.add(item.payload.single));
      final m = await served;
      for (var i = 0; i < 10; i++) {
        unawaited(m.replyItem(bytes([i])));
      }
      await pumpEventQueue();
      expect(items, [0]);
      // Two grants in one turn: both count.
      s
        ..grant(2)
        ..grant(1);
      expect(s.window, 3);
      await pumpEventQueue();
      expect(items, [0, 1, 2, 3]);
      expect(m.window, 0);
      // A grant ahead of consumption adds to what is left.
      s.grant(2);
      await pumpEventQueue();
      expect(items, hasLength(6));
      expect(p.credits.map((c) => c.creditItems), [2, 1, 2]);
      s.cancel();
      await p.close();
    });

    test('window 0 starts exhausted, and the first grant starts '
        'delivery', () async {
      final p = Pair();
      final served = nextRequest(p.b);
      final s = p.a.streamRequest('LIST', Uint8List(0), window: 0);
      final items = <int>[];
      s.items.listen((item) => items.add(item.payload.single));
      final m = await served;
      expect(p.aToB.single.window, 0);
      expect(hexString(p.aToB.single.encode()), endsWith('00 00 00 00'));
      expect(m.window, 0);
      final first = Outcome(m.replyItem(bytes([1])));
      unawaited(m.replyItem(bytes([2])));
      await pumpEventQueue();
      expect(items, isEmpty);
      expect(p.items, isEmpty, reason: 'nothing on the wire');
      expect(first.isDone, isFalse);
      s.grant(2);
      await pumpEventQueue();
      expect(items, [1, 2]);
      expect(first.isDone, isTrue);
      unawaited(m.reply(Uint8List(0)));
      await s.done;
      await p.close();
    });

    test('the final response, an abort and an extend pass at zero', () async {
      final p = Pair();
      final served = StreamQueue(p.b.messages);
      final declared = <Duration?>[];
      final s = p.a.streamRequest(
        'LIST',
        Uint8List(0),
        window: 0,
        onExtend: (deadline, renew) => declared.add(deadline),
      );
      final aborted = p.a.streamRequest('LIST', Uint8List(0), window: 0);
      final m = await served.next;
      final n = await served.next;
      // An item waits; the extend and the final do not.
      final item = Outcome(m.replyItem(bytes([1])));
      await pumpEventQueue();
      expect(item.isDone, isFalse);
      await m.extend(deadline: const Duration(minutes: 1));
      await pumpEventQueue();
      expect(declared, hasLength(1));
      // A final made while an item waits goes after it: it does not wait
      // itself, but it does not overtake.
      final trailer = Outcome(m.reply(bytes([0xEE])));
      expect(m.canReply, isFalse);
      expect(
        () => m.replyItem(bytes([2])),
        throwsA(isStatus(StatusCode.failedPrecondition)),
      );
      await pumpEventQueue();
      expect(trailer.isDone, isFalse);
      final items = <int>[];
      s.items.listen((i) => items.add(i.payload.single));
      s.grant(1);
      final last = await s.done;
      expect(items, [1]);
      expect(last.payload, [0xEE]);
      expect(trailer.isDone, isTrue);
      expect(
        [
          for (final f in p.bToA)
            if (f.responseId == s.requestId) f.kind,
        ],
        [TalkKind.extend, TalkKind.streamItem, TalkKind.message],
      );
      // An abort goes at once, whatever waits: the items are not sent.
      final dropped = Outcome(n.replyItem(bytes([3])));
      await n.replyAbort(Status.of(StatusCode.notFound, 'gone'));
      await expectLater(aborted.done, throwsA(isRemote(StatusCode.notFound)));
      await pumpEventQueue();
      expect(dropped.error, isStatus(StatusCode.notFound));
      expect(
        [
          for (final f in p.bToA)
            if (f.responseId == aborted.requestId) f.kind,
        ],
        [TalkKind.abort],
      );
      await served.cancel();
      await p.close();
    });

    test('an empty result completes under window 0, and so does one that '
        'produced only a trailer', () async {
      final p = Pair();
      final served = StreamQueue(p.b.messages);
      final empty = p.a.streamRequest('LIST', Uint8List(0), window: 0);
      final trailer = p.a.streamRequest('LIST', Uint8List(0), window: 0);
      final m = await served.next;
      final n = await served.next;
      unawaited(m.replyStream(const Stream<Uint8List>.empty()));
      unawaited(n.reply(bytes([7])));
      expect(await empty.items.toList(), isEmpty);
      expect((await empty.done).payload, isEmpty);
      expect((await trailer.done).payload, [7]);
      expect(p.credits, isEmpty);
      await served.cancel();
      await p.close();
    });

    test('a page that exactly fills its window delivers its trailer while '
        'the requester still holds its items, and no grant is owed', () async {
      final p = Pair();
      final served = nextRequest(p.b);
      final s = p.a.streamRequest('PAGE', Uint8List(0), window: 3);
      final m = await served;
      unawaited(
        m.replyStream(
          Stream.fromIterable([
            bytes([1]),
            bytes([2]),
            bytes([3]),
          ]),
          trailer: bytes([0xC0]),
        ),
      );
      // The requester has not taken a single item yet.
      final last = await s.done;
      expect(last.payload, [0xC0]);
      expect(s.window, 0);
      // Granting now is a no-op: the request ended.
      s.grant(10);
      expect((await s.items.toList()).map((i) => i.payload.single), [1, 2, 3]);
      await pumpEventQueue();
      expect(p.credits, isEmpty);
      expect(p.b.incomingRequestCount, 0);
      await p.close();
    });

    test('item requests count as one item each', () async {
      final p = Pair();
      final served = nextRequest(p.b);
      final s = p.a.streamRequest('LIST', Uint8List(0), window: 2);
      final asked = <int>[];
      s.items.listen((item) {
        asked.add(item.payload.single);
        item.reply(bytes([item.payload.single + 100]));
      });
      final m = await served;
      final answers = [
        for (var i = 0; i < 3; i++) m.startReplyItemRequest(bytes([i])),
      ];
      // The handle of the waiting one exists at once, its id reserved.
      expect(answers[2].requestId, isNot(answers[1].requestId));
      expect(p.b.outgoingRequestCount, 3);
      await pumpEventQueue();
      expect(asked, [0, 1]);
      expect(p.items, hasLength(2));
      expect((await answers[0].response).payload, [100]);
      expect((await answers[1].response).payload, [101]);
      // Answered or not, they took the window.
      expect(m.window, 0);
      s.grant(1);
      expect((await answers[2].response).payload, [102]);
      expect(asked, [0, 1, 2]);
      unawaited(m.reply(Uint8List(0)));
      await s.done;
      await p.close();
    });

    test('an item request cancelled while it waits for the window is sent, '
        'then cancelled, once the window lets it go', () async {
      final p = Pair();
      final served = nextRequest(p.b);
      final s = p.a.streamRequest('LIST', Uint8List(0), window: 0);
      final seen = <TalkMessage>[];
      s.items.listen(seen.add);
      final m = await served;
      final waiting = m.startReplyItemRequest(bytes([1]));
      waiting.cancel();
      await expectLater(
        waiting.response,
        throwsA(isStatus(StatusCode.cancelled)),
      );
      await pumpEventQueue();
      expect(p.bToA, isEmpty);
      s.grant(1);
      await pumpEventQueue();
      expect(
        [
          for (final f in p.bToA)
            if (f.requestId == waiting.requestId) f.kind,
        ],
        [TalkKind.streamItem, TalkKind.abort],
      );
      expect(seen.single.isCancelled, isTrue);
      await pumpEventQueue();
      expect(p.b.outgoingRequestCount, 0);
      unawaited(m.reply(Uint8List(0)));
      await s.done;
      await p.close();
    });

    test('what arrives for a request that waits for the window, before its '
        'frame went, is for an unknown id', () async {
      final peer = RawPeer();
      final requests = StreamQueue(peer.talk.messages);
      peer.send(streamRequestFrame(1, 0));
      final m = await requests.next;
      final item = Outcome(m.replyItem(bytes([1])));
      final chained = m.startReplyRequest(bytes([2]), procedure: 'MORE');
      final answer = Outcome(chained.response);
      final id = chained.requestId;
      // The peer cannot know of the id yet: whatever names it is stale, for
      // an earlier request of ours with the id, or invented.
      peer
        ..send(
          TalkFrame(
            kind: TalkKind.message,
            responseId: id,
            payload: bytes([7]),
          ),
        )
        ..send(itemFrame(id, 8))
        ..send(TalkFrame(kind: TalkKind.extend, responseId: id))
        ..send(
          TalkFrame(
            kind: TalkKind.abort,
            responseId: id,
            payload: Status.of(StatusCode.notFound).encode(),
          ),
        )
        ..send(
          TalkFrame(
            kind: TalkKind.message,
            requestId: 5,
            responseId: id,
            payload: bytes([9]),
          ),
        );
      await pumpEventQueue();
      expect(peer.talk.isOpen, isTrue);
      expect(answer.isDone, isFalse);
      expect(peer.talk.outgoingRequestCount, 1);
      // Only the chained response that is a request is answered, NOT_FOUND.
      expect(peer.received.single.responseId, 5);
      expect(peer.received.single.status.known, StatusCode.notFound);
      // The grant lets the item and then the request go; its answer counts.
      peer.send(credit(1, 1));
      await pumpEventQueue();
      expect(item.isDone && item.error == null, isTrue);
      expect(
        [for (final f in peer.repliesTo(1)) f.kind],
        [TalkKind.streamItem, TalkKind.message],
      );
      expect(peer.repliesTo(1).last.requestId, id);
      peer.send(
        TalkFrame(kind: TalkKind.message, responseId: id, payload: bytes([3])),
      );
      await pumpEventQueue();
      expect(answer.value!.payload, [3]);
      expect(peer.talk.outgoingRequestCount, 0);
      await requests.cancel();
      await peer.talk.close();
    });

    test('a stream request in a reply carries its window, and grants made '
        'while it waits for the window go with it', () async {
      final p = Pair();
      final served = nextRequest(p.b);
      final s = p.a.streamRequest('LIST', Uint8List(0), window: 0);
      final nested = Completer<TalkMessage>();
      s.items.listen(nested.complete);
      final m = await served;
      final sub = m.replyItemStreamRequest(bytes([1]), window: 2);
      expect(sub.window, 2);
      sub.grant(3);
      expect(sub.window, 5);
      await pumpEventQueue();
      expect(p.bToA, isEmpty, reason: 'it waits for the window of LIST');
      s.grant(1);
      final asked = await nested.future;
      expect(asked.expectsStream, isTrue);
      expect(asked.window, 5);
      expect(
        p.bToA.where((f) => f.isCredit),
        isEmpty,
        reason: 'the grant went with the request',
      );
      // A chained response that is a stream request carries one too.
      final chained = Completer<TalkMessage>();
      unawaited(s.done.then(chained.complete));
      final last = m.replyStreamRequest(bytes([2]), window: 4);
      final answer = await chained.future;
      expect(answer.expectsStream, isTrue);
      expect(answer.window, 4);
      expect(last.window, 4);
      unawaited(answer.reply(Uint8List(0)));
      unawaited(asked.reply(Uint8List(0)));
      await last.done;
      await sub.done;
      await p.close();
    });

    test(
      'an unlimited window is the default, and nothing is counted',
      () async {
        final p = Pair();
        final served = nextRequest(p.b);
        final s = p.a.streamRequest('LIST', Uint8List(0));
        final m = await served;
        expect(p.aToB.single.window, TalkFrame.unlimitedWindow);
        expect(s.window, isNull);
        expect(m.window, isNull);
        await m.whenCredit;
        for (var i = 0; i < 50; i++) {
          unawaited(m.replyItem(bytes([i])));
        }
        unawaited(m.reply(Uint8List(0)));
        expect(await s.items.length, 50);
        expect(s.window, isNull);
        expect(() => s.grant(1), throwsStateError);
        await p.close();
      },
    );

    test('grant checks its argument and the window', () async {
      final p = Pair();
      final served = nextRequest(p.b);
      final s = p.a.streamRequest(
        'LIST',
        Uint8List(0),
        window: TalkFrame.unlimitedWindow - 3,
      );
      final m = await served;
      expect(() => s.grant(0), throwsArgumentError);
      expect(() => s.grant(-1), throwsArgumentError);
      // Up to 0xFFFFFFFE left, never 0xFFFFFFFF.
      expect(() => s.grant(3), throwsArgumentError);
      s.grant(2);
      expect(s.window, TalkFrame.unlimitedWindow - 1);
      await pumpEventQueue();
      expect(m.window, TalkFrame.unlimitedWindow - 1);
      expect(p.a.isOpen, isTrue);
      expect(p.b.isOpen, isTrue);
      // The window values the request API takes.
      expect(
        () => p.a.streamRequest('X', Uint8List(0), window: -1),
        throwsRangeError,
      );
      expect(
        () => p.a.streamRequest(
          'X',
          Uint8List(0),
          window: TalkFrame.unlimitedWindow,
        ),
        throwsRangeError,
      );
      expect(p.a.outgoingRequestCount, 1);
      unawaited(m.reply(Uint8List(0)));
      await s.done;
      // After the end: a no-op, though the arguments are still checked.
      s.grant(1);
      expect(() => s.grant(0), throwsArgumentError);
      await pumpEventQueue();
      expect(p.credits, hasLength(1));
      await p.close();
    });
  });

  group('credits received', () {
    test('a credit for a request that is not outstanding is ignored', () async {
      final peer = RawPeer();
      final requests = StreamQueue(peer.talk.messages);
      peer.send(streamRequestFrame(1, 1));
      final m = await requests.next;
      await m.reply(Uint8List(0));
      // For the request just answered, and for one never seen.
      peer
        ..send(credit(1, 4))
        ..send(credit(9, 4));
      await pumpEventQueue();
      expect(peer.talk.isOpen, isTrue);
      // The channel still serves.
      peer.send(streamRequestFrame(2, 0));
      final n = await requests.next;
      expect(n.window, 0);
      await requests.cancel();
      await peer.talk.close();
    });

    for (final (label, setup, hex) in [
      ('a credit of 0', 3, '32 01 00 00 00 00 00 00'),
      (
        'a credit on an unlimited window',
        TalkFrame.unlimitedWindow,
        '32 01 00 00 01 00 00 00',
      ),
      (
        'a credit that brings the window to 0xFFFFFFFF',
        5,
        '32 01 00 00 FA FF FF FF',
      ),
      (
        'a credit that overflows u32 arithmetic',
        TalkFrame.unlimitedWindow - 1,
        '32 01 00 00 FF FF FF FF',
      ),
      (
        'a credit with an 8-byte payload',
        3,
        '32 01 00 00 A0 BB 0D 00 E0 93 04 00',
      ),
    ]) {
      test('$label closes the channel PROTOCOL_ERROR', () async {
        final peer = RawPeer();
        final requests = StreamQueue(peer.talk.messages);
        peer.send(streamRequestFrame(1, setup));
        final m = await requests.next;
        peer.sendHex(hex);
        expect((await peer.talk.done).known, StatusCode.protocolError);
        expect(m.isCancelled, isTrue);
        await requests.cancel();
      });
    }

    test('the largest credit that fits is no error', () async {
      final peer = RawPeer();
      final requests = StreamQueue(peer.talk.messages);
      peer.send(streamRequestFrame(1, 5));
      final m = await requests.next;
      peer.sendHex('32 01 00 00 F9 FF FF FF');
      await pumpEventQueue();
      expect(peer.talk.isOpen, isTrue);
      expect(m.window, TalkFrame.unlimitedWindow - 1);
      await requests.cancel();
      await peer.talk.close();
    });

    test('a credit for a request that is not a stream request closes the '
        'channel PROTOCOL_ERROR', () async {
      final peer = RawPeer();
      final requests = StreamQueue(peer.talk.messages);
      peer.send(
        TalkFrame(kind: TalkKind.message, procedure: Name('GET'), requestId: 1),
      );
      final m = await requests.next;
      expect(m.window, isNull);
      await m.whenCredit;
      peer.send(credit(1, 1));
      expect((await peer.talk.done).known, StatusCode.protocolError);
      await requests.cancel();
    });

    test('an item beyond the window is a channel protocol error on the '
        'responder', () async {
      final peer = RawPeer();
      final s = peer.talk.streamRequest('LIST', Uint8List(0), window: 1);
      final items = <int>[];
      final failed = expectLater(
        s.done,
        throwsA(isStatus(StatusCode.protocolError)),
      );
      s.items.listen((i) => items.add(i.payload.single), onError: (_) {});
      await pumpEventQueue();
      final id = peer.received.single.requestId;
      // The first item fills the window; the second exceeds it.
      peer
        ..send(itemFrame(id, 1))
        ..send(itemFrame(id, 2));
      await pumpEventQueue();
      expect(items, [1]);
      expect((await peer.talk.done).known, StatusCode.protocolError);
      await failed;
      await peer.talk.close();
    });

    test('the first item of a request opened with window 0 is a channel '
        'protocol error on the responder', () async {
      final peer = RawPeer();
      final s = peer.talk.streamRequest('LIST', Uint8List(0), window: 0);
      final failed = expectLater(
        s.done,
        throwsA(isStatus(StatusCode.protocolError)),
      );
      s.items.listen((_) {}, onError: (_) {});
      await pumpEventQueue();
      final id = peer.received.single.requestId;
      peer.send(itemFrame(id, 1));
      expect((await peer.talk.done).known, StatusCode.protocolError);
      await failed;
      await peer.talk.close();
    });
  });

  group('timeouts', () {
    const second = Duration(seconds: 1);

    test('the requester timeout stops when the last granted item arrives, '
        'and a grant restarts it', () {
      fakeAsync((async) {
        final peer = RawPeer(
          options: const TalkOptions(requestTimeout: Duration(seconds: 10)),
        );
        final s = peer.talk.streamRequest('LIST', Uint8List(0), window: 2);
        final done = Outcome(s.done);
        s.items.listen((_) {}, onError: (_) {});
        async.flushMicrotasks();
        final id = peer.received.single.requestId;
        expect(s.deadline, isNotNull);
        async.elapse(second * 5);
        peer.send(itemFrame(id, 1));
        async.flushMicrotasks();
        // An item that leaves room restarts the gap, as before.
        async.elapse(second * 9);
        expect(done.isDone, isFalse);
        peer.send(itemFrame(id, 2));
        async.flushMicrotasks();
        expect(s.window, 0);
        expect(s.deadline, isNull);
        async.elapse(const Duration(hours: 2));
        expect(done.isDone, isFalse, reason: 'no timeout while exhausted');
        // An EXTEND while exhausted changes nothing while it lasts.
        peer.send(TalkFrame(kind: TalkKind.extend, responseId: id));
        async.flushMicrotasks();
        async.elapse(const Duration(hours: 2));
        expect(done.isDone, isFalse);
        s.grant(1);
        async.flushMicrotasks();
        expect(peer.received.last.isCredit, isTrue);
        expect(s.deadline, isNotNull);
        async.elapse(second * 9);
        expect(done.isDone, isFalse);
        async.elapse(second);
        expect(done.error, isStatus(StatusCode.deadlineExceeded));
        expect(peer.received.last.kind, TalkKind.abort);
        peer.talk.close();
        async.flushMicrotasks();
      });
    });

    test('a request opened with window 0 starts no requester timeout until '
        'its first grant', () {
      fakeAsync((async) {
        final peer = RawPeer(
          options: const TalkOptions(requestTimeout: Duration(seconds: 10)),
        );
        final s = peer.talk.streamRequest('LIST', Uint8List(0), window: 0);
        final done = Outcome(s.done);
        async.flushMicrotasks();
        expect(s.deadline, isNull);
        async.elapse(const Duration(days: 3));
        expect(done.isDone, isFalse);
        expect(async.pendingTimers, isEmpty);
        s.grant(1);
        async.flushMicrotasks();
        async.elapse(second * 10);
        expect(done.error, isStatus(StatusCode.deadlineExceeded));
        peer.talk.close();
        async.flushMicrotasks();
      });
    });

    test('a cancel while exhausted keeps the id until the final, or the '
        'timeout', () {
      fakeAsync((async) {
        final peer = RawPeer(
          options: const TalkOptions(requestTimeout: Duration(seconds: 10)),
        );
        final s = peer.talk.streamRequest('LIST', Uint8List(0), window: 0);
        final done = Outcome(s.done);
        async.flushMicrotasks();
        s.cancel();
        async.flushMicrotasks();
        expect(done.error, isStatus(StatusCode.cancelled));
        expect(peer.received.last.kind, TalkKind.abort);
        expect(peer.received.where((f) => f.isCredit), isEmpty);
        expect(peer.talk.outgoingRequestCount, 1);
        async.elapse(second * 10);
        expect(peer.talk.outgoingRequestCount, 0);
        peer.talk.close();
        async.flushMicrotasks();
      });
    });

    test('the responder timeout stops when the last granted item goes, and '
        'a grant restarts it', () {
      fakeAsync((async) {
        final peer = RawPeer(
          options: const TalkOptions(replyTimeout: Duration(seconds: 10)),
        );
        TalkMessage? held;
        peer.talk.messages.listen((m) => held = m);
        peer.send(streamRequestFrame(1, 1));
        async.flushMicrotasks();
        held!.replyItem(bytes([1]));
        async.flushMicrotasks();
        expect(held!.window, 0);
        // Nothing waits for a grant: neither the responder timeout nor the
        // credit bound runs.
        async.elapse(const Duration(hours: 2));
        expect(peer.repliesTo(1).map((f) => f.kind), [TalkKind.streamItem]);
        expect(held!.canReply, isTrue);
        peer.send(credit(1, 1));
        async.flushMicrotasks();
        async.elapse(second * 9);
        expect(peer.repliesTo(1), hasLength(1));
        async.elapse(second);
        final abort = peer.repliesTo(1).last;
        expect(abort.kind, TalkKind.abort);
        expect(abort.status.known, StatusCode.deadlineExceeded);
        peer.talk.close();
        async.flushMicrotasks();
      });
    });

    test('a declared deadline that passes while the window is exhausted '
        'fires right after the grant, on the requester', () {
      fakeAsync((async) {
        final peer = RawPeer(
          options: const TalkOptions(requestTimeout: Duration(seconds: 10)),
        );
        final s = peer.talk.streamRequest('LIST', Uint8List(0), window: 1);
        final done = Outcome(s.done);
        s.items.listen((_) {}, onError: (_) {});
        async.flushMicrotasks();
        final id = peer.received.single.requestId;
        // The responder declares a deadline of 5 s.
        peer.send(
          TalkFrame(
            kind: TalkKind.extend,
            responseId: id,
            payload: TalkFrame.extendPayload(deadline: second * 5),
          ),
        );
        async.flushMicrotasks();
        expect(s.deadline, isNotNull);
        peer.send(itemFrame(id, 1));
        async.flushMicrotasks();
        // Exhausted: the suspension stops the clocks, the declared deadline
        // among them.
        expect(s.window, 0);
        expect(s.deadline, isNull);
        async.elapse(const Duration(hours: 1));
        expect(done.isDone, isFalse, reason: 'no timeout while exhausted');
        // The grant restarts the timeout; the deadline passed during the
        // wait, so it fires at once.
        s.grant(1);
        async.flushMicrotasks();
        async.elapse(const Duration(milliseconds: 1));
        expect(done.error, isStatus(StatusCode.deadlineExceeded));
        expect(peer.received.last.kind, TalkKind.abort);
        peer.talk.close();
        async.flushMicrotasks();
      });
    });

    test('a declared deadline that passes while the window is exhausted '
        'fires right after the credit, on the responder', () {
      fakeAsync((async) {
        final peer = RawPeer(
          options: const TalkOptions(replyTimeout: Duration(seconds: 10)),
        );
        TalkMessage? held;
        peer.talk.messages.listen((m) => held = m);
        peer.send(streamRequestFrame(1, 1));
        async.flushMicrotasks();
        final m = held!;
        unawaited(m.extend(deadline: second * 5));
        async.flushMicrotasks();
        m.replyItem(bytes([1]));
        async.flushMicrotasks();
        // Exhausted, and nothing waits for a grant: neither the declared
        // deadline nor the credit bound runs.
        expect(m.window, 0);
        async.elapse(const Duration(hours: 1));
        expect(peer.repliesTo(1).map((f) => f.kind), [
          TalkKind.extend,
          TalkKind.streamItem,
        ]);
        expect(m.canReply, isTrue);
        // The credit restarts the timeout; the deadline passed during the
        // wait, so it fires at once.
        peer.send(credit(1, 1));
        async.flushMicrotasks();
        async.elapse(const Duration(milliseconds: 1));
        final abort = peer.repliesTo(1).last;
        expect(abort.kind, TalkKind.abort);
        expect(abort.status.known, StatusCode.deadlineExceeded);
        peer.talk.close();
        async.flushMicrotasks();
      });
    });

    test('the credit bound aborts DEADLINE_EXCEEDED after the default, '
        'while an item waits', () {
      fakeAsync((async) {
        final peer = RawPeer();
        TalkMessage? held;
        peer.talk.messages.listen((m) => held = m);
        peer.send(streamRequestFrame(1, 1));
        async.flushMicrotasks();
        final m = held!;
        m.replyItem(bytes([1]));
        async.elapse(second * 30);
        // The bound starts when something waits for a grant.
        final waiting = Outcome(m.replyItem(bytes([2])));
        async.elapse(second * 59);
        expect(peer.repliesTo(1), hasLength(1));
        expect(m.isCancelled, isFalse);
        async.elapse(second);
        final abort = peer.repliesTo(1).last;
        expect(abort.kind, TalkKind.abort);
        expect(abort.status.known, StatusCode.deadlineExceeded);
        expect(waiting.error, isStatus(StatusCode.deadlineExceeded));
        expect(m.isCancelled, isTrue);
        expect(m.canReply, isFalse);
        expect(peer.talk.incomingRequestCount, 0);
        // A credit arriving afterwards is ignored.
        peer.send(credit(1, 1));
        async.flushMicrotasks();
        expect(peer.talk.isOpen, isTrue);
        expect(peer.repliesTo(1), hasLength(2));
        peer.talk.close();
        async.flushMicrotasks();
      });
    });

    test('the credit bound runs from the arrival of a request opened with '
        'window 0', () {
      fakeAsync((async) {
        final peer = RawPeer(
          options: const TalkOptions(creditTimeout: Duration(seconds: 3)),
        );
        TalkMessage? held;
        peer.talk.messages.listen((m) => held = m);
        peer.send(streamRequestFrame(1, 0));
        async.flushMicrotasks();
        expect(held, isNotNull);
        async.elapse(second * 2);
        expect(peer.repliesTo(1), isEmpty);
        async.elapse(second);
        expect(
          peer.repliesTo(1).single.status.known,
          StatusCode.deadlineExceeded,
        );
        expect(held!.isCancelled, isTrue);
        // A grant in time stops it, and starts the responder timeout,
        // which did not run before.
        peer.send(streamRequestFrame(2, 0));
        async.flushMicrotasks();
        async.elapse(second * 2);
        peer.send(credit(2, 1));
        async.flushMicrotasks();
        async.elapse(second * 9);
        expect(peer.repliesTo(2), isEmpty);
        async.elapse(second);
        expect(
          peer.repliesTo(2).single.status,
          Status.of(StatusCode.deadlineExceeded, 'no reply in time'),
        );
        peer.talk.close();
        async.flushMicrotasks();
      });
    });

    test('the credit bound per request: setCreditTimeout, replyStream and '
        'Duration.zero', () {
      fakeAsync((async) {
        final peer = RawPeer();
        final held = <TalkMessage>[];
        peer.talk.messages.listen(held.add);
        for (var id = 1; id <= 3; id++) {
          peer.send(streamRequestFrame(id, 0));
        }
        async.flushMicrotasks();
        held[0].setCreditTimeout(second * 5);
        held[1].replyStream(
          Stream.value(bytes([1])),
          creditTimeout: second * 7,
        );
        held[2].setCreditTimeout(Duration.zero);
        async.elapse(second * 5);
        expect(
          peer.repliesTo(1).single.status.known,
          StatusCode.deadlineExceeded,
        );
        expect(peer.repliesTo(2), isEmpty);
        async.elapse(second * 2);
        expect(
          peer.repliesTo(2).single.status.known,
          StatusCode.deadlineExceeded,
        );
        held[2].replyItem(bytes([1]));
        async.elapse(const Duration(days: 2));
        expect(peer.repliesTo(3), isEmpty);
        expect(held[2].canReply, isTrue);
        // Back to the channel default: it restarts from now.
        held[2].setCreditTimeout(null);
        async.elapse(second * 59);
        expect(peer.repliesTo(3), isEmpty);
        async.elapse(second);
        expect(
          peer.repliesTo(3).single.status.known,
          StatusCode.deadlineExceeded,
        );
        expect(async.pendingTimers, isEmpty);
        peer.talk.close();
        async.flushMicrotasks();
      });
    });

    test('waiting for whenCredit runs the credit bound', () {
      fakeAsync((async) {
        final peer = RawPeer(
          options: const TalkOptions(creditTimeout: Duration(seconds: 4)),
        );
        TalkMessage? held;
        peer.talk.messages.listen((m) => held = m);
        peer.send(streamRequestFrame(1, 1));
        async.flushMicrotasks();
        held!.replyItem(bytes([1]));
        async.elapse(const Duration(hours: 1));
        expect(peer.repliesTo(1), hasLength(1));
        final credit = Outcome(held!.whenCredit);
        async.elapse(second * 4);
        expect(credit.isDone, isTrue);
        expect(held!.isCancelled, isTrue);
        expect(
          peer.repliesTo(1).last.status.known,
          StatusCode.deadlineExceeded,
        );
        peer.talk.close();
        async.flushMicrotasks();
      });
    });

    test('a credit that raced the end of a request by its credit bound is '
        'ignored, and one after the peer reused the id is for the new '
        'request', () {
      fakeAsync((async) {
        final peer = RawPeer(
          options: const TalkOptions(creditTimeout: Duration(seconds: 3)),
        );
        final held = <TalkMessage>[];
        peer.talk.messages.listen(held.add);
        peer.send(streamRequestFrame(1, 0));
        async.flushMicrotasks();
        final old = Outcome(held.single.replyItem(bytes([1])));
        async.elapse(second * 3);
        expect(old.error, isStatus(StatusCode.deadlineExceeded));
        expect(
          peer.repliesTo(1).single.status.known,
          StatusCode.deadlineExceeded,
        );
        // Sent before the abort reached the peer: ignored.
        peer.send(credit(1, 1));
        async.flushMicrotasks();
        expect(peer.talk.isOpen, isTrue);
        expect(peer.repliesTo(1), hasLength(1));
        // The peer reuses the id, released by the abort; then reuses it
        // again while it is outstanding here (its own timeout released
        // it): the earlier one is abandoned, what waited for its window
        // fails, and credits go to the request that holds the id now.
        peer.send(streamRequestFrame(1, 0));
        async.flushMicrotasks();
        final reused = Outcome(held[1].replyItem(bytes([2])));
        peer.send(streamRequestFrame(1, 0));
        async.flushMicrotasks();
        expect(reused.error, isStatus(StatusCode.failedPrecondition));
        expect(held[1].isCancelled, isTrue);
        final current = Outcome(held[2].replyItem(bytes([3])));
        peer.send(credit(1, 1));
        async.flushMicrotasks();
        expect(current.isDone && current.error == null, isTrue);
        expect(
          [
            for (final f in peer.repliesTo(1))
              if (f.kind == TalkKind.streamItem) f.payload.single,
          ],
          [3],
        );
        expect(held[2].window, 0);
        peer.talk.close();
        async.flushMicrotasks();
      });
    });

    test('a long credit bound is armed in steps a JavaScript timer takes, '
        'and expires on time', () {
      fakeAsync((async) {
        // The longest delay a JavaScript timer takes, a signed 32-bit
        // number of milliseconds (about 24.8 days); a longer one fires at
        // once.
        const maxTimerDelay = Duration(milliseconds: 0x7FFFFFFF);
        void expectTimersInRange() => expect(
          async.pendingTimers.map((t) => t.duration),
          everyElement(lessThanOrEqualTo(maxTimerDelay)),
        );
        final peer = RawPeer(
          options: const TalkOptions(creditTimeout: Duration(days: 30)),
        );
        TalkMessage? held;
        peer.talk.messages.listen((m) => held = m);
        peer.send(streamRequestFrame(1, 0));
        async.flushMicrotasks();
        expect(held, isNotNull);
        expectTimersInRange();
        async.elapse(const Duration(days: 29));
        expectTimersInRange();
        expect(peer.repliesTo(1), isEmpty);
        async.elapse(const Duration(days: 1));
        expect(
          peer.repliesTo(1).single.status.known,
          StatusCode.deadlineExceeded,
        );
        expect(async.pendingTimers, isEmpty);
        peer.talk.close();
        async.flushMicrotasks();
      });
    });
  });

  group('the responder', () {
    test('window and whenCredit follow the grants', () async {
      final p = Pair();
      final served = StreamQueue(p.b.messages);
      final s = p.a.streamRequest('LIST', Uint8List(0), window: 1);
      final plain = p.a.request('GET', Uint8List(0));
      s.items.listen((_) {});
      final m = await served.next;
      final g = await served.next;
      expect(g.window, isNull);
      await g.whenCredit;
      expect(m.window, 1);
      await m.whenCredit;
      unawaited(m.replyItem(bytes([1])));
      expect(m.window, 0);
      final credit = Outcome(m.whenCredit);
      await pumpEventQueue();
      expect(credit.isDone, isFalse);
      s.grant(2);
      await pumpEventQueue();
      expect(credit.isDone, isTrue);
      expect(m.window, 2);
      // A grant used up at once by what waited does not complete it.
      unawaited(m.replyItem(bytes([2])));
      unawaited(m.replyItem(bytes([3])));
      unawaited(m.replyItem(bytes([4])));
      final next = Outcome(m.whenCredit);
      s.grant(1);
      await pumpEventQueue();
      expect(m.window, 0);
      expect(next.isDone, isFalse);
      // The end of the request completes it too.
      s.cancel();
      await pumpEventQueue();
      expect(next.isDone, isTrue);
      expect(next.error, isNull);
      expect(m.isCancelled, isTrue);
      await g.reply(Uint8List(0));
      await plain;
      await served.cancel();
      await p.close();
    });

    test('the future of replyItem waits at zero and completes on a '
        'grant', () async {
      final p = Pair();
      final served = nextRequest(p.b);
      final s = p.a.streamRequest('LIST', Uint8List(0), window: 0);
      s.items.listen((_) {});
      final m = await served;
      final sent = Outcome(m.replyItem(bytes([1])));
      final bulk = Outcome(
        m.replyItemBulk(Stream.value(bytes([1, 2, 3])), length: 3),
      );
      await pumpEventQueue();
      expect(sent.isDone, isFalse);
      expect(bulk.isDone, isFalse);
      s.grant(1);
      await pumpEventQueue();
      expect(sent.isDone, isTrue);
      expect(sent.error, isNull);
      // The bulk item waits for its own grant; over a channel that is not
      // a mux channel it then fails as a bulk payload does there.
      expect(bulk.isDone, isFalse);
      s.grant(1);
      await pumpEventQueue();
      expect(bulk.error, isStatus(StatusCode.unimplemented));
      expect(m.window, 1, reason: 'an item not sent gives its place back');
      unawaited(m.reply(Uint8List(0)));
      await s.done;
      await p.close();
    });

    test('replyStream pauses an async* source at its yield, and resumes it '
        'there on a grant', () async {
      final p = Pair();
      final served = nextRequest(p.b);
      final s = p.a.streamRequest('LIST', Uint8List(0), window: 2);
      final items = <int>[];
      s.items.listen((i) => items.add(i.payload.single));
      final log = <String>[];
      Stream<Uint8List> source() async* {
        for (var i = 0; i < 5; i++) {
          log.add('yield $i');
          yield bytes([i]);
          log.add('resumed $i');
        }
        log.add('end');
      }

      final m = await served;
      final answered = Outcome(m.replyStream(source(), trailer: bytes([9])));
      await pumpEventQueue();
      expect(items, [0, 1]);
      // Item 2 is read ahead and waits; the source is held at its yield.
      expect(log.last, 'yield 2');
      expect(log, isNot(contains('resumed 2')));
      s.grant(1);
      await pumpEventQueue();
      expect(items, [0, 1, 2]);
      expect(log.last, 'yield 3');
      s.grant(2);
      await pumpEventQueue();
      expect(items, [0, 1, 2, 3, 4]);
      // The end of the source is seen at zero: the trailer goes at once.
      expect(log.last, 'end');
      expect(m.window, 0);
      expect((await s.done).payload, [9]);
      expect(answered.isDone, isTrue);
      await p.close();
    });

    test('replyStream sees the end of its source at zero', () async {
      final p = Pair();
      final served = nextRequest(p.b);
      final s = p.a.streamRequest('LIST', Uint8List(0), window: 2);
      final m = await served;
      var ended = false;
      Stream<Uint8List> source() async* {
        yield bytes([1]);
        yield bytes([2]);
        ended = true;
      }

      unawaited(m.replyStream(source(), trailer: bytes([0xAA])));
      final last = await s.done;
      expect(ended, isTrue);
      expect(last.payload, [0xAA]);
      expect((await s.items.toList()).length, 2);
      expect(p.credits, isEmpty);
      await p.close();
    });

    test('a source that awaits whenCredit produces nothing it cannot '
        'deliver', () async {
      final p = Pair();
      final served = nextRequest(p.b);
      final s = p.a.streamRequest('LIST', Uint8List(0), window: 1);
      s.items.listen((_) {});
      final m = await served;
      var produced = 0;
      Stream<Uint8List> source() async* {
        for (var i = 0; i < 3; i++) {
          await m.whenCredit;
          if (m.isCancelled) {
            return;
          }
          produced++;
          yield bytes([i]);
        }
      }

      unawaited(m.replyStream(source()));
      await pumpEventQueue();
      expect(produced, 1);
      s.grant(1);
      await pumpEventQueue();
      expect(produced, 2);
      s.grant(1);
      await s.done;
      expect(produced, 3);
      await p.close();
    });

    test('cancel while exhausted, without a grant', () async {
      final p = Pair();
      final served = nextRequest(p.b);
      final s = p.a.streamRequest('LIST', Uint8List(0), window: 1);
      final m = await served;
      var cancelled = false;
      final source = StreamController<Uint8List>(
        onCancel: () => cancelled = true,
      );
      unawaited(m.replyStream(source.stream));
      source
        ..add(bytes([1]))
        ..add(bytes([2]))
        ..add(bytes([3]));
      final items = <int>[];
      s.items.listen((i) => items.add(i.payload.single), onError: (_) {});
      await pumpEventQueue();
      expect(items, [1]);
      expect(s.window, 0);
      expect(p.items, hasLength(1));
      s.cancel();
      await expectLater(s.done, throwsA(isStatus(StatusCode.cancelled)));
      await pumpEventQueue();
      expect(cancelled, isTrue);
      expect(m.isCancelled, isTrue);
      expect(p.credits, isEmpty);
      // What waited for credit never went.
      expect(p.items, hasLength(1));
      expect(p.a.outgoingRequestCount, 0);
      expect(p.b.incomingRequestCount, 0);
      await source.close();
      await p.close();
    });

    test('a cancel while items and a chained final wait for the window is '
        'answered ABORT CANCELLED, the only final; nothing that waited '
        'goes', () async {
      final peer = RawPeer();
      final requests = StreamQueue(peer.talk.messages);
      peer.send(streamRequestFrame(1, 0));
      final m = await requests.next;
      final item = Outcome(m.replyItem(bytes([1])));
      final chained = m.replyStreamRequest(bytes([2]), window: 3);
      final chainedDone = Outcome(chained.done);
      expect(m.canReply, isFalse);
      expect(peer.talk.outgoingRequestCount, 1);
      peer.send(
        TalkFrame(
          kind: TalkKind.abort,
          requestId: 1,
          payload: Status.of(StatusCode.cancelled).encode(),
        ),
      );
      await pumpEventQueue();
      expect(peer.repliesTo(1).single.kind, TalkKind.abort);
      expect(peer.repliesTo(1).single.status.known, StatusCode.cancelled);
      expect(item.error, isStatus(StatusCode.cancelled));
      expect(chainedDone.error, isStatus(StatusCode.cancelled));
      expect(m.isCancelled, isTrue);
      // The chained request never went: its id is free, and a grant for it
      // sends nothing.
      expect(peer.talk.outgoingRequestCount, 0);
      chained.grant(1);
      // A credit for the cancelled request, sent before the peer saw the
      // abort, is ignored.
      peer.send(credit(1, 2));
      await pumpEventQueue();
      expect(peer.received, hasLength(1));
      expect(peer.talk.isOpen, isTrue);
      await requests.cancel();
      await peer.talk.close();
    });

    test('a listener that throws after its final reply, waiting behind '
        'items for the window, has answered', () async {
      final peer = RawPeer();
      late TalkMessage m;
      final sent = <Outcome<void>>[];
      peer.talk.messages.listen((message) {
        m = message;
        sent
          ..add(Outcome(message.replyItem(bytes([1]))))
          ..add(Outcome(message.reply(bytes([2]))));
        throw StateError('after the answer');
      });
      peer.send(streamRequestFrame(1, 0));
      await pumpEventQueue();
      // Nothing went, and no ABORT INTERNAL either: the request is
      // answered, its final waiting behind the item.
      expect(peer.received, isEmpty);
      expect(m.canReply, isFalse);
      expect(m.isCancelled, isFalse);
      peer.send(credit(1, 1));
      await pumpEventQueue();
      expect(
        [for (final f in peer.repliesTo(1)) f.kind],
        [TalkKind.streamItem, TalkKind.message],
      );
      expect(peer.repliesTo(1).last.payload, [2]);
      expect(sent.every((o) => o.isDone && o.error == null), isTrue);
      expect(peer.talk.incomingRequestCount, 0);
      await peer.talk.close();
    });

    test('the end of the channel while exhausted fails the request with '
        'its status on both sides', () async {
      // The requester.
      final requester = RawPeer();
      final s = requester.talk.streamRequest('LIST', Uint8List(0), window: 0);
      final failed = expectLater(
        s.done,
        throwsA(isStatus(StatusCode.unavailable)),
      );
      await pumpEventQueue();
      requester.end(Status.of(StatusCode.unavailable, 'gone'));
      await failed;
      // The responder, with an item waiting for a grant.
      final responder = RawPeer();
      final requests = StreamQueue(responder.talk.messages);
      responder.send(streamRequestFrame(1, 0));
      final m = await requests.next;
      final waiting = Outcome(m.replyItem(bytes([1])));
      final credit = Outcome(m.whenCredit);
      responder.end(Status.of(StatusCode.unavailable, 'gone'));
      await pumpEventQueue();
      expect(waiting.error, isStatus(StatusCode.unavailable));
      expect(credit.isDone, isTrue);
      expect(m.isCancelled, isTrue);
      expect(responder.repliesTo(1), isEmpty);
      await requests.cancel();
    });

    test('over a synchronous transport, a cancel sent as the first granted '
        'item arrives is answered after the items granted, and drops the '
        'rest', () async {
      final c = StreamChannelController<Uint8List>(sync: true);
      final talk = TalkChannel(c.local);
      final sent = <TalkFrame>[];
      c.foreign.stream.listen((data) {
        final frame = TalkFrame.decode(data);
        sent.add(frame);
        if (frame.kind == TalkKind.streamItem && sent.length == 1) {
          // Delivered once the credit that let the item go was handled:
          // a subscription does not re-enter its listener.
          c.foreign.sink.add(
            TalkFrame(
              kind: TalkKind.abort,
              requestId: 1,
              payload: Status.of(StatusCode.cancelled).encode(),
            ).encode(),
          );
        }
      });
      final held = Completer<TalkMessage>();
      talk.messages.listen(held.complete);
      c.foreign.sink.add(streamRequestFrame(1, 0).encode());
      final m = await held.future;
      final replies = [
        for (var i = 0; i < 3; i++) Outcome(m.replyItem(bytes([i]))),
      ];
      c.foreign.sink.add(credit(1, 2).encode());
      await pumpEventQueue();
      expect(sent.map((f) => f.kind), [
        TalkKind.streamItem,
        TalkKind.streamItem,
        TalkKind.abort,
      ]);
      expect(sent.last.status.known, StatusCode.cancelled);
      expect(replies[0].isDone && replies[0].error == null, isTrue);
      expect(replies[1].isDone && replies[1].error == null, isTrue);
      expect(replies[2].error, isStatus(StatusCode.cancelled));
      expect(m.isCancelled, isTrue);
      expect(talk.incomingRequestCount, 0);
      await talk.close();
    });

    test('a bulk item counts as one item', () async {
      final (client, server) = muxPair();
      final accepted = StreamController<TalkChannel>();
      server.incoming.listen((channel) {
        if (TalkBulkOpen.isBulk(ChannelAddress.decode(channel.openPayload))) {
          TalkChannel.adoptBulk(channel);
        } else {
          accepted.add(TalkChannel(channel));
        }
      });
      client.incoming.listen(TalkChannel.adoptBulk);
      final talk = TalkChannel(client.open(Uint8List(0)));
      final responder = await accepted.stream.first;
      final served = nextRequest(responder);
      final s = talk.streamRequest('GET', Uint8List(0), window: 2);
      final m = await served;
      final lengths = <int>[];
      s.items.listen((i) => lengths.add(i.payload.length));
      // A bulk payload of the reply API, and an item too large inline,
      // which goes bulk all the same: one item each.
      final first = Outcome(
        m.replyItemBulk(Stream.value(Uint8List(100000)), length: 100000),
      );
      final second = Outcome(m.replyItem(Uint8List(60000)));
      final third = Outcome(m.replyItem(bytes([3])));
      expect(m.window, 0);
      await until(() => lengths.length == 2);
      expect(lengths, [100000, 60000]);
      expect(first.error, isNull);
      expect(second.error, isNull);
      expect(third.isDone, isFalse);
      expect(s.window, 0);
      s.grant(1);
      await until(() => lengths.length == 3);
      expect(lengths.last, 1);
      expect(third.isDone, isTrue);
      unawaited(m.reply(Uint8List(0)));
      await s.done;
      await talk.close();
      await responder.done;
      await accepted.close();
      await client.close();
      await server.close();
    });
  });
}
