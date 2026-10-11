/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Opus 5.5 <noreply@anthropic.com>
  GPT-6 Astra <noreply@openai.com>
*/

// The binding seam: the node, Talk (bulk payloads, whenWritable), identity,
// the proxy, slot channels and the reconnecting client over connections
// that are not the frame mux ([FakeBinding], a stream-shaped test double),
// the binding registry and Switchboard.adopt.

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:switchboard/switchboard.dart';
import 'package:test/test.dart';

import 'fake_binding.dart';

const fast = MuxOptions(
  goAwayGrace: Duration(milliseconds: 100),
  keepAliveInterval: null,
);
const limit = Duration(seconds: 5);

final echo = Name('echo');
final npc = Name('npc');

final authoritySeed = Uint8List.fromList(List.generate(32, (i) => 3 * i));

late CredentialIssuer authority;
late HolderKey npcKey;

Uint8List bytes(String value) => Uint8List.fromList(utf8.encode(value));
String text(Uint8List data) => utf8.decode(data);

/// [length] bytes of a pattern a corrupted transfer would not keep.
Uint8List pattern(int length) =>
    Uint8List.fromList(List.generate(length, (i) => (i * 7 + i ~/ 251) & 0xFF));

Matcher throwsCode(StatusCode code) =>
    throwsA(isA<SwitchboardException>().having((e) => e.code, 'code', code));

Matcher hasCode(StatusCode code) =>
    isA<Status>().having((s) => s.known, 'known', code);

Future<void> until(bool Function() condition) async {
  final deadline = DateTime.now().add(limit);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('condition not met within $limit');
    }
    await Future<void>.delayed(const Duration(milliseconds: 2));
  }
}

/// Lets the timers that deliver the fake wire's records run.
Future<void> settle() => Future<void>.delayed(const Duration(milliseconds: 20));

/// A node closed after the test.
Switchboard node({
  List<SwitchboardBinding> bindings = const [],
  Credential? credential,
  HolderKey? holderKey,
  CredentialVerifier? verifier,
  bool identifyOutgoing = true,
  Duration identityTimeout = limit,
  Resolver? resolver,
  EndpointPolicy? endpointPolicy,
  Duration connectTimeout = const Duration(seconds: 10),
}) {
  final s = Switchboard(
    muxOptions: fast,
    bindings: bindings,
    credential: credential,
    holderKey: holderKey,
    verifier: verifier,
    identifyOutgoing: identifyOutgoing,
    identityTimeout: identityTimeout,
    resolver: resolver,
    endpointPolicy: endpointPolicy,
    connectTimeout: connectTimeout,
  );
  addTearDown(s.close);
  return s;
}

/// A fake listener at `fake://[name]` for [server], stopped after the test.
Uri listen(Switchboard server, String name, {ChannelPolicy? policy}) {
  addTearDown(() => FakeBinding.stop(name));
  return FakeBinding.listen(server, name, policy: policy);
}

/// A Talk service answering every request with its payload.
void echoService(IncomingChannel incoming) {
  incoming.talk().messages.listen((m) {
    if (m.expectsReply) {
      unawaited(m.reply(m.payload));
    }
  });
}

/// A handler answering with the peer's identity and closing.
void answer(IncomingChannel incoming) {
  unawaited(
    incoming.channel.send(
      bytes(
        '${incoming.address.type} for '
        '${incoming.peerIdentity?.identity ?? 'nobody'}',
      ),
    ),
  );
  unawaited(incoming.channel.close());
}

/// What an [answer] handler said, or the close status.
Future<String> answerOf(MuxChannel channel) async {
  final frames = await channel.stream.toList().timeout(limit);
  final status = await channel.done.timeout(limit);
  if (frames.isEmpty) {
    return 'closed ${status.known?.name ?? status.code}';
  }
  return text(frames.single);
}

/// Talk on both ends of a fake connection pair, the bulk channels of each
/// side routed to its Talk layer as a node routes them.
Future<(TalkChannel, TalkChannel, FakeConnection, FakeConnection)>
talkPair() async {
  final (a, b) = FakeConnection.pair(initiator: fast, acceptor: fast);
  addTearDown(a.close);
  addTearDown(b.close);
  final server = Completer<TalkChannel>();
  void route(MuxChannel channel) {
    final address = ChannelAddress.decode(channel.openPayload);
    if (TalkBulkOpen.isBulk(address)) {
      TalkChannel.adoptBulk(channel);
    } else if (!server.isCompleted) {
      server.complete(TalkChannel(channel));
    }
  }

  a.incoming.listen(route);
  b.incoming.listen(route);
  final client = TalkChannel(a.open(ChannelAddress(type: echo).encode()));
  return (client, await server.future.timeout(limit), a, b);
}

/// A binding whose dials end when the test says so.
class _HeldBinding extends SwitchboardBinding {
  final List<Completer<MuxConnection>> attempts = [];

  @override
  Set<String> get schemes => const {'held'};

  @override
  Future<MuxConnection> dial(Uri endpoint, BindingDialSettings settings) {
    final attempt = Completer<MuxConnection>();
    attempts.add(attempt);
    return attempt.future;
  }
}

/// A binding claiming [schemes] and dialling nothing.
class _Schemes extends SwitchboardBinding {
  _Schemes(this.schemes);

  @override
  final Set<String> schemes;

  @override
  Future<MuxConnection> dial(Uri endpoint, BindingDialSettings settings) =>
      Future.error(StateError('not dialled in this test'));
}

void main() {
  setUpAll(() async {
    authority = await CredentialIssuer.ed25519FromSeed(
      Name('auth1'),
      authoritySeed,
    );
    npcKey = await HolderKey.generate();
  });

  group('contract', () {
    test('the frame mux charges a subframe its length plus 16, a stream '
        'binding its length plus record overhead', () {
      final (a, b) = MemoryTransport.pair();
      final frame = MuxConnection(a, isInitiator: true, options: fast);
      final peer = MuxConnection(b, isInitiator: false, options: fast);
      addTearDown(frame.close);
      addTearDown(peer.close);
      final framed = frame.open(Uint8List(0));
      expect(framed, isA<FrameMuxChannel>());
      expect(framed.sendCost(10), 26);
      final (x, y) = FakeConnection.pair(initiator: fast, acceptor: fast);
      addTearDown(x.close);
      addTearDown(y.close);
      final streamed = x.open(Uint8List(0));
      expect(streamed, isNot(isA<FrameMuxChannel>()));
      expect(streamed.sendCost(10), 11);
      // A binding's connection answers the frame mux's own diagnostics.
      expect(x.peerLimits, isNull);
      expect(x.heldOutputBytes, 0);
      expect(x.unconfirmedCloseCount, 0);
      expect(() => x.nextChannelIdForTesting = 8, throwsUnsupportedError);
    });

    test(
      'channel ids are the same at both ends, with the mux parity',
      () async {
        final (a, b) = FakeConnection.pair(initiator: fast, acceptor: fast);
        addTearDown(a.close);
        addTearDown(b.close);
        final atB = <MuxChannel>[];
        final atA = <MuxChannel>[];
        b.incoming.listen(atB.add);
        a.incoming.listen(atA.add);
        final first = a.open(bytes('one'));
        final second = a.open(bytes('two'));
        final back = b.open(bytes('back'));
        await until(() => atB.length == 2 && atA.length == 1);
        expect([first.id, second.id], [atB[0].id, atB[1].id]);
        expect(first.id.isEven && second.id.isEven, isTrue);
        expect(back.id.isOdd, isTrue);
        expect(atA.single.id, back.id);
        expect(b.channelWithId(first.id), same(atB[0]));
        expect(text(atB[1].openPayload), 'two');
      },
    );
  });

  group('a node over a binding', () {
    test(
      'dials, pools by the normalised endpoint, dispatches and closes',
      () async {
        final server = node();
        server.registerService(echo, echoService);
        final uri = listen(server, 'echo-1');
        final accepted = server.connections.first;
        final binding = FakeBinding();
        final asked = <Uri>[];
        final client = node(
          bindings: [binding],
          endpointPolicy: (endpoint) {
            asked.add(endpoint);
            return null;
          },
        );
        expect(client.bindingSchemes, {'fake'});
        final talk = await client.openTalkAt(uri, ChannelAddress(type: echo));
        final reply = await talk.request('ECHO', bytes('hello')).timeout(limit);
        expect(text(reply.payload), 'hello');
        // The channel is the binding's, not the frame mux's.
        expect(talk.raw, isA<FakeChannel>());
        final connection = binding.dialled.single;
        expect(connection.sent['data'], greaterThan(0));
        expect(asked, [Uri.parse('fake://echo-1')]);
        // Another spelling of the endpoint shares the pooled connection.
        final again = await client.openTalkAt(
          Uri.parse('FAKE://Echo-1#echo'),
          ChannelAddress(type: echo),
        );
        expect(
          text((await again.request('ECHO', bytes('again'))).payload),
          'again',
        );
        expect(binding.dialled, hasLength(1));
        expect(identical(await client.connect(uri), connection), isTrue);
        // The accepted side was adopted by the server, which dispatched.
        final atServer = await accepted.timeout(limit);
        expect(atServer, isA<FakeConnection>());
        expect(atServer.isInitiator, isFalse);
        // Closing the client sends the server's side GOAWAY.
        await client.close();
        expect(
          await atServer.done.timeout(limit),
          hasCode(StatusCode.goingAway),
        );
      },
    );

    test('the open methods resolve to a binding endpoint', () async {
      final server = node();
      server.registerService(echo, echoService, instance: 1);
      final uri = listen(server, 'echo-2');
      final resolver = StaticResolver([
        ServiceRecord(ServiceAddress(echo, 1), endpoints: [uri]),
      ]);
      addTearDown(resolver.close);
      final client = node(bindings: [FakeBinding()], resolver: resolver);
      final talk = await client.openTalk(ServiceAddress(echo));
      final reply = await talk.request('ECHO', bytes('found')).timeout(limit);
      expect(text(reply.payload), 'found');
    });

    test('a binding that cannot reach its endpoint, or not in time', () async {
      final held = _HeldBinding();
      final client = node(
        bindings: [FakeBinding(), held],
        connectTimeout: const Duration(milliseconds: 50),
      );
      await expectLater(
        client.connect(Uri.parse('fake://nobody')),
        throwsCode(StatusCode.unavailable),
      );
      await expectLater(
        client.connect(Uri.parse('held://slow')),
        throwsCode(StatusCode.unavailable),
      );
      // The connection that arrives after the node gave up is closed.
      final (late, peer) = FakeConnection.pair(initiator: fast, acceptor: fast);
      addTearDown(peer.close);
      held.attempts.single.complete(late);
      expect(
        await late.done.timeout(limit),
        hasCode(StatusCode.connectionLost),
      );
      // A scheme no binding claims is still unimplemented.
      await expectLater(
        client.connect(Uri.parse('quic://host:4433')),
        throwsCode(StatusCode.unimplemented),
      );
    });

    test('registerBinding refuses built-in, malformed and claimed schemes', () {
      final s = node();
      for (final scheme in ['ws', 'wss', 'tcp', 'mem', 'Fake', '1x']) {
        expect(
          () => s.registerBinding(_Schemes({scheme})),
          throwsArgumentError,
          reason: scheme,
        );
      }
      // Nothing of a refused binding is registered.
      expect(
        () => s.registerBinding(_Schemes({'good', 'tcp'})),
        throwsArgumentError,
      );
      expect(s.bindingSchemes, isEmpty);
      s.registerBinding(FakeBinding());
      expect(() => s.registerBinding(FakeBinding()), throwsArgumentError);
      expect(s.bindingSchemes, {'fake'});
      expect(
        () => Switchboard(
          bindings: [
            _Schemes({'tcp'}),
          ],
        ),
        throwsArgumentError,
      );
    });

    test('adopt takes a connection of any binding once, and refuses it '
        'while closing', () async {
      final server = node();
      server.registerService(echo, echoService);
      final (a, b) = FakeConnection.pair(initiator: fast, acceptor: fast);
      addTearDown(a.close);
      expect(await server.adopt(b, remote: 'adopted'), same(b));
      await expectLater(server.adopt(b), throwsArgumentError);
      final talk = TalkChannel(a.open(ChannelAddress(type: echo).encode()));
      expect(
        text((await talk.request('ECHO', bytes('x')).timeout(limit)).payload),
        'x',
      );
      // A connection whose channels something else reads.
      final (c, d) = FakeConnection.pair(initiator: fast, acceptor: fast);
      addTearDown(c.close);
      addTearDown(d.close);
      d.incoming.listen((_) {});
      await expectLater(server.adopt(d), throwsStateError);
      await server.close();
      expect(await a.done.timeout(limit), hasCode(StatusCode.goingAway));
      final (e, f) = FakeConnection.pair(initiator: fast, acceptor: fast);
      addTearDown(e.close);
      await expectLater(
        server.adopt(f),
        throwsCode(StatusCode.failedPrecondition),
      );
      expect(
        await e.peerGoAwayStatus.timeout(limit),
        hasCode(StatusCode.goingAway),
      );
    });
  });

  group('Talk over a binding', () {
    test('bulk payloads go on channels the binding opens after their '
        'parent', () async {
      final (client, server, a, b) = await talkPair();
      server.messages.listen((m) => unawaited(m.reply(m.payload)));
      final payload = pattern(300 * 1000);
      expect(
        payload.length,
        greaterThan((client.raw as MuxChannel).maxSubframeLength),
      );
      final reply = await client.request('BIG', payload).timeout(limit);
      expect(reply.isBulk, isTrue);
      expect(reply.payload, payload);
      // Each way: the channel and one bulk channel opened after it.
      expect(a.sent['open'], 2);
      expect(b.sent['open'], 1);
      // The bulk channels ended with their transfers.
      await until(() => a.openChannelCount == 1 && b.openChannelCount == 1);
      // A streamed bulk payload from the client.
      final streamed = await client
          .request(
            'STREAM',
            Uint8List(0),
            bulk: Stream.fromIterable([pattern(70000), pattern(90000)]),
          )
          .timeout(limit);
      expect(streamed.payload, [...pattern(70000), ...pattern(90000)]);
    });

    test('whenWritable paces a publisher at the binding\'s window', () async {
      final (client, server, _, _) = await talkPair();
      final raw = client.raw as MuxChannel;
      final serverRaw = server.raw as MuxChannel;
      final received = <TalkMessage>[];
      final subscription = server.messages.listen(received.add)..pause();
      const count = 300;
      var sent = 0;
      final publisher = () async {
        for (var i = 0; i < count; i++) {
          await client.whenWritable;
          unawaited(client.send('EVENT', Uint8List(1000)));
          sent++;
        }
      }();
      await settle();
      // Writable while the window has room for the largest subframe: the
      // publisher stopped with less than that left.
      expect(client.isWritable, isFalse);
      expect(raw.sendWindow, lessThan(raw.maxSubframeLength));
      expect(sent, lessThan(70));
      expect(serverRaw.heldBytes, lessThanOrEqualTo(fast.initialWindow));
      await settle();
      expect(sent, lessThan(70));
      subscription.resume();
      await publisher.timeout(limit);
      await until(() => received.length == count);
      await subscription.cancel();
      await client.close();
      await expectLater(
        client.whenWritable,
        throwsCode(StatusCode.failedPrecondition),
      );
    });
  });

  group('identity over a binding', () {
    Future<CredentialVerifier> verifier() async {
      final v = CredentialVerifier();
      await v.addIssuer(authority);
      return v;
    }

    Future<Credential> npcCredential() => authority.issue(
      kind: CredentialKind.node,
      identity: 'npc-7',
      scopes: [Scope.of(Right.open, 'npc')],
      holderKey: npcKey.publicKey,
    );

    test('a node identifies when it dials, and the policy sees it', () async {
      final server = node(
        verifier: await verifier(),
        identityTimeout: const Duration(milliseconds: 200),
      );
      server.registerService(npc, answer);
      final uri = listen(server, 'id-1', policy: ChannelPolicies.scoped());
      final client = node(
        bindings: [FakeBinding()],
        credential: await npcCredential(),
        holderKey: npcKey,
      );
      expect(
        await answerOf(
          await client.openChannelAt(uri, ChannelAddress(type: npc)),
        ),
        'npc for npc-7',
      );
      final connection = await client.connect(uri);
      expect((connection as FakeConnection).sent['_Ident'], 1);
      // A node without a credential is refused once the hold expires.
      final anonymous = node(bindings: [FakeBinding()]);
      expect(
        await answerOf(
          await anonymous.openChannelAt(uri, ChannelAddress(type: npc)),
        ),
        'closed unauthenticated',
      );
    });

    test('a channel that overtakes the IDENT is held until the peer '
        'identifies', () async {
      var asked = 0;
      final scoped = ChannelPolicies.scoped();
      final server = node(verifier: await verifier());
      server.registerService(npc, answer);
      final uri = listen(
        server,
        'id-2',
        policy: (address, connection) {
          asked++;
          return scoped(address, connection);
        },
      );
      // Control records arrive well after the channels.
      final client = node(
        bindings: [FakeBinding(controlDelay: const Duration(milliseconds: 50))],
        credential: await npcCredential(),
        holderKey: npcKey,
        identifyOutgoing: false,
      );
      final connection = await client.connect(uri);
      final identified = client.identifyOn(connection);
      final channel = client.openChannelOn(
        connection,
        ChannelAddress(type: npc),
      );
      expect(await answerOf(channel), 'npc for npc-7');
      await identified.timeout(limit);
      // Refused while the peer had not identified, admitted once it had.
      expect(asked, 2);
    });

    test('a credential the peer refuses fails the identification', () async {
      final rogue = await CredentialIssuer.ed25519FromSeed(
        Name('auth1'),
        Uint8List.fromList(List.generate(32, (i) => 5 * i + 1)),
      );
      final server = node(verifier: await verifier());
      server.registerService(npc, answer);
      final uri = listen(server, 'id-3');
      final client = node(
        bindings: [FakeBinding()],
        credential: await rogue.issue(
          kind: CredentialKind.node,
          identity: 'npc-7',
          scopes: [Scope.of(Right.open, 'npc')],
          holderKey: npcKey.publicKey,
        ),
        holderKey: npcKey,
      );
      await expectLater(
        client.connect(uri),
        throwsCode(StatusCode.unauthenticated),
      );
    });
  });

  test('the proxy forwards between the frame mux and a binding, bulk '
      'payloads included', () async {
    final backend = node();
    backend.registerService(echo, echoService, instance: 1);
    final backendUri = listen(backend, 'backend-1');
    final resolver = StaticResolver([
      ServiceRecord(ServiceAddress(echo, 1), endpoints: [backendUri]),
    ]);
    addTearDown(resolver.close);
    final binding = FakeBinding();
    final proxy = node(bindings: [binding], resolver: resolver);
    proxy.catchAll = proxyHandler(proxy);
    final proxyUri = await proxy.listenMemory();
    final client = node();
    final talk = await client.openTalkAt(proxyUri, ChannelAddress(type: echo));
    expect(talk.raw, isA<FrameMuxChannel>());
    final small = await talk.request('ECHO', bytes('through')).timeout(limit);
    expect(text(small.payload), 'through');
    final payload = pattern(200 * 1000);
    final big = await talk.request('BIG', payload).timeout(limit);
    expect(big.isBulk, isTrue);
    expect(big.payload, payload);
    // The proxy opened the channel and a bulk twin on the binding.
    expect(binding.dialled.single.sent['open'], 2);
  });

  test('a slot channel holds what it sends while it reopens at its '
      'channel\'s sendCost', () async {
    final (a, b) = FakeConnection.pair(initiator: fast, acceptor: fast);
    addTearDown(a.close);
    addTearDown(b.close);
    b.incoming.listen(
      (channel) => unawaited(channel.close(Status.of(StatusCode.moved))),
    );
    final first = a.open(ChannelAddress(type: echo).encode());
    final reopened = Completer<MuxChannel?>();
    addTearDown(() {
      if (!reopened.isCompleted) {
        reopened.complete(null);
      }
    });
    final slot = SlotChannel(
      echo,
      3,
      first,
      (_) => reopened.future,
      maxHeldBytes: 100,
    );
    expect(await first.done.timeout(limit), hasCode(StatusCode.moved));
    // Two subframes of 40 bytes fit 100 bytes at the binding's cost of 41 each (the
    // frame mux's, 56 each, would not); a third does not.
    final held = [slot.send(Uint8List(40)), slot.send(Uint8List(40))];
    await expectLater(
      slot.send(Uint8List(40)),
      throwsCode(StatusCode.resourceExhausted),
    );
    expect(
      await slot.done.timeout(limit),
      hasCode(StatusCode.resourceExhausted),
    );
    for (final future in held) {
      await expectLater(future, throwsCode(StatusCode.failedPrecondition));
    }
  });

  test('a reconnecting client over a binding reconnects and reopens its '
      'channels', () async {
    final server = node();
    server.registerService(echo, echoService);
    final accepted = <FakeConnection>[];
    final client = ReconnectingClient.connections(
      (options) async {
        final (local, remote) = FakeConnection.pair(
          initiator: options,
          acceptor: server.muxOptions,
        );
        accepted.add(remote);
        await server.adopt(remote);
        return local;
      },
      muxOptions: fast,
      initialBackoff: const Duration(milliseconds: 10),
      jitter: 0,
    );
    addTearDown(client.close);
    final ready = <TalkChannel>[];
    client.openPersistentTalk(
      ChannelAddress(type: echo),
      (talk) async => ready.add(talk),
    );
    await until(() => ready.length == 1);
    expect(ready.single.raw, isA<FakeChannel>());
    expect(
      text((await ready.single.request('ECHO', bytes('one'))).payload),
      'one',
    );
    // The connection is lost: the client connects again and reopens.
    await accepted.single.close();
    await until(() => ready.length == 2);
    expect(accepted, hasLength(2));
    expect(
      text((await ready.last.request('ECHO', bytes('two'))).payload),
      'two',
    );
  });
}
