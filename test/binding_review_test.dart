/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  GPT-6 Astra <noreply@openai.com>
*/

import 'dart:async';
import 'dart:typed_data';

import 'package:fake_async/fake_async.dart';
import 'package:switchboard/switchboard.dart';
import 'package:test/test.dart';

import 'binding_seam_test.dart' as h;
import 'inproc_binding.dart';

(InProcessConnection, InProcessConnection) pair({
  MuxOptions? options,
  Duration Function(int)? delay,
}) {
  final (a, b) = InProcessConnection.pair(
    initiator: options ?? h.fast,
    acceptor: options ?? h.fast,
    channelDelay: delay,
  );
  addTearDown(a.close);
  addTearDown(b.close);
  return (a, b);
}

class DialBinding extends SwitchboardBinding {
  DialBinding(this.connect);
  final Future<MuxConnection> Function(Uri, BindingDialSettings) connect;

  @override
  Set<String> get schemes => const {'review'};

  @override
  Uri normalise(Uri endpoint) => endpoint.replace(fragment: '').normalizePath();

  @override
  Future<MuxConnection> dial(Uri endpoint, BindingDialSettings settings) =>
      connect(endpoint, settings);
}

void main() {
  group('independent channel streams', () {
    test('bulk opens waiting for unknown parents expire', () {
      fakeAsync((clock) {
        final (a, b) = InProcessConnection.pair(
          initiator: h.fast,
          acceptor: h.fast,
        );
        b.incoming.listen(TalkChannel.adoptBulk);
        final child = a.open(TalkBulkOpen(1000, 1).encode());
        Status? ended;
        unawaited(child.done.then((status) => ended = status));
        clock.elapse(Duration.zero);
        expect(ended, isNull);
        clock.elapse(const Duration(seconds: 10));
        expect(ended, h.hasCode(StatusCode.failedPrecondition));
        unawaited(a.close());
        unawaited(b.close());
        clock.elapse(Duration.zero);
      });
    });

    test(
      'pending proxy bulk opens count against its forwarding bound',
      () async {
        final backend = h.node();
        final delivered = <IncomingChannel>[];
        backend.registerService(h.echo, delivered.add, instance: 1);
        final backendUri = await backend.listenMemory();
        final resolver = StaticResolver([
          ServiceRecord(ServiceAddress(h.echo, 1), endpoints: [backendUri]),
        ]);
        addTearDown(resolver.close);
        final proxy = h.node(resolver: resolver);
        proxy.catchAll = proxyHandler(proxy, maxChannelsPerConnection: 2);
        final endpoint = h.listen(proxy, 'review-proxy-bound');
        final client = h.node(bindings: [InProcessBinding()]);
        final parent = await client.openChannelAt(
          endpoint,
          ChannelAddress(type: h.echo),
        );
        await h.until(() => delivered.length == 1);
        final first = parent.openAfter(TalkBulkOpen(parent.id, 1).encode());
        final second = parent.openAfter(TalkBulkOpen(parent.id, 2).encode());
        expect(
          await second.done.timeout(h.limit),
          h.hasCode(StatusCode.resourceExhausted),
        );
        expect(first.state, MuxChannelState.open);
        await parent.close(Status.of(StatusCode.cancelled));
        expect(
          await first.done.timeout(h.limit),
          h.hasCode(StatusCode.cancelled),
        );
        // Releasing the parent and its pending child leaves room again.
        final next = await client.openChannelAt(
          endpoint,
          ChannelAddress(type: h.echo),
        );
        await h.until(() => delivered.length == 2);
        await next.close();
      },
    );

    test('the proxy restores bulk-open order toward the frame mux', () async {
      final (client, front) = pair(
        delay: (id) => Duration(milliseconds: id == 4 ? 60 : 0),
      );
      final (x, y) = MemoryTransport.pair();
      final back = MuxConnection(x, isInitiator: true, options: h.fast);
      final server = MuxConnection(y, isInitiator: false, options: h.fast);
      addTearDown(back.close);
      addTearDown(server.close);
      final arrived = <int>[];
      final forwarded = <int>[];
      front.incoming.listen((channel) {
        final address = ChannelAddress.decode(channel.openPayload);
        if (TalkBulkOpen.isBulk(address)) {
          arrived.add(TalkBulkOpen.decode(address).number);
          TalkChannel.adoptBulk(channel);
        } else {
          unawaited(pipeChannels(channel, back.open(channel.openPayload)));
        }
      });
      back.incoming.listen(TalkChannel.adoptBulk);
      client.incoming.listen(TalkChannel.adoptBulk);
      server.incoming.listen((channel) {
        final address = ChannelAddress.decode(channel.openPayload);
        if (TalkBulkOpen.isBulk(address)) {
          forwarded.add(TalkBulkOpen.decode(address).number);
          TalkChannel.adoptBulk(channel);
        } else {
          TalkChannel(channel).messages
              .listen((m) => unawaited(m.reply(m.payload)));
        }
      });
      final talk = TalkChannel(
        client.open(ChannelAddress(type: h.echo).encode()),
      );
      final payloads = [h.pattern(70000), h.pattern(90000)];
      final replies = await Future.wait([
        for (final payload in payloads) talk.request('ECHO', payload),
      ]).timeout(h.limit);
      expect(arrived, [2, 1]);
      expect(forwarded, [1, 2]);
      expect(replies.map((reply) => reply.payload), payloads);
      await talk.close().timeout(h.limit);
    });

    test(
      'bulk-shaped opaque subframes pass through the proxy without waiting',
      () async {
        final (a, b) = pair();
        final (c, d) = pair();
        final incoming = b.incoming.first;
        final outgoing = d.incoming.first;
        final sender = a.open(Uint8List(1));
        final destination = c.open(Uint8List(1));
        unawaited(pipeChannels(await incoming, destination));
        final bytes = TalkFrame(
          kind: TalkKind.message,
          procedure: Name('RAW'),
          bulk: true,
          payload: TalkBulkReference(7).encode(),
        ).encode();
        await sender.send(bytes);
        expect(await (await outgoing).stream.first.timeout(h.limit), bytes);
      },
    );

    test('Talk switches to bulk at the binding subframe boundary', () async {
      final (a, b) = pair(options: h.fast.copyWith(initialWindow: 128));
      final received = <TalkMessage>[];
      final opens = <MuxChannel>[];
      b.incoming.listen((channel) {
        opens.add(channel);
        if (TalkBulkOpen.isBulk(ChannelAddress.decode(channel.openPayload))) {
          TalkChannel.adoptBulk(channel);
        } else {
          TalkChannel(channel).messages.listen(received.add);
        }
      });
      final parent = a.open(ChannelAddress(type: h.echo).encode());
      final sender = TalkChannel(parent);
      final header = TalkFrame(
        kind: TalkKind.message,
        procedure: Name('A'),
        payload: Uint8List(0),
      ).encode().length;
      await sender.send('A', Uint8List(parent.maxSubframeLength - header));
      await h.until(() => received.length == 1);
      expect(received.single.isBulk, isFalse);
      expect(opens, hasLength(1));
      await sender.send('A', Uint8List(parent.maxSubframeLength - header + 1));
      await h.until(() => received.length == 2);
      expect(received.last.isBulk, isTrue);
      expect(opens, hasLength(2));
    });

    for (final parentDelayed in [false, true]) {
      test(
        'Talk handles reordered bulk opens, parent delayed: $parentDelayed',
        () async {
          final (a, b) = pair(
            delay: (id) => Duration(
              milliseconds: id == 2 && parentDelayed ? 100 : (id == 4 ? 60 : 0),
            ),
          );
          final receiver = Completer<TalkChannel>();
          final openOrder = <int>[];
          b.incoming.listen((channel) {
            openOrder.add(channel.id);
            final address = ChannelAddress.decode(channel.openPayload);
            if (TalkBulkOpen.isBulk(address)) {
              TalkChannel.adoptBulk(channel);
            } else {
              receiver.complete(TalkChannel(channel));
            }
          });
          a.incoming.listen(TalkChannel.adoptBulk);
          final sender = TalkChannel(
            a.open(ChannelAddress(type: h.echo).encode()),
          );
          final first = h.pattern(70000);
          final second = h.pattern(90000);
          final sent = [
            sender.send('FIRST', first),
            sender.send('SECOND', second),
          ];
          final server = await receiver.future.timeout(h.limit);
          final messages = <TalkMessage>[];
          server.messages.listen(messages.add);
          await Future.wait(sent).timeout(h.limit);
          await h.until(() => messages.length == 2);
          expect(messages.map((m) => m.procedure.toString()), [
            'FIRST',
            'SECOND',
          ]);
          expect(messages[0].payload, first);
          expect(messages[1].payload, second);
          expect(openOrder.indexOf(6), lessThan(openOrder.indexOf(4)));
          if (parentDelayed) expect(openOrder.last, 2);
          await sender.close().timeout(h.limit);
        },
      );
    }

    test(
      'duplicate bulk opens still fail the parent on independent streams',
      () async {
        final (a, b) = pair();
        final receiver = Completer<TalkChannel>();
        b.incoming.listen((channel) {
          if (TalkBulkOpen.isBulk(ChannelAddress.decode(channel.openPayload))) {
            TalkChannel.adoptBulk(channel);
          } else {
            receiver.complete(TalkChannel(channel));
          }
        });
        final parent = a.open(ChannelAddress(type: h.echo).encode());
        final talk = await receiver.future;
        talk.messages.listen((_) {});
        parent.openAfter(TalkBulkOpen(parent.id, 2).encode());
        parent.openAfter(TalkBulkOpen(parent.id, 2).encode());
        expect(
          await parent.done.timeout(h.limit),
          h.hasCode(StatusCode.protocolError),
        );
      },
    );

    test('unknown bulk parents share one bounded backlog', () async {
      final (a, b) = pair();
      b.incoming.listen(TalkChannel.adoptBulk);
      final children = [
        for (var i = 0; i < 17; i++)
          a.open(TalkBulkOpen(1000 + i * 2, 1).encode()),
      ];
      expect(
        await children.last.done.timeout(h.limit),
        h.hasCode(StatusCode.resourceExhausted),
      );
      expect(children.first.state, MuxChannelState.open);
      await a.close(); // Also removes the unknown-parent timers.
    });
  });

  group('in-process binding flow and lifetime', () {
    test(
      'empty subframes stall a stopped reader and resume on consumption',
      () async {
        final (a, b) = pair(options: h.fast.copyWith(initialWindow: 8));
        final arriving = b.incoming.first;
        final channel = a.open(Uint8List(0));
        final remote = await arriving;
        for (var i = 0; i < 8; i++) {
          await channel.send(Uint8List(0));
        }
        var ninthSent = false;
        final ninth = channel.send(Uint8List(0)).then((_) => ninthSent = true);
        await h.settle();
        expect(ninthSent, isFalse);
        expect(remote.bufferedBytes, 8);
        expect(remote.receiveWindow, 0);
        remote.manualCredit = true;
        final received = <Uint8List>[];
        remote.stream.listen(received.add);
        await h.until(() => received.length == 8);
        expect(remote.heldBytes, 8);
        expect(() => remote.consumed(0, subframes: 9), throwsStateError);
        remote.consumed(0, subframes: 4);
        await ninth.timeout(h.limit);
        await h.until(() => received.length == 9);
        expect(remote.heldBytes, 5);
      },
    );

    test(
      'one blocked channel leaves another writable; close follows queued data',
      () async {
        final (a, b) = pair(options: h.fast.copyWith(initialWindow: 8));
        final incoming = <MuxChannel>[];
        b.incoming.listen(incoming.add);
        final slow = a.open(Uint8List(0));
        final other = a.open(Uint8List(0));
        await h.until(() => incoming.length == 2);
        await slow.send(Uint8List.fromList([1, 2, 3]));
        await slow.send(Uint8List.fromList([4, 5, 6]));
        final third = slow.send(Uint8List.fromList([7]));
        final closed = slow.close(Status.of(StatusCode.cancelled, 'finished'));
        await h.settle();
        expect(slow.state, MuxChannelState.halfClosedLocal);
        expect(incoming[0].state, MuxChannelState.open);
        await other.send(Uint8List.fromList([9]));
        expect(await incoming[1].stream.first, [9]);
        final data = await incoming[0].stream.toList().timeout(h.limit);
        await third;
        await closed;
        expect(data, [
          [1, 2, 3],
          [4, 5, 6],
          [7],
        ]);
        expect(await incoming[0].done, h.hasCode(StatusCode.cancelled));
      },
    );

    test(
      'peer close fails queued sends and cancels an openAfter still waiting',
      () async {
        final (a, b) = pair(options: h.fast.copyWith(initialWindow: 8));
        final incoming = <MuxChannel>[];
        b.incoming.listen(incoming.add);
        final parent = a.open(Uint8List(0));
        await h.until(() => incoming.isNotEmpty);
        await parent.send(Uint8List(3));
        await parent.send(Uint8List(3));
        final blocked = parent.send(Uint8List(1));
        final child = parent.openAfter(Uint8List(0));
        final grandchild = child.openAfter(Uint8List(0));
        await incoming.single.close(Status.of(StatusCode.permissionDenied));
        await expectLater(blocked, h.throwsCode(StatusCode.failedPrecondition));
        expect(await child.done, h.hasCode(StatusCode.cancelled));
        expect(await grandchild.done, h.hasCode(StatusCode.cancelled));
        expect(a.openChannelCount, 0);
        expect(incoming, hasLength(1));
      },
    );

    test(
      'closed channels still count against the undelivered backlog',
      () async {
        final (a, b) = pair(options: h.fast.copyWith(maxChannels: 2));
        for (var i = 0; i < 2; i++) {
          await a.open(Uint8List(0)).close();
        }
        expect(b.openChannelCount, 0);
        final rejected = a.open(Uint8List(0));
        expect(
          await rejected.done.timeout(h.limit),
          h.hasCode(StatusCode.resourceExhausted),
        );
        final pending = <MuxChannel>[];
        b.incoming.listen(pending.add);
        await h.until(() => pending.length == 2);
        a.open(Uint8List(0));
        await h.until(() => pending.length == 3);
      },
    );

    test(
      'open payload budget is retained until closed channels are delivered',
      () async {
        final (a, b) = pair(options: h.fast.copyWith(maxOpenPayloadBytes: 5));
        await a.open(Uint8List(5)).close();
        expect(b.openPayloadBytes, 5);
        final rejected = a.open(Uint8List(1));
        expect(
          await rejected.done.timeout(h.limit),
          h.hasCode(StatusCode.resourceExhausted),
        );
        b.incoming.listen((_) {});
        await h.until(() => b.openPayloadBytes == 0);
      },
    );

    test(
      'GOAWAY allows a bulk continuation and refuses ordinary new channels',
      () async {
        final (a, b) = pair(options: h.fast.copyWith(goAwayGrace: h.limit));
        final incoming = <MuxChannel>[];
        b.incoming.listen(incoming.add);
        final parent = a.open(Uint8List(0));
        await h.until(() => incoming.length == 1);
        final shutdown = b.goAway();
        await a.peerGoAwayStatus;
        expect(
          () => a.open(Uint8List(0)),
          h.throwsCode(StatusCode.failedPrecondition),
        );
        final child = parent.openAfter(TalkBulkOpen(parent.id, 1).encode());
        await h.until(() => incoming.length == 2);
        await child.close();
        await parent.close();
        await shutdown.timeout(h.limit);
        expect(await a.done, h.hasCode(StatusCode.goingAway));
      },
    );
  });

  group('binding node lifecycle', () {
    test('the pool uses the binding channel cap and preserves paths', () async {
      final server = Switchboard(muxOptions: h.fast.copyWith(maxChannels: 1));
      addTearDown(server.close);
      server.defaultService = (_) {};
      var calls = 0;
      final binding = DialBinding((_, settings) async {
        calls++;
        final (a, b) = InProcessConnection.pair(
          initiator: settings.options,
          acceptor: server.muxOptions,
        );
        await server.adopt(b);
        return a;
      });
      final client = h.node(bindings: [binding]);
      final uri = Uri.parse('review://host/one');
      final first = await client.connect(uri);
      expect(await client.connect(uri.replace(fragment: 'x')), same(first));
      first.open(Uint8List(1));
      final second = await client.connect(uri);
      expect(second, isNot(same(first)));
      expect(
        await client.connect(Uri.parse('review://host/two')),
        isNot(same(second)),
      );
      expect(calls, 3);
    });

    test(
      'adopting a dialled connection cannot replace its policy or remote',
      () async {
        final (a, b) = pair();
        final client = Switchboard(
          muxOptions: h.fast,
          bindings: [DialBinding((_, _) async => a)],
          outgoingPolicy: ChannelPolicies.denyAll,
        );
        addTearDown(client.close);
        var delivered = 0;
        client.defaultService = (_) => delivered++;
        await client.connect(Uri.parse('review://host'));
        await expectLater(
          client.adopt(
            a,
            policy: ChannelPolicies.allowAll,
            remote: 'replacement',
          ),
          throwsArgumentError,
        );
        final request = b.open(Uint8List(1));
        expect(
          await request.done.timeout(h.limit),
          h.hasCode(StatusCode.permissionDenied),
        );
        expect(delivered, 0);
      },
    );

    for (final synchronous in [false, true]) {
      test('binding errors map to UNAVAILABLE (sync: $synchronous)', () async {
        final client = h.node(
          bindings: [
            DialBinding((_, _) {
              if (synchronous) throw StateError('dial failed');
              return Future.error(StateError('dial failed'));
            }),
          ],
        );
        await expectLater(
          client.connect(Uri.parse('review://host')),
          h.throwsCode(StatusCode.unavailable),
        );
      });
    }

    test(
      'close waits for a dial that arrives during shutdown and sends GOAWAY',
      () async {
        final attempt = Completer<MuxConnection>();
        final client = h.node(
          bindings: [DialBinding((_, _) => attempt.future)],
        );
        final dial = client.connect(Uri.parse('review://host'));
        final rejected = expectLater(
          dial,
          h.throwsCode(StatusCode.failedPrecondition),
        );
        var closed = false;
        final shutdown = client.close().then((_) => closed = true);
        await h.settle();
        expect(closed, isFalse);
        final (a, b) = pair();
        attempt.complete(a);
        await rejected;
        await shutdown.timeout(h.limit);
        expect(
          await b.peerGoAwayStatus.timeout(h.limit),
          h.hasCode(StatusCode.goingAway),
        );
        expect(a.isOpen, isFalse);
        expect(
          () => client.registerBinding(DialBinding((_, _) async => a)),
          h.throwsCode(StatusCode.failedPrecondition),
        );
      },
    );

    test(
      'endpoint, identity and credential hooks see their socket-dial inputs',
      () async {
        final issuer = CredentialIssuer.hmac(Name('review'), Uint8List(32));
        final credential = await issuer.issue(
          kind: CredentialKind.node,
          identity: 'client',
        );
        final verifier = CredentialVerifier()
          ..addHmacKey(Name('review'), Uint8List(32));
        final server = h.node(verifier: verifier);
        final payloads = <Uint8List>[];
        server.defaultService = (incoming) {
          payloads.add(incoming.address.payload);
          unawaited(incoming.channel.close());
        };
        final uri = Uri.parse('review://host/path#service');
        final policyInputs = <Uri>[];
        final identityInputs = <Uri>[];
        final credentialInputs = <Uri>[];
        BindingDialSettings? seenSettings;
        final binding = DialBinding((_, settings) async {
          seenSettings = settings;
          final (a, b) = InProcessConnection.pair(
            initiator: settings.options,
            acceptor: server.muxOptions,
          );
          a.localIdentity = settings.localIdentity;
          await server.adopt(b);
          return a;
        });
        final client = Switchboard(
          muxOptions: h.fast,
          credential: credential,
          bindings: [binding],
          endpointPolicy: (endpoint) {
            policyInputs.add(endpoint);
            return ChannelPolicies.denyAll;
          },
          expectedIdentityFor: (endpoint, _) {
            identityInputs.add(endpoint);
            return null;
          },
          credentialFor: (endpoint, _) {
            credentialInputs.add(endpoint);
            return Uint8List.fromList([42]);
          },
        );
        addTearDown(client.close);
        final channel = await client.openChannelAt(uri, ChannelAddress());
        await channel.done.timeout(h.limit);
        expect(seenSettings!.localIdentity, 'client');
        expect(seenSettings!.options, same(client.muxOptions));
        expect(policyInputs, [uri.replace(fragment: '')]);
        expect(identityInputs, policyInputs);
        expect(credentialInputs, [uri]);
        expect(payloads, [
          [42],
        ]);
      },
    );
  });

  test(
    'the relay waits for identity on the independent control stream',
    () async {
      final issuer = CredentialIssuer.hmac(Name('relay'), Uint8List(32));
      final credential = await issuer.issue(
        kind: CredentialKind.node,
        identity: 'consumer',
        scopes: [Scope.of(Right.open, 'echo')],
      );
      final verifier = CredentialVerifier()
        ..addHmacKey(Name('relay'), Uint8List(32));
      final backend = h.node();
      backend.registerService(h.echo, h.echoService, instance: 1);
      final endpoint = h.listen(backend, 'review-relay-backend');
      final resolver = StaticResolver([
        ServiceRecord(ServiceAddress(h.echo, 1), endpoints: [endpoint]),
      ]);
      addTearDown(resolver.close);
      final relay = h.node(
        bindings: [InProcessBinding()],
        resolver: resolver,
        verifier: verifier,
      );
      relay.registerService(
        Services.relay,
        RelayService(relay, allowEndpoints: true).handler,
      );
      final relayEndpoint = h.listen(relay, 'review-relay');
      final client = h.node(
        bindings: [
          InProcessBinding(controlDelay: const Duration(milliseconds: 50)),
        ],
        credential: credential,
        identifyOutgoing: false,
      );
      final connection = await client.connect(relayEndpoint);
      final identified = client.identifyOn(connection);
      final talk = TalkChannel(
        connection.open(
          ChannelAddress(
            type: Services.relay,
            payload: ChannelAddress(type: h.echo, instance: 1).encode(),
          ).encode(),
        ),
      );
      final response = await talk
          .request('R', Uint8List.fromList([42]))
          .timeout(h.limit);
      expect(response.payload, [42]);
      await identified;
    },
  );

  test(
    'a connector returning a listened connection fails and retries',
    () async {
      final (bad, _) = pair();
      bad.incoming.listen((_) {});
      final (good, _) = pair();
      var attempts = 0;
      final client = ReconnectingClient.connections(
        (_) async => ++attempts == 1 ? bad : good,
        muxOptions: h.fast,
        initialBackoff: const Duration(milliseconds: 10),
        jitter: 0,
      );
      addTearDown(client.close);
      expect(await client.connected.timeout(h.limit), same(good));
      expect(attempts, 2);
      expect(bad.isOpen, isFalse);
    },
  );

  for (final endedBy in ['timeout', 'stop', 'close']) {
    test(
      'ReconnectingClient closes a connection arriving after $endedBy',
      () async {
        final attempt = Completer<MuxConnection>();
        final client = ReconnectingClient.connections(
          (_) => attempt.future,
          muxOptions: h.fast,
          connectTimeout: const Duration(milliseconds: 30),
          initialBackoff: const Duration(seconds: 10),
          autoStart: false,
          jitter: 0,
        );
        addTearDown(client.close);
        client.start();
        if (endedBy == 'timeout') {
          await h.until(() => client.state.phase == ClientPhase.disconnected);
        } else if (endedBy == 'stop') {
          client.stop();
        } else {
          await client.close();
        }
        final (a, _) = pair();
        attempt.complete(a);
        expect(
          await a.done.timeout(h.limit),
          h.hasCode(StatusCode.connectionLost),
        );
        expect(client.state.phase, ClientPhase.disconnected);
      },
    );
  }
}
