/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Opus 5.5 <noreply@anthropic.com>
*/

// Bulk channels over a binding whose channels are independent streams
// ([MemoryBinding]): the receiver rules of the binding specification
// (section "Bulk payloads": a parent not arrived yet, a parent ended, the
// number order), Talk keeping the payloads referenced before a graceful
// close, the proxy forwarding them toward the frame mux, the bounds of all
// of it; and the endpoints of a binding's listeners as the node's own.

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:fake_async/fake_async.dart';
import 'package:switchboard/switchboard.dart';
import 'package:test/test.dart';

import 'binding_seam_test.dart' as h;
import 'memory_binding.dart';

/// A memory binding pair, the records of the channels [delayed] names
/// [delay] late.
(MemoryBindingConnection, MemoryBindingConnection) pair({
  bool Function(int id)? delayed,
  Duration delay = const Duration(milliseconds: 100),
}) {
  final (a, b) = MemoryBindingConnection.pair(
    initiator: h.fast,
    acceptor: h.fast,
    channelDelay: delayed == null
        ? null
        : (id) => delayed(id) ? delay : Duration.zero,
  );
  addTearDown(a.close);
  addTearDown(b.close);
  return (a, b);
}

/// A plain message whose payload of [length] bytes is bulk number
/// [number], as its sender's Talk layer encodes it.
Uint8List bulkMessage(int number, int length) => TalkFrame(
  kind: TalkKind.message,
  procedure: Name('BIG'),
  bulk: true,
  payload: TalkBulkReference(number, length: length).encode(),
).encode();

Uint8List plainMessage() => TalkFrame(
  kind: TalkKind.message,
  procedure: Name('SMALL'),
  payload: Uint8List(1),
).encode();

Uint8List bulkOpen(MuxChannel parent, int number) =>
    TalkBulkOpen(parent.id, number).encode();

/// The receiving side of [connection]: a Talk layer on every channel the
/// peer opens, bulk channels routed as a node routes them.
class Receiver {
  Receiver(MuxConnection connection, {TalkOptions? options}) {
    connection.incoming.listen((channel) {
      final address = ChannelAddress.decode(channel.openPayload);
      if (TalkBulkOpen.isBulk(address)) {
        TalkChannel.adoptBulk(channel);
        return;
      }
      final talk = TalkChannel(channel, options: options);
      talks.add(talk);
      talk.messages.listen(messages.add, onError: (Object _) {});
    });
  }

  final List<TalkChannel> talks = [];
  final List<TalkMessage> messages = [];
}

void main() {
  group('Talk over independent streams', () {
    for (final length in [50000, 200000]) {
      test('a payload on a slow bulk channel, the sender closing at once, is '
          'delivered ($length bytes)', () async {
        // The sender's first bulk channel (4) is slow; the parent (2) is
        // not. Talk's graceful close waits for its bulk channels to be
        // closed both ways, so the close follows the payload anyway; a
        // sender that does not wait is below ("after a graceful close").
        final (a, b) = pair(delayed: (id) => id == 4);
        final receiver = Receiver(b);
        final sender = TalkChannel(
          a.open(ChannelAddress(type: h.echo).encode()),
        );
        final payload = h.pattern(length);
        final sent = sender.send('BIG', payload);
        await sender.close().timeout(h.limit);
        await sent.timeout(h.limit);
        await h.until(() => receiver.messages.isNotEmpty);
        expect(receiver.messages.single.payload, payload);
        await receiver.talks.single.done.timeout(h.limit);
      });
    }

    test('a response on a slow bulk channel, the responder closing at once, '
        'completes its request', () async {
      final (a, b) = pair(delayed: (id) => id == 3);
      final reply = h.pattern(120000);
      b.incoming.listen((channel) {
        if (TalkBulkOpen.isBulk(ChannelAddress.decode(channel.openPayload))) {
          TalkChannel.adoptBulk(channel);
          return;
        }
        final talk = TalkChannel(channel);
        talk.messages.listen((m) async {
          await m.reply(reply);
          await talk.close();
        });
      });
      a.incoming.listen(TalkChannel.adoptBulk);
      final client = TalkChannel(a.open(ChannelAddress(type: h.echo).encode()));
      final answer = await client.request('GET', Uint8List(0)).timeout(h.limit);
      expect(answer.payload, reply);
      await client.done.timeout(h.limit);
    });

    test('after a graceful close, a bulk channel a message waits for is '
        'admitted and any other refused at once', () async {
      final (a, b) = pair();
      final receiver = Receiver(b);
      final parent = a.open(ChannelAddress(type: h.echo).encode());
      await parent.send(bulkMessage(1, 3));
      await parent.close();
      await h.until(() => receiver.talks.isNotEmpty);
      final stray = a.open(bulkOpen(parent, 2));
      expect(
        await stray.done.timeout(const Duration(seconds: 2)),
        h.hasCode(StatusCode.failedPrecondition),
      );
      final late = a.open(bulkOpen(parent, 1));
      await late.send(Uint8List.fromList([1, 2, 3]));
      await late.close();
      await h.until(() => receiver.messages.isNotEmpty);
      expect(receiver.messages.single.payload, [1, 2, 3]);
      expect(await late.done, Status.ok);
      await receiver.talks.single.done.timeout(h.limit);
    });

    test('a bulk channel below the highest reference that nothing waits for '
        'is cancelled, and the channel goes on', () async {
      final (a, b) = pair();
      final receiver = Receiver(b);
      final parent = a.open(ChannelAddress(type: h.echo).encode());
      await parent.send(bulkMessage(2, 1));
      final second = a.open(bulkOpen(parent, 2));
      await second.send(Uint8List.fromList([9]));
      await second.close();
      await h.until(() => receiver.messages.length == 1);
      final stale = a.open(bulkOpen(parent, 1));
      expect(
        await stale.done.timeout(h.limit),
        h.hasCode(StatusCode.cancelled),
      );
      await parent.send(plainMessage());
      await h.until(() => receiver.messages.length == 2);
      expect(parent.state, MuxChannelState.open);
    });

    test('an unreferenced bulk channel is cancelled once a later reference '
        'passes it', () async {
      final (a, b) = pair();
      final receiver = Receiver(b);
      final parent = a.open(ChannelAddress(type: h.echo).encode());
      final skipped = a.open(bulkOpen(parent, 1));
      await h.settle();
      await parent.send(bulkMessage(2, 1));
      expect(
        await skipped.done.timeout(const Duration(seconds: 2)),
        h.hasCode(StatusCode.cancelled),
      );
      final second = a.open(bulkOpen(parent, 2));
      await second.send(Uint8List.fromList([7]));
      await second.close();
      await h.until(() => receiver.messages.isNotEmpty);
      expect(receiver.messages.single.payload, [7]);
      expect(parent.state, MuxChannelState.open);
    });

    test('a second bulk channel with a number still waiting is a protocol '
        'error', () async {
      final (a, b) = pair();
      Receiver(b);
      final parent = a.open(ChannelAddress(type: h.echo).encode());
      a.open(bulkOpen(parent, 3));
      final twin = a.open(bulkOpen(parent, 3));
      expect(
        await twin.done.timeout(h.limit),
        h.hasCode(StatusCode.protocolError),
      );
      expect(
        await parent.done.timeout(h.limit),
        h.hasCode(StatusCode.protocolError),
      );
    });

    test('a bulk channel naming a channel of the receiver\'s own parity that '
        'is not open is refused at once', () {
      fakeAsync((clock) {
        final (a, b) = MemoryBindingConnection.pair(
          initiator: h.fast,
          acceptor: h.fast,
        );
        b.incoming.listen(TalkChannel.adoptBulk);
        // The acceptor's ids are odd.
        final child = a.open(TalkBulkOpen(1001, 1).encode());
        Status? ended;
        unawaited(child.done.then((status) => ended = status));
        clock.elapse(Duration.zero);
        expect(ended, h.hasCode(StatusCode.failedPrecondition));
        unawaited(a.close());
        unawaited(b.close());
        clock.elapse(Duration.zero);
      });
    });

    test('the bulk number order of the frame mux is unchanged', () async {
      // Over the frame mux, a bulk channel below one already seen is a
      // protocol error, never cancelled as over independent streams.
      final (x, y) = MemoryTransport.pair();
      final a = MuxConnection(x, isInitiator: true, options: h.fast);
      final b = MuxConnection(y, isInitiator: false, options: h.fast);
      addTearDown(a.close);
      addTearDown(b.close);
      Receiver(b);
      final parent = a.open(ChannelAddress(type: h.echo).encode());
      parent.openAfter(bulkOpen(parent, 2));
      final lower = parent.openAfter(bulkOpen(parent, 1));
      expect(
        await lower.done.timeout(h.limit),
        h.hasCode(StatusCode.protocolError),
      );
      expect(
        await parent.done.timeout(h.limit),
        h.hasCode(StatusCode.protocolError),
      );
    });
  });

  group('bounds over independent streams', () {
    test('a graceful close waits for an awaited bulk channel at most the '
        'bulk open timeout, then lets go of it', () {
      fakeAsync((clock) {
        final (a, b) = MemoryBindingConnection.pair(
          initiator: h.fast,
          acceptor: h.fast,
        );
        final receiver = Receiver(b);
        final parent = a.open(ChannelAddress(type: h.echo).encode());
        unawaited(parent.send(bulkMessage(1, 3)));
        unawaited(parent.close());
        clock.elapse(const Duration(milliseconds: 10));
        var done = false;
        unawaited(receiver.talks.single.done.then((_) => done = true));
        clock.elapse(const Duration(seconds: 9));
        expect(done, isFalse);
        clock.elapse(const Duration(seconds: 2));
        expect(done, isTrue);
        expect(receiver.messages, isEmpty);
        // No longer waited for: held as for a parent not arrived yet, then
        // refused.
        final late = a.open(bulkOpen(parent, 1));
        Status? ended;
        unawaited(late.done.then((status) => ended = status));
        clock.elapse(const Duration(seconds: 11));
        expect(ended, h.hasCode(StatusCode.failedPrecondition));
        unawaited(a.close());
        unawaited(b.close());
        clock.elapse(Duration.zero);
      });
    });

    test('a late transfer that stalls is cut by the bulk idle timeout', () {
      fakeAsync((clock) {
        final (a, b) = MemoryBindingConnection.pair(
          initiator: h.fast,
          acceptor: h.fast,
        );
        final receiver = Receiver(
          b,
          options: const TalkOptions(bulkIdleTimeout: Duration(seconds: 2)),
        );
        final parent = a.open(ChannelAddress(type: h.echo).encode());
        unawaited(parent.send(bulkMessage(1, 3)));
        unawaited(parent.close());
        clock.elapse(const Duration(milliseconds: 10));
        final late = a.open(bulkOpen(parent, 1));
        unawaited(late.send(Uint8List.fromList([1])));
        var done = false;
        unawaited(receiver.talks.single.done.then((_) => done = true));
        clock.elapse(const Duration(seconds: 1));
        expect(done, isFalse);
        clock.elapse(const Duration(seconds: 2));
        expect(done, isTrue);
        expect(receiver.messages, isEmpty);
        Status? ended;
        unawaited(late.done.then((status) => ended = status));
        clock.elapse(Duration.zero);
        // Cut by its own idle timeout or by the channel's, whichever fires
        // first: either way it failed.
        expect(ended?.isOk, isFalse);
        unawaited(a.close());
        unawaited(b.close());
        clock.elapse(Duration.zero);
      });
    });

    test(
      'at most 16 ended channels of a connection wait for bulk channels',
      () {
        fakeAsync((clock) {
          final (a, b) = MemoryBindingConnection.pair(
            initiator: h.fast,
            acceptor: h.fast,
          );
          final receiver = Receiver(b);
          final parents = [
            for (var i = 0; i < 17; i++)
              a.open(ChannelAddress(type: h.echo).encode()),
          ];
          for (final parent in parents) {
            unawaited(parent.send(bulkMessage(1, 1)));
            unawaited(parent.close());
          }
          clock.elapse(const Duration(milliseconds: 10));
          final ended = <TalkChannel>{};
          for (final talk in receiver.talks) {
            unawaited(talk.done.then((_) => ended.add(talk)));
          }
          clock.elapse(const Duration(milliseconds: 10));
          // One could not wait: it ended at once, its message dropped.
          expect(receiver.talks, hasLength(17));
          expect(ended, hasLength(1));
          for (final parent in parents) {
            final late = a.open(bulkOpen(parent, 1));
            unawaited(late.send(Uint8List.fromList([5])));
            unawaited(late.close());
          }
          clock.elapse(const Duration(milliseconds: 10));
          expect(receiver.messages, hasLength(16));
          expect(ended, hasLength(17));
          clock.elapse(const Duration(seconds: 11));
          expect(receiver.messages, hasLength(16));
          unawaited(a.close());
          unawaited(b.close());
          clock.elapse(Duration.zero);
        });
      },
    );

    test('a failed parent does not wait for its bulk channels, and cancels '
        'late ones', () async {
      final (a, b) = pair();
      final receiver = Receiver(b);
      final parent = a.open(ChannelAddress(type: h.echo).encode());
      await parent.send(bulkMessage(1, 3));
      await parent.close(Status.of(StatusCode.cancelled));
      await h.until(() => receiver.talks.isNotEmpty);
      expect(
        await receiver.talks.single.done.timeout(const Duration(seconds: 2)),
        h.hasCode(StatusCode.cancelled),
      );
      expect(receiver.messages, isEmpty);
      // Its payloads failed with it: a late bulk channel is cancelled at
      // once.
      final late = a.open(bulkOpen(parent, 1));
      expect(
        await late.done.timeout(const Duration(seconds: 2)),
        h.hasCode(StatusCode.cancelled),
      );
    });
  });

  group('the proxy over independent streams', () {
    /// A client on the memory binding (its first bulk channel slow), a proxy
    /// piping its channels to a frame mux, and a Talk server behind.
    Future<(MemoryBindingConnection, Receiver)> proxied() async {
      final (client, front) = pair(delayed: (id) => id == 4);
      final (x, y) = MemoryTransport.pair();
      final back = MuxConnection(x, isInitiator: true, options: h.fast);
      final server = MuxConnection(y, isInitiator: false, options: h.fast);
      addTearDown(back.close);
      addTearDown(server.close);
      front.incoming.listen((channel) {
        if (TalkBulkOpen.isBulk(ChannelAddress.decode(channel.openPayload))) {
          TalkChannel.adoptBulk(channel);
        } else {
          unawaited(pipeChannels(channel, back.open(channel.openPayload)));
        }
      });
      back.incoming.listen(TalkChannel.adoptBulk);
      return (client, Receiver(server));
    }

    test('a payload on a slow bulk channel, the client closing at once, '
        'passes the proxy whole', () async {
      final (client, server) = await proxied();
      final sender = TalkChannel(
        client.open(ChannelAddress(type: h.echo).encode()),
      );
      final payload = h.pattern(90000);
      final sent = sender.send('BIG', payload);
      await sender.close().timeout(h.limit);
      await sent.timeout(h.limit);
      await h.until(() => server.messages.isNotEmpty);
      expect(server.messages.single.payload, payload);
      expect(await server.talks.single.done.timeout(h.limit), Status.ok);
    });

    test('after the client\'s graceful close the proxy refuses a bulk channel '
        'no reference names, and forwards the one referenced', () async {
      final (client, server) = await proxied();
      final parent = client.open(ChannelAddress(type: h.echo).encode());
      await parent.send(bulkMessage(1, 2));
      await parent.close();
      final stray = client.open(bulkOpen(parent, 2));
      expect(
        await stray.done.timeout(h.limit),
        h.hasCode(StatusCode.failedPrecondition),
      );
      final late = client.open(bulkOpen(parent, 1));
      await late.send(Uint8List.fromList([4, 2]));
      await late.close();
      await h.until(() => server.messages.isNotEmpty);
      expect(server.messages.single.payload, [4, 2]);
    });

    test('the proxy holds a graceful close for an awaited bulk channel at '
        'most the hold timeout', () {
      fakeAsync((clock) {
        final (client, front) = MemoryBindingConnection.pair(
          initiator: h.fast,
          acceptor: h.fast,
        );
        final (back, server) = MemoryBindingConnection.pair(
          initiator: h.fast,
          acceptor: h.fast,
        );
        final piped = <MuxChannel>[];
        front.incoming.listen((channel) {
          if (TalkBulkOpen.isBulk(ChannelAddress.decode(channel.openPayload))) {
            TalkChannel.adoptBulk(channel);
          } else {
            final target = back.open(channel.openPayload);
            piped.add(target);
            unawaited(pipeChannels(channel, target));
          }
        });
        back.incoming.listen(TalkChannel.adoptBulk);
        final behind = <MuxChannel>[];
        server.incoming.listen(behind.add);
        final parent = client.open(ChannelAddress(type: h.echo).encode());
        unawaited(parent.send(bulkMessage(1, 2)));
        unawaited(parent.close());
        clock.elapse(const Duration(seconds: 5));
        expect(behind.single.state, MuxChannelState.open);
        clock.elapse(const Duration(seconds: 6));
        expect(piped.single.state, isNot(MuxChannelState.open));
        for (final c in [client, front, back, server]) {
          unawaited(c.close());
        }
        clock.elapse(Duration.zero);
      });
    });
  });

  group('the listeners of a binding', () {
    test('a declared endpoint is listed, the node\'s own, and reached '
        'in-process under its policy', () async {
      final binding = MemoryBinding();
      final node = h.node(bindings: [binding]);
      final remotes = <String?>[];
      node.registerService(h.echo, (incoming) {
        remotes.add(incoming.remote);
        h.answer(incoming);
      });
      final policy = ChannelPolicies.allowTypes({h.echo});
      final endpoint = h.listen(node, 'self', policy: policy);
      await node.addListeningEndpoint(endpoint, policy: policy);
      expect(node.listeningEndpoints, [endpoint]);
      expect(node.isOwnEndpoint(Uri.parse('MEMBINDING://Self#echo')), isTrue);
      expect(node.isOwnEndpoint(Uri.parse('membinding://other')), isFalse);
      final channel = await node.openChannelAt(
        endpoint,
        ChannelAddress(type: h.echo),
      );
      expect(await h.answerOf(channel), 'echo for nobody');
      expect(remotes, ['local']);
      expect(binding.dialled, isEmpty);
      final refused = await node.openChannelAt(
        endpoint,
        ChannelAddress(type: h.npc),
      );
      expect(
        await refused.done.timeout(h.limit),
        h.hasCode(StatusCode.permissionDenied),
      );
    });

    test('without local connections a declared endpoint is dialled through '
        'its binding', () async {
      final binding = MemoryBinding();
      final node = Switchboard(
        muxOptions: h.fast,
        bindings: [binding],
        localConnections: false,
      );
      addTearDown(node.close);
      node.registerService(h.echo, h.answer);
      final endpoint = h.listen(node, 'self-dialled');
      await node.addListeningEndpoint(endpoint);
      final channel = await node.openChannelAt(
        endpoint,
        ChannelAddress(type: h.echo),
      );
      expect(await h.answerOf(channel), 'echo for nobody');
      expect(binding.dialled, hasLength(1));
    });

    test('the proxy never forwards to a declared endpoint', () async {
      final binding = MemoryBinding();
      final self = Uri.parse('membinding://proxy-self');
      final resolver = StaticResolver([
        ServiceRecord(ServiceAddress(h.echo, 1), endpoints: [self]),
      ]);
      addTearDown(resolver.close);
      final proxy = h.node(bindings: [binding], resolver: resolver);
      proxy.catchAll = proxyHandler(proxy);
      await proxy.addListeningEndpoint(self);
      final front = await proxy.listenMemory();
      final client = h.node();
      final channel = await client.openChannelAt(
        front,
        ChannelAddress(type: h.echo, instance: 1),
      );
      final status = await channel.done.timeout(h.limit);
      expect(status.isOk, isFalse);
      expect(binding.dialled, isEmpty);
    });

    test('a wildcard address answers to the local names; removal and close '
        'forget the endpoint', () async {
      final node = h.node();
      final endpoint = Uri.parse('membinding://0.0.0.0:4433');
      await node.addListeningEndpoint(endpoint);
      expect(
        node.isOwnEndpoint(Uri.parse('membinding://127.0.0.1:4433')),
        isTrue,
      );
      expect(
        node.isOwnEndpoint(Uri.parse('membinding://localhost:4433')),
        isTrue,
      );
      expect(
        node.isOwnEndpoint(Uri.parse('membinding://localhost:4434')),
        isFalse,
      );
      final named = Uri.parse('membinding://node.example:4444');
      await node.addListeningEndpoint(
        named,
        address: InternetAddress.loopbackIPv4,
      );
      expect(
        node.isOwnEndpoint(Uri.parse('membinding://NODE.example:4444')),
        isTrue,
      );
      expect(
        node.isOwnEndpoint(Uri.parse('membinding://localhost:4444')),
        isTrue,
      );
      expect(
        node.isOwnEndpoint(Uri.parse('membinding://127.0.0.2:4444')),
        isFalse,
      );
      await expectLater(
        node.addListeningEndpoint(Uri.parse('membinding://0.0.0.0:4433')),
        throwsArgumentError,
      );
      await expectLater(
        node.addListeningEndpoint(Uri.parse('tcp://127.0.0.1:4433')),
        throwsArgumentError,
      );
      expect(node.removeListeningEndpoint(endpoint), isTrue);
      expect(node.removeListeningEndpoint(endpoint), isFalse);
      expect(
        node.isOwnEndpoint(Uri.parse('membinding://127.0.0.1:4433')),
        isFalse,
      );
      expect(node.listeningEndpoints, [named]);
      await node.close();
      expect(node.listeningEndpoints, isEmpty);
      expect(node.isOwnEndpoint(named), isFalse);
      await expectLater(
        node.addListeningEndpoint(named),
        h.throwsCode(StatusCode.failedPrecondition),
      );
    });
  });
}
