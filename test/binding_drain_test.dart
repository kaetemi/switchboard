/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Opus 5.5 <noreply@anthropic.com>
*/

// The drain holds of a binding whose channels are independent streams
// (`BindingConnection.retainForDrain`, `hasDrainHolds`, `retainedForDrain`):
// a parent closed gracefully while its reader still waits for a bulk
// channel it referenced is kept for the binding's idle check and its
// GOAWAY admission, until the wait settles, and only within Talk's bound
// of lingering parents per connection.

import 'dart:async';
import 'dart:typed_data';

import 'package:fake_async/fake_async.dart';
import 'package:switchboard/src/talk/talk_bulk.dart' show BulkRoutes;
import 'package:switchboard/switchboard.dart';
import 'package:test/test.dart';

import 'binding_bulk_test.dart' show Receiver, bulkMessage, bulkOpen;
import 'binding_seam_test.dart' as h;
import 'memory_binding.dart';

void main() {
  test('a parent waiting for a referenced bulk channel after its graceful '
      'close is held until the channel arrived and ended', () async {
    final (a, b) = MemoryBindingConnection.pair(
      initiator: h.fast,
      acceptor: h.fast,
    );
    addTearDown(a.close);
    addTearDown(b.close);
    final receiver = Receiver(b);
    final parent = a.open(ChannelAddress(type: h.echo).encode());
    await parent.send(bulkMessage(1, 3));
    await parent.close();
    await h.until(() => b.hasDrainHolds);
    expect(b.retainedForDrain(parent.id), isTrue);
    expect(b.channelWithId(parent.id), isNull);
    // The sender keeps nothing: its own parents are not waited for.
    expect(a.hasDrainHolds, isFalse);
    final late = a.open(bulkOpen(parent, 1));
    await late.send(Uint8List.fromList([1, 2, 3]));
    await late.close();
    await h.until(() => receiver.messages.isNotEmpty);
    expect(await late.done.timeout(h.limit), Status.ok);
    await receiver.talks.single.done.timeout(h.limit);
    await h.until(() => !b.hasDrainHolds);
    expect(b.retainedForDrain(parent.id), isFalse);
  });

  test('a parent closed with nothing referenced is not held', () async {
    final (a, b) = MemoryBindingConnection.pair(
      initiator: h.fast,
      acceptor: h.fast,
    );
    addTearDown(a.close);
    addTearDown(b.close);
    final receiver = Receiver(b);
    final parent = a.open(ChannelAddress(type: h.echo).encode());
    await parent.close();
    await h.until(() => receiver.talks.isNotEmpty);
    await receiver.talks.single.done.timeout(h.limit);
    await h.settle();
    expect(b.hasDrainHolds, isFalse);
  });

  test('holds end with their wait, and at most the bound of lingering '
      'parents is held per connection', () {
    fakeAsync((clock) {
      final (a, b) = MemoryBindingConnection.pair(
        initiator: h.fast,
        acceptor: h.fast,
      );
      Receiver(b);
      final parents = <MuxChannel>[];
      for (var i = 0; i <= BulkRoutes.maxHeld; i++) {
        final parent = a.open(ChannelAddress(type: h.echo).encode());
        parents.add(parent);
        unawaited(parent.send(bulkMessage(1, 3)));
        unawaited(parent.close());
      }
      clock.elapse(const Duration(milliseconds: 10));
      final held = parents.where((p) => b.retainedForDrain(p.id)).length;
      expect(held, BulkRoutes.maxHeld);
      // The awaited references give up after their 10 s bound.
      clock.elapse(const Duration(seconds: 11));
      expect(b.hasDrainHolds, isFalse);
      unawaited(a.close());
      unawaited(b.close());
      clock.elapse(Duration.zero);
    });
  });
}
