/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Opus 5.5 <noreply@anthropic.com>
  GPT-6 Astra <noreply@openai.com>
*/

import 'dart:async';

import 'mux_channel.dart';
import 'mux_connection.dart';
import 'mux_frame.dart';

/// The base of a binding's connection other than the frame mux: implements
/// [MuxConnection], answering the frame mux's own diagnostics as a
/// connection without them does, and leaves the binding contract (see
/// [MuxConnection], section "The binding contract") to the subclass.
///
/// Answered here: [sentBytes] and [receivedBytes] 0 (nothing counted),
/// [peerLimits] null (no LIMITS), [peerMaxChannels] 0 (no stated limit),
/// [bufferedBytes] the sum of the channels' [MuxChannel.bufferedBytes],
/// [openPayloadBytes] 0, [isReceivePaused] false, [unconfirmedCloseCount]
/// 0 and [heldOutputBytes] 0; [nextChannelIdForTesting] throws
/// [UnsupportedError], and [localIdentity] is a plain field. A subclass
/// overrides whatever it can answer better; everything else is its own to
/// implement, its channels extending [MuxChannel].
/// [hasOrderedChannels] defaults to false for independent channel streams.
abstract class BindingConnection implements MuxConnection {
  /// For subclasses.
  BindingConnection();

  // Only independent-stream bindings need to keep ended parents alive
  // while their application still waits for already referenced streams.
  final Map<int, Future<void>> _drainHolds = {};

  /// Whether admitted work still waits for streams after its parent ended.
  bool get hasDrainHolds => _drainHolds.isNotEmpty;

  /// Whether [id] is an ended parent whose reader still waits for streams.
  bool retainedForDrain(int id) => _drainHolds.containsKey(id);

  /// Keeps admitted work on [parentId] alive until [until] settles. Talk
  /// calls this only for its bounded backlog of lingering parents. A
  /// binding includes these holds in its idle check and admits their
  /// continuation streams after GOAWAY. Its grace timeout still applies.
  void retainForDrain(int parentId, Future<void> until) {
    _drainHolds[parentId] = until;
    until.whenComplete(() {
      if (identical(_drainHolds[parentId], until)) {
        _drainHolds.remove(parentId);
        drainStateChanged();
      }
    }).ignore();
    drainStateChanged();
  }

  /// Called when a drain hold is added or released. A binding schedules
  /// its idle check here, after channel listeners have handled their end.
  void drainStateChanged() {}

  @override
  String? localIdentity;

  @override
  int get sentBytes => 0;

  @override
  int get receivedBytes => 0;

  @override
  MuxLimits? get peerLimits => null;

  @override
  int get peerMaxChannels => 0;

  @override
  bool get hasOrderedChannels => false;

  @override
  int get bufferedBytes {
    var total = 0;
    for (final channel in channels) {
      total += channel.bufferedBytes;
    }
    return total;
  }

  @override
  int get openPayloadBytes => 0;

  @override
  bool get isReceivePaused => false;

  @override
  int get unconfirmedCloseCount => 0;

  @override
  int get heldOutputBytes => 0;

  @override
  set nextChannelIdForTesting(int id) =>
      throw UnsupportedError('channel ids are allocated by the binding');
}
