/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Opus 5.5 <noreply@anthropic.com>
*/

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
abstract class BindingConnection implements MuxConnection {
  /// For subclasses.
  BindingConnection();

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
