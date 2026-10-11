/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Opus 5.5 <noreply@anthropic.com>
  GPT-6 Astra <noreply@anthropic.com>
*/

import 'dart:typed_data';

import '../bytes.dart';
import '../status.dart';
import 'mux_protocol.dart';

export 'mux_protocol.dart';

/// Mux command, carried in bits 4 and 5 of the frame flags.
///
/// The wire value 3 is reserved and decoding it is a protocol error.
enum MuxCommand {
  /// Carries one subframe on an open channel, or a control message on
  /// channel 0.
  data(0),

  /// Opens a channel; the payload is the open payload.
  open(1),

  /// Closes a channel; the payload is a status payload, possibly empty.
  close(2);

  const MuxCommand(this.code);

  /// Wire value, 0 to 2.
  final int code;
}

/// One mux frame: a flags byte, a 2 or 6 byte channel id and a payload.
///
/// See the wiki page "Polyverse Switchboard Mux", section "Frame header".
class MuxFrame {
  /// Creates a frame. [payload] is used as is, not copied.
  MuxFrame(
    this.command,
    this.channelId,
    Uint8List? payload, {
    this.longForm = false,
  }) : payload = payload ?? Uint8List(0);

  /// A DATA frame carrying [payload] on [channelId].
  MuxFrame.data(int channelId, Uint8List payload)
    : this(MuxCommand.data, channelId, payload);

  /// An OPEN frame for [channelId] with the given open payload.
  MuxFrame.open(int channelId, Uint8List openPayload)
    : this(MuxCommand.open, channelId, openPayload);

  /// A CLOSE frame for [channelId] carrying [status].
  ///
  /// When [status] is null the payload is empty, which the receiver reads
  /// as `OK`.
  MuxFrame.close(int channelId, [Status? status])
    : this(MuxCommand.close, channelId, status?.encode());

  /// Flag bit 0x02: the channel id is a `u16` rather than a `u48`.
  static const int flagShort = 0x02;

  /// Flag bits 0x01, 0x04 and 0x08: reserved for incompatible extensions,
  /// must be 0.
  static const int flagsReservedIncompatible = 0x0D;

  /// Mask of the command bits.
  static const int commandMask = 0x30;

  /// Shift of the command bits.
  static const int commandShift = 4;

  /// Flag bits 0x40 and 0x80: reserved for compatible extensions, ignored.
  static const int flagsReservedCompatible = 0xC0;

  /// Size of the header with a short channel id.
  static const int shortHeaderSize = 3;

  /// Size of the header with a long channel id.
  static const int longHeaderSize = 7;

  /// Largest id that fits the short form.
  static const int maxShortId = 0xFFFF;

  /// Largest channel id (`u48`).
  static const int maxId = 0xFFFFFFFFFFFF;

  /// The control channel.
  static const int controlChannelId = 0;

  /// Reserved for future protocol use; any frame on it is a protocol error.
  static const int reservedChannelId = 1;

  /// Whether [id] is reserved: 1, 0xFFFF, 0xFFFF00000000 or 0xFFFFFFFFFFFF.
  ///
  /// Reserved ids can never be opened and any frame on them is a protocol
  /// error. Channel 0 is not reserved in this sense; it is the control
  /// channel.
  static bool isReservedId(int id) =>
      id == reservedChannelId ||
      id == 0xFFFF ||
      id == 0xFFFF00000000 ||
      id == 0xFFFFFFFFFFFF;

  /// The command.
  final MuxCommand command;

  /// The channel id, 0 to 0xFFFFFFFFFFFF.
  final int channelId;

  /// The payload: subframe, open payload or status payload. May be empty.
  final Uint8List payload;

  /// Whether the frame was decoded from the long (6 byte) id form.
  ///
  /// Informational only. [encode] always uses the short form when the id
  /// fits, as the specification recommends.
  final bool longForm;

  /// Size of the encoded frame in bytes, header included.
  int get encodedLength => headerSizeFor(channelId) + payload.length;

  /// Header size used by [encode] for [channelId].
  static int headerSizeFor(int channelId) =>
      channelId <= maxShortId ? shortHeaderSize : longHeaderSize;

  /// Encodes the frame. Uses the short id form whenever the id is below
  /// 0x10000.
  ///
  /// Throws [ArgumentError] if the id does not fit in a `u48`. The encoder
  /// does not reject reserved ids so that tests can produce invalid frames;
  /// the mux connection never sends them.
  Uint8List encode() {
    if (channelId < 0 || channelId > maxId) {
      throw ArgumentError.value(channelId, 'channelId', 'must fit in u48');
    }
    final short = channelId <= maxShortId;
    final headerSize = short ? shortHeaderSize : longHeaderSize;
    final out = Uint8List(headerSize + payload.length);
    out[0] = (command.code << commandShift) | (short ? flagShort : 0);
    if (short) {
      out[1] = channelId & 0xFF;
      out[2] = (channelId >> 8) & 0xFF;
    } else {
      // Split as ByteWriter.u48 does: shifts past 32 bits are wrong when
      // compiled to JavaScript.
      final hi = channelId ~/ 0x100000000;
      final lo = channelId - hi * 0x100000000;
      out[1] = lo & 0xFF;
      out[2] = (lo >> 8) & 0xFF;
      out[3] = (lo >> 16) & 0xFF;
      out[4] = (lo >> 24) & 0xFF;
      out[5] = hi & 0xFF;
      out[6] = (hi >> 8) & 0xFF;
    }
    out.setRange(headerSize, out.length, payload);
    return out;
  }

  /// Decodes one mux frame. The payload is a view into [bytes].
  ///
  /// Throws [ProtocolException] when an incompatible reserved flag is set,
  /// on command 3, on a truncated header, on any frame for a reserved id
  /// (1, 0xFFFF, 0xFFFF00000000, 0xFFFFFFFFFFFF), and on OPEN or CLOSE for
  /// the control channel 0. Compatible reserved flags (0x40, 0x80) are
  /// ignored.
  static MuxFrame decode(Uint8List bytes) {
    if (bytes.isEmpty) {
      throw ProtocolException('empty mux frame');
    }
    final flags = bytes[0];
    if (flags & flagsReservedIncompatible != 0) {
      throw ProtocolException(
        'reserved mux flag set: 0x${flags.toRadixString(16)}',
      );
    }
    final commandCode = (flags & commandMask) >> commandShift;
    if (commandCode == 3) {
      throw ProtocolException('reserved mux command 3');
    }
    final command = MuxCommand.values[commandCode];
    final short = flags & flagShort != 0;
    final reader = ByteReader(bytes, 1);
    final int channelId;
    try {
      channelId = short ? reader.u16('channel id') : reader.u48('channel id');
    } on FormatException catch (e) {
      throw ProtocolException('truncated mux header: ${e.message}');
    }
    if (isReservedId(channelId)) {
      throw ProtocolException('frame on reserved channel $channelId');
    }
    if (channelId == controlChannelId && command != MuxCommand.data) {
      throw ProtocolException('${command.name} on control channel 0');
    }
    return MuxFrame(command, channelId, reader.rest(), longForm: !short);
  }

  @override
  String toString() =>
      'MuxFrame(${command.name}, $channelId, ${hexString(payload)})';
}

/// Control message types carried on channel 0.
enum MuxControlType {
  /// Keep-alive or latency probe; the receiver answers with [pong].
  ping(1),

  /// Answer to [ping] with the same payload.
  pong(2),

  /// The sender is shutting down; payload is a status payload.
  goAway(3),

  /// Advisory limits of the sender; payload is [MuxLimits].
  limits(4),

  /// 32 random bytes: the sender intends to identify, or wants the peer
  /// to. Answered with the receiver's own NONCE if it has not sent one.
  nonce(5),

  /// The sender's credential and proof of possession; payload is
  /// [MuxIdent].
  ident(6),

  /// Flow-control credit for one channel; payload is [MuxCredit].
  credit(7);

  const MuxControlType(this.code);

  /// Wire value.
  final int code;

  static final Map<int, MuxControlType> _byCode = {
    for (final value in values) value.code: value,
  };

  /// The type for [code], or null for unknown types (which receivers must
  /// ignore).
  static MuxControlType? fromCode(int code) => _byCode[code];
}

/// A control message: a type byte followed by a control payload, carried
/// as the payload of a DATA frame on channel 0.
class MuxControlMessage {
  /// Creates a control message. [type] may be an unknown type code.
  MuxControlMessage(this.type, Uint8List? payload)
    : payload = payload ?? Uint8List(0);

  /// PING with an opaque payload of at most [maxPingPayload] bytes.
  ///
  /// Throws [ArgumentError] if the payload is too long.
  factory MuxControlMessage.ping([Uint8List? payload]) {
    if (payload != null && payload.length > maxPingPayload) {
      throw ArgumentError.value(
        payload.length,
        'payload',
        'PING payload is at most $maxPingPayload bytes',
      );
    }
    return MuxControlMessage(MuxControlType.ping.code, payload);
  }

  /// PONG echoing a PING payload.
  MuxControlMessage.pong(Uint8List payload)
    : this(MuxControlType.pong.code, payload);

  /// GOAWAY carrying [status].
  ///
  /// The reason is shortened, on a UTF-8 character boundary, so that the
  /// payload stays within [maxControlPayload] bytes.
  MuxControlMessage.goAway(Status status)
    : this(
        MuxControlType.goAway.code,
        truncateStatus(status, maxControlPayload).encode(),
      );

  /// LIMITS carrying [limits].
  MuxControlMessage.limits(MuxLimits limits)
    : this(MuxControlType.limits.code, limits.encode());

  /// NONCE carrying [nonce], which must be [MuxIdent.nonceLength] bytes.
  ///
  /// Throws [ArgumentError] for another length.
  factory MuxControlMessage.nonce(Uint8List nonce) {
    if (nonce.length != MuxIdent.nonceLength) {
      throw ArgumentError.value(
        nonce.length,
        'nonce',
        'a NONCE is ${MuxIdent.nonceLength} bytes',
      );
    }
    return MuxControlMessage(MuxControlType.nonce.code, nonce);
  }

  /// IDENT carrying [ident]. Throws [ArgumentError] like [MuxIdent.encode].
  MuxControlMessage.ident(MuxIdent ident)
    : this(MuxControlType.ident.code, ident.encode());

  /// CREDIT carrying [credit]. Throws [ArgumentError] like
  /// [MuxCredit.encode].
  MuxControlMessage.credit(MuxCredit credit)
    : this(MuxControlType.credit.code, credit.encode());

  /// Largest PING payload a sender may use.
  static const int maxPingPayload = 125;

  /// Largest control payload (after the type byte) a receiver accepts.
  static const int maxControlPayload = maxControlPayloadLength;

  /// Control type code.
  final int type;

  /// Control payload, after the type byte.
  final Uint8List payload;

  /// The known type, or null if [type] is not in [MuxControlType].
  MuxControlType? get knownType => MuxControlType.fromCode(type);

  /// Encodes as the payload of a DATA frame on channel 0.
  Uint8List encode() {
    final out = Uint8List(1 + payload.length);
    out[0] = type;
    out.setRange(1, out.length, payload);
    return out;
  }

  /// This message as a DATA frame on channel 0.
  MuxFrame toFrame() => MuxFrame.data(MuxFrame.controlChannelId, encode());

  /// Decodes the payload of a DATA frame on channel 0.
  ///
  /// Throws [ProtocolException] if the payload is empty or the control
  /// payload exceeds [maxControlPayload] bytes. Unknown types decode fine.
  static MuxControlMessage decode(Uint8List bytes) {
    if (bytes.isEmpty) {
      throw ProtocolException('control message without type');
    }
    if (bytes.length - 1 > maxControlPayload) {
      throw ProtocolException(
        'control payload of ${bytes.length - 1} bytes exceeds '
        '$maxControlPayload',
      );
    }
    return MuxControlMessage(bytes[0], Uint8List.sublistView(bytes, 1));
  }

  /// Decodes the payload of a GOAWAY message as a status.
  ///
  /// Throws [ProtocolException] if the status payload is malformed.
  Status get goAwayStatus => decodeStatusPayload(payload, 'GOAWAY');

  @override
  String toString() =>
      'MuxControlMessage(${knownType?.name ?? type}, ${hexString(payload)})';
}

/// What a peer announces with the LIMITS control message: the largest frame
/// and the number of channels it accepts (advisory, 0 meaning "no stated
/// limit"), and the initial flow-control window of the channels opened
/// after the message is received.
///
/// See the wiki page "Polyverse Switchboard Mux", sections "Control channel"
/// and "Flow control".
class MuxLimits {
  /// Creates limits. [initialWindow] defaults to [defaultInitialWindow],
  /// the window a sender assumes before any LIMITS arrives.
  const MuxLimits({
    required this.maxFrameSize,
    required this.maxChannels,
    this.initialWindow = defaultInitialWindow,
  });

  /// The initial window of every channel until the peer's LIMITS has been
  /// received: 64 KiB.
  static const int defaultInitialWindow = 64 * 1024;

  /// Largest window: a window above `2^32 - 1` is a protocol error.
  static const int maxWindow = 0xFFFFFFFF;

  /// Largest mux frame, header included, the sender accepts.
  final int maxFrameSize;

  /// Largest number of simultaneously open channels the sender accepts.
  final int maxChannels;

  /// The flow-control window, in bytes of cost (see [MuxCredit.costOf]),
  /// that the sender of the LIMITS grants every channel opened after the
  /// message was received, in each channel's direction towards it. Never 0.
  final int initialWindow;

  /// Encoded size of the fields this implementation understands, and the
  /// smallest LIMITS payload it accepts.
  static const int encodedLength = 12;

  /// Encodes as `u32 maxFrameSize`, `u32 maxChannels`, `u32 initialWindow`.
  ///
  /// Throws [ArgumentError] if a value does not fit in a `u32`, or if
  /// [initialWindow] is 0.
  Uint8List encode() {
    for (final (name, value) in [
      ('maxFrameSize', maxFrameSize),
      ('maxChannels', maxChannels),
      ('initialWindow', initialWindow),
    ]) {
      if (value < 0 || value > 0xFFFFFFFF) {
        throw ArgumentError.value(value, name, 'must fit in u32');
      }
    }
    if (initialWindow == 0) {
      throw ArgumentError.value(
        initialWindow,
        'initialWindow',
        'must not be 0',
      );
    }
    return (ByteWriter(encodedLength)
          ..u32(maxFrameSize)
          ..u32(maxChannels)
          ..u32(initialWindow))
        .toBytes();
  }

  /// Decodes a LIMITS payload. Fields beyond the first three are ignored.
  ///
  /// Throws [ProtocolException] if the payload is shorter than
  /// [encodedLength] bytes or the initial window is 0.
  static MuxLimits decode(Uint8List bytes) {
    if (bytes.length < encodedLength) {
      throw ProtocolException(
        'LIMITS of ${bytes.length} bytes, at least $encodedLength expected',
      );
    }
    final reader = ByteReader(bytes);
    final limits = MuxLimits(
      maxFrameSize: reader.u32('max frame size'),
      maxChannels: reader.u32('max channels'),
      initialWindow: reader.u32('initial window'),
    );
    if (limits.initialWindow == 0) {
      throw ProtocolException('LIMITS with an initial window of 0');
    }
    return limits;
  }

  @override
  bool operator ==(Object other) =>
      other is MuxLimits &&
      other.maxFrameSize == maxFrameSize &&
      other.maxChannels == maxChannels &&
      other.initialWindow == initialWindow;

  @override
  int get hashCode => Object.hash(maxFrameSize, maxChannels, initialWindow);

  @override
  String toString() =>
      'MuxLimits(maxFrameSize: $maxFrameSize, maxChannels: $maxChannels, '
      'initialWindow: $initialWindow)';
}

/// The payload of a CREDIT control message: `u48` channel id, `u32` bytes.
///
/// The sender has consumed [bytes] of cost on [channelId] (or grants that
/// much more), and the receiver may send as many more bytes of cost on it.
/// See the wiki page "Polyverse Switchboard Mux", section "Flow control".
class MuxCredit {
  /// Credit of [bytes] for [channelId].
  const MuxCredit(this.channelId, this.bytes);

  /// What a DATA frame costs beyond its payload, so that a flood of empty
  /// subframes is bounded too: 16 bytes.
  static const int frameOverhead = 16;

  /// The flow-control cost of a DATA frame carrying a subframe of
  /// [subframeLength] bytes: the length plus [frameOverhead].
  static int costOf(int subframeLength) => subframeLength + frameOverhead;

  /// Encoded size: exactly 10 bytes.
  static const int encodedLength = 10;

  /// The channel the credit is for, from the receiver's point of view the
  /// same id as on the wire.
  final int channelId;

  /// Bytes of cost returned or granted, 0 to `2^32 - 1`.
  final int bytes;

  /// Encodes as `u48 channelId`, `u32 bytes`.
  ///
  /// Throws [ArgumentError] if [channelId] does not fit in a `u48` or
  /// [bytes] in a `u32`.
  Uint8List encode() {
    if (channelId < 0 || channelId > MuxFrame.maxId) {
      throw ArgumentError.value(channelId, 'channelId', 'must fit in u48');
    }
    if (bytes < 0 || bytes > 0xFFFFFFFF) {
      throw ArgumentError.value(bytes, 'bytes', 'must fit in u32');
    }
    return (ByteWriter(encodedLength)
          ..u48(channelId)
          ..u32(bytes))
        .toBytes();
  }

  /// Decodes a CREDIT payload.
  ///
  /// Throws [ProtocolException] unless the payload is exactly
  /// [encodedLength] bytes.
  static MuxCredit decode(Uint8List bytes) {
    if (bytes.length != encodedLength) {
      throw ProtocolException(
        'CREDIT of ${bytes.length} bytes, $encodedLength expected',
      );
    }
    final reader = ByteReader(bytes);
    return MuxCredit(reader.u48('channel id'), reader.u32('bytes'));
  }

  @override
  bool operator ==(Object other) =>
      other is MuxCredit &&
      other.channelId == channelId &&
      other.bytes == bytes;

  @override
  int get hashCode => Object.hash(channelId, bytes);

  @override
  String toString() => 'MuxCredit($channelId, $bytes)';
}
