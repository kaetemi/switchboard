/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Opus 5.5 <noreply@anthropic.com>
*/

import 'dart:typed_data';

import '../bytes.dart';
import '../name.dart';
import '../status.dart';
import 'talk_bulk.dart';

/// The kind of a Talk message, bits `0x30` of the flags byte.
///
/// The enum index is the wire value: `(flags & 0x30) >> 4`.
enum TalkKind {
  /// Kind 0: plain message, request, final response or chained response.
  message,

  /// Kind 1: a non-final response to a `STREAM` request.
  streamItem,

  /// Kind 2: abort response, cancel, or channel abort, by the ids present.
  abort,

  /// Kind 3: control of an outstanding request, by either side. From the
  /// responder (a response id): a response timeout extension, a deadline
  /// and a renewal it declares, or, empty, a restart of the requester's
  /// default timeout. From the requester (a request id of its own): a
  /// credit, more items of the item window of its stream request
  /// ([TalkFrame.isCredit]).
  extend,
}

/// One Talk message header plus payload, as carried in one mux subframe.
///
/// A [requestId] or [responseId] of 0 means the field is absent (0 is never
/// a valid id on the wire). A null [procedure] means the procedure field is
/// absent, which receivers treat as the all-zero name. With [bulk] the
/// payload field is a [TalkBulkReference] to a bulk channel carrying the
/// payload (see [TalkBulkReference]). A stream request ([stream]) carries
/// its item [window] after the ids.
///
/// See the wiki page "Polyverse Switchboard Talk" for the layout and the flag rules.
class TalkFrame {
  /// Creates a frame. Nothing is validated until [encode].
  TalkFrame({
    required this.kind,
    this.procedure,
    this.requestId = 0,
    this.responseId = 0,
    this.stream = false,
    this.window = unlimitedWindow,
    this.bulk = false,
    Uint8List? payload,
  }) : payload = payload ?? _emptyPayload;

  /// Flag bit: an 8-byte procedure name follows.
  static const int flagHasProcedure = 0x01;

  /// Flag bit: a request id follows; the sender expects a response.
  static const int flagHasRequest = 0x02;

  /// Flag bit: a response id follows; this responds to the peer's request.
  static const int flagHasResponse = 0x04;

  /// Flag bit: with [flagHasRequest], the sender accepts stream items, and
  /// a `u32` item window follows the ids ([window]).
  static const int flagStream = 0x08;

  /// Mask of the kind field.
  static const int kindMask = 0x30;

  /// Flag bit: the payload is on a bulk channel; the payload field is a
  /// [TalkBulkReference]. Valid on [TalkKind.message] and
  /// [TalkKind.streamItem].
  static const int flagBulk = 0x40;

  /// Reserved bit; receiving it set is a channel protocol error.
  static const int reservedMask = 0x80;

  /// Largest valid request or response id (`u24`).
  static const int maxId = 0xFFFFFF;

  /// Length of a non-empty `EXTEND` payload: `u32 deadline ms`,
  /// `u32 renew ms`.
  static const int extendPayloadLength = 8;

  /// Largest value of an `EXTEND` field, in milliseconds (`u32`).
  static const int maxExtendMillis = 0xFFFFFFFF;

  /// The item [window] of a stream request that takes any number of items:
  /// no accounting, the channel's flow control alone bounds the stream.
  /// Finite windows are 0 to `unlimitedWindow - 1`.
  static const int unlimitedWindow = 0xFFFFFFFF;

  /// Length of a credit's payload ([isCredit]): `u32 items`.
  static const int creditPayloadLength = 4;

  static final Uint8List _emptyPayload = Uint8List(0);

  /// Message kind.
  final TalkKind kind;

  /// Procedure name, or null when the field is absent.
  final Name? procedure;

  /// The sender's request id, or 0 if the sender expects no response.
  final int requestId;

  /// The peer's request id this frame responds to, or 0 if not a response.
  final int responseId;

  /// The `STREAM` flag: the sender accepts `STREAM_ITEM` responses.
  final bool stream;

  /// The item window of a stream request ([stream]): how many
  /// `STREAM_ITEM`s the requester is ready to take before it grants more
  /// with a credit ([isCredit]), 0 for none until a grant, or
  /// [unlimitedWindow]. On the wire only with [stream]; any other frame
  /// keeps the default, [unlimitedWindow]. See the wiki page "Polyverse
  /// Switchboard Talk", section "Item windows".
  final int window;

  /// The `BULK` flag: [payload] is a [TalkBulkReference], and the payload
  /// itself travels on the bulk channel it names.
  final bool bulk;

  /// The payload. After [decode] this is a view into the decoded buffer.
  /// With [bulk], the 10-byte bulk reference ([bulkReference]).
  final Uint8List payload;

  /// The bulk reference a [bulk] frame carries as its payload. Throws
  /// [StateError] for a frame without [bulk], and [ProtocolException] for
  /// a malformed reference (which [decode] never returns).
  TalkBulkReference get bulkReference {
    if (!bulk) {
      throw StateError('only BULK frames carry a bulk reference');
    }
    return TalkBulkReference.decode(payload);
  }

  /// A copy of this frame with [bulk] set and [reference] as its payload.
  TalkFrame withBulk(TalkBulkReference reference) => TalkFrame(
    kind: kind,
    procedure: procedure,
    requestId: requestId,
    responseId: responseId,
    stream: stream,
    window: window,
    bulk: true,
    payload: reference.encode(),
  );

  /// True when [requestId] is present.
  bool get hasRequest => requestId != 0;

  /// True when [responseId] is present.
  bool get hasResponse => responseId != 0;

  /// True for a credit: an `EXTEND` from the requester ([requestId], its own
  /// stream request), granting the responder [creditItems] more items of
  /// that request's item window.
  bool get isCredit => kind == TalkKind.extend && hasRequest;

  /// The number of items a credit ([isCredit]) grants, at least 1. Throws
  /// [StateError] for any other frame.
  int get creditItems {
    if (!isCredit) {
      throw StateError('only credits carry an item count');
    }
    return ByteData.sublistView(payload).getUint32(0, Endian.little);
  }

  /// The payload of a credit granting [items] more items (`u32`, little
  /// endian). Throws [ArgumentError] unless [items] is 1 to 0xFFFFFFFF.
  static Uint8List creditPayload(int items) {
    if (items < 1 || items > 0xFFFFFFFF) {
      throw ArgumentError.value(items, 'items', 'must be 1 to 0xFFFFFFFF');
    }
    final out = Uint8List(creditPayloadLength);
    ByteData.sublistView(out).setUint32(0, items, Endian.little);
    return out;
  }

  /// The flags byte this frame encodes to.
  int get flags =>
      (procedure != null ? flagHasProcedure : 0) |
      (hasRequest ? flagHasRequest : 0) |
      (hasResponse ? flagHasResponse : 0) |
      (stream ? flagStream : 0) |
      (kind.index << 4) |
      (bulk ? flagBulk : 0);

  /// The status carried by an [TalkKind.abort] frame.
  ///
  /// An empty payload means [StatusCode.unknown] (unlike a mux CLOSE, where
  /// it means OK). Throws [StateError] for other kinds.
  Status get status {
    if (kind != TalkKind.abort) {
      throw StateError('only abort frames carry a status');
    }
    if (payload.isEmpty) {
      return Status.of(StatusCode.unknown);
    }
    return Status.decode(payload);
  }

  /// The values carried by an [TalkKind.extend] frame from the responder:
  /// the deadline and the renewal it declares, each null when the field is
  /// 0 ("unchanged"). Both are null for an empty payload, which restarts
  /// the requester's default timeout; [payload] tells the two apart. Throws
  /// [StateError] for other kinds, and for a credit ([isCredit]).
  ({Duration? deadline, Duration? renew}) get extension {
    if (kind != TalkKind.extend) {
      throw StateError('only EXTEND frames carry an extension');
    }
    if (hasRequest) {
      throw StateError('a credit carries no extension');
    }
    if (payload.isEmpty) {
      return (deadline: null, renew: null);
    }
    final data = ByteData.sublistView(payload);
    Duration? field(int offset) {
      final ms = data.getUint32(offset, Endian.little);
      return ms == 0 ? null : Duration(milliseconds: ms);
    }

    return (deadline: field(0), renew: field(4));
  }

  /// The payload of an `EXTEND` declaring [deadline] and [renew]: each in
  /// whole milliseconds, rounded up (a value under 1 ms is sent as 1 ms,
  /// since 0 means "unchanged") and capped at [maxExtendMillis]; null is
  /// sent as 0. Empty when both are null.
  ///
  /// Throws [ArgumentError] for a negative value.
  static Uint8List extendPayload({Duration? deadline, Duration? renew}) {
    if (deadline == null && renew == null) {
      return _emptyPayload;
    }
    final out = Uint8List(extendPayloadLength);
    final data = ByteData.sublistView(out);
    data.setUint32(0, _extendMillis(deadline, 'deadline'), Endian.little);
    data.setUint32(4, _extendMillis(renew, 'renew'), Endian.little);
    return out;
  }

  static int _extendMillis(Duration? value, String name) {
    if (value == null) {
      return 0;
    }
    if (value.isNegative) {
      throw ArgumentError.value(value, name, 'must not be negative');
    }
    final ms = (value.inMicroseconds + 999) ~/ 1000;
    return ms < 1 ? 1 : (ms > maxExtendMillis ? maxExtendMillis : ms);
  }

  /// Encodes to the short canonical form: optional fields are present only
  /// when set, the item [window] whenever [stream] is (unlimited
  /// included).
  ///
  /// Throws [ArgumentError] if the frame breaks a rule a receiver would
  /// reject as a protocol error, so that this implementation never sends
  /// such a frame.
  Uint8List encode() {
    if (requestId < 0 || requestId > maxId) {
      throw ArgumentError.value(requestId, 'requestId', 'must fit in u24');
    }
    if (responseId < 0 || responseId > maxId) {
      throw ArgumentError.value(responseId, 'responseId', 'must fit in u24');
    }
    if (window < 0 || window > unlimitedWindow) {
      throw ArgumentError.value(window, 'window', 'must fit in u32');
    }
    if (!stream && window != unlimitedWindow) {
      throw ArgumentError.value(window, 'window', 'without STREAM');
    }
    final error = _ruleViolation(
      kind: kind,
      hasProcedure: procedure != null,
      hasRequest: hasRequest,
      hasResponse: hasResponse,
      stream: stream,
      bulk: bulk,
      payloadLength: payload.length,
    );
    if (error != null) {
      throw ArgumentError(error);
    }
    if (isCredit && creditItems == 0) {
      throw ArgumentError('credit of 0 items');
    }
    if (bulk) {
      try {
        TalkBulkReference.decode(payload);
      } on ProtocolException catch (e) {
        throw ArgumentError(e.status.reason);
      }
    }
    final length =
        1 +
        (procedure != null ? Name.byteLength : 0) +
        (hasRequest ? 3 : 0) +
        (hasResponse ? 3 : 0) +
        (stream ? 4 : 0) +
        payload.length;
    final out = Uint8List(length);
    out[0] = flags;
    var offset = 1;
    final name = procedure;
    if (name != null) {
      name.writeTo(out, offset);
      offset += Name.byteLength;
    }
    if (hasRequest) {
      out[offset++] = requestId & 0xFF;
      out[offset++] = (requestId >> 8) & 0xFF;
      out[offset++] = (requestId >> 16) & 0xFF;
    }
    if (hasResponse) {
      out[offset++] = responseId & 0xFF;
      out[offset++] = (responseId >> 8) & 0xFF;
      out[offset++] = (responseId >> 16) & 0xFF;
    }
    if (stream) {
      ByteData.sublistView(out).setUint32(offset, window, Endian.little);
      offset += 4;
    }
    out.setRange(offset, length, payload);
    return out;
  }

  /// Decodes one Talk message.
  ///
  /// Throws [ProtocolException] for a truncated header (a stream request
  /// without its item window among them), the reserved bit, an invalid
  /// flag combination, an invalid procedure name, or a violation of the
  /// kind rules (an `EXTEND` with both ids or neither, an extend payload
  /// that is neither empty nor [extendPayloadLength] bytes long, a credit
  /// payload that is not [creditPayloadLength] bytes long or grants 0
  /// items, `BULK` on an `ABORT` or an `EXTEND`, and a bulk reference that
  /// is not a valid [TalkBulkReference] among them).
  static TalkFrame decode(Uint8List bytes) {
    final reader = ByteReader(bytes);
    try {
      final flags = reader.u8('flags');
      if (flags & reservedMask != 0) {
        throw ProtocolException(
          'reserved talk flag bits set: 0x${flags.toRadixString(16)}',
        );
      }
      final kind = TalkKind.values[(flags & kindMask) >> 4];
      final hasProcedure = flags & flagHasProcedure != 0;
      final hasRequest = flags & flagHasRequest != 0;
      final hasResponse = flags & flagHasResponse != 0;
      final stream = flags & flagStream != 0;
      final bulk = flags & flagBulk != 0;
      final procedure = hasProcedure ? reader.name('procedure') : null;
      final requestId = hasRequest ? reader.u24('request id') : 0;
      if (hasRequest && requestId == 0) {
        throw ProtocolException('request id 0');
      }
      final responseId = hasResponse ? reader.u24('response id') : 0;
      if (hasResponse && responseId == 0) {
        throw ProtocolException('response id 0');
      }
      final streamError = _streamViolation(kind, hasRequest);
      if (stream && streamError != null) {
        throw ProtocolException(streamError);
      }
      final window = stream ? reader.u32('item window') : unlimitedWindow;
      final payload = reader.rest();
      final error = _ruleViolation(
        kind: kind,
        hasProcedure: hasProcedure,
        hasRequest: hasRequest,
        hasResponse: hasResponse,
        stream: stream,
        bulk: bulk,
        payloadLength: payload.length,
      );
      if (error != null) {
        throw ProtocolException(error);
      }
      if (bulk) {
        TalkBulkReference.decode(payload);
      }
      final frame = TalkFrame(
        kind: kind,
        procedure: procedure,
        requestId: requestId,
        responseId: responseId,
        stream: stream,
        window: window,
        bulk: bulk,
        payload: payload,
      );
      if (frame.isCredit && frame.creditItems == 0) {
        throw ProtocolException('credit of 0 items');
      }
      return frame;
    } on FormatException catch (e) {
      throw ProtocolException(e.message);
    }
  }

  /// The flag and kind rules shared by [encode] and [decode]. Returns a
  /// description of the first violation, or null if the combination is
  /// valid.
  static String? _ruleViolation({
    required TalkKind kind,
    required bool hasProcedure,
    required bool hasRequest,
    required bool hasResponse,
    required bool stream,
    required bool bulk,
    required int payloadLength,
  }) {
    if (stream) {
      final error = _streamViolation(kind, hasRequest);
      if (error != null) {
        return error;
      }
    }
    if (bulk) {
      if (kind == TalkKind.abort || kind == TalkKind.extend) {
        return 'BULK on ${kind == TalkKind.abort ? 'ABORT' : 'EXTEND'}';
      }
      if (payloadLength != TalkBulkReference.encodedLength) {
        return 'BULK reference of $payloadLength bytes';
      }
    }
    switch (kind) {
      case TalkKind.message:
        if (!hasResponse && !hasProcedure) {
          return hasRequest
              ? 'request without procedure'
              : 'plain message without procedure';
        }
      case TalkKind.streamItem:
        if (!hasResponse) {
          return 'STREAM_ITEM without HAS_RESPONSE';
        }
      case TalkKind.abort:
        if (hasRequest && hasResponse) {
          return 'ABORT with both request and response id';
        }
        if (payloadLength == 1) {
          return 'ABORT payload is not a status';
        }
      case TalkKind.extend:
        if (hasRequest && hasResponse) {
          return 'EXTEND with both request and response id';
        }
        if (!hasRequest && !hasResponse) {
          return 'EXTEND without an id';
        }
        if (hasRequest) {
          if (payloadLength != creditPayloadLength) {
            return 'credit payload of $payloadLength bytes';
          }
        } else if (payloadLength != 0 && payloadLength != extendPayloadLength) {
          return 'EXTEND payload of $payloadLength bytes';
        }
    }
    return null;
  }

  /// Where the `STREAM` flag may not be: without a request id, and on an
  /// `ABORT` or an `EXTEND`, whose request id names an outstanding request
  /// (a cancel, a credit) instead of opening one. Returns a description of
  /// the violation, or null.
  static String? _streamViolation(TalkKind kind, bool hasRequest) {
    if (!hasRequest) {
      return 'STREAM without HAS_REQUEST';
    }
    if (kind == TalkKind.abort || kind == TalkKind.extend) {
      return 'STREAM on ${kind == TalkKind.abort ? 'ABORT' : 'EXTEND'}';
    }
    return null;
  }

  @override
  bool operator ==(Object other) {
    if (other is! TalkFrame ||
        other.kind != kind ||
        other.procedure != procedure ||
        other.requestId != requestId ||
        other.responseId != responseId ||
        other.stream != stream ||
        other.window != window ||
        other.bulk != bulk ||
        other.payload.length != payload.length) {
      return false;
    }
    for (var i = 0; i < payload.length; i++) {
      if (other.payload[i] != payload[i]) {
        return false;
      }
    }
    return true;
  }

  @override
  int get hashCode => Object.hash(
    kind,
    procedure,
    requestId,
    responseId,
    stream,
    window,
    bulk,
    payload.length,
  );

  @override
  String toString() {
    final parts = <String>[kind.name];
    if (procedure != null) {
      parts.add('"$procedure"');
    }
    if (hasRequest) {
      parts.add('req=$requestId');
    }
    if (hasResponse) {
      parts.add('resp=$responseId');
    }
    if (stream) {
      parts.add(
        window == unlimitedWindow ? 'stream' : 'stream, window=$window',
      );
    }
    if (bulk) {
      parts.add('bulk');
    }
    parts.add('${payload.length} bytes');
    return 'TalkFrame(${parts.join(', ')})';
  }
}
