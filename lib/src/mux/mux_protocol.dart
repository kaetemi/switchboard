/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Opus 5.5 <noreply@anthropic.com>
  GPT-6 Astra <noreply@openai.com>
*/

/// What every binding of a connection carries identically, whatever its
/// framing (the frame mux, a stream binding such as QUIC): the `IDENT`
/// payload and its verification (the proof bound to the nonces, or also to
/// the TLS exporter a stream binding supplies), the bound on control
/// payloads, and the rules for the status of a channel CLOSE or a GOAWAY.
///
/// The frame mux's own wire format (frame header, LIMITS, CREDIT) is in
/// `mux_frame.dart`, which re-exports this library.
library;

import 'dart:convert';
import 'dart:typed_data';

import 'package:clock/clock.dart';
import 'package:meta/meta.dart';

import '../bytes.dart';
import '../identity/credential_verifier.dart';
import '../identity/ed25519.dart';
import '../identity/peer_identity.dart';
import '../status.dart';

/// Largest control payload (after the type byte) of `PING`, `PONG`,
/// `GOAWAY`, `NONCE` and `IDENT`: 1024 bytes, and the bound this
/// implementation keeps every CLOSE and GOAWAY status payload within.
/// The frame mux's `MuxControlMessage.maxControlPayload`.
const int maxControlPayloadLength = 1024;

/// Throws [ArgumentError] if [status] carries an application code (256
/// and above), which must not be used in a channel CLOSE or a GOAWAY,
/// whichever binding carries them.
void checkMuxStatus(Status status) {
  if (status.code < 0 || status.isApplicationCode) {
    throw ArgumentError.value(
      status,
      'status',
      'application status codes are not allowed in mux CLOSE or GOAWAY',
    );
  }
}

/// [status] with its reason shortened so that its encoded payload (the
/// `u16` code and the bytes after it) is at most [maxPayload] bytes.
///
/// Cuts the encoded bytes on a UTF-8 character boundary, so that fixed
/// fields a code places before its reason (`MOVED` and `RELOCATED`: 10
/// bytes) are kept whole; when they do not fit, everything
/// after the code is dropped. Returns [status] itself when it fits; drops
/// the reason entirely when [maxPayload] leaves no room for it.
Status truncateStatus(Status status, int maxPayload) {
  if (status.reason.isEmpty) {
    return status;
  }
  final room = maxPayload - 2;
  if (room <= 0) {
    return Status(status.code);
  }
  // The bytes after the code are at most 3 per UTF-16 code unit of the
  // reason (a replaced malformed sequence is at most 3 bytes as well).
  if (status.reason.length * 3 <= room) {
    return status;
  }
  final encoded = status.encode();
  if (encoded.length - 2 <= room) {
    return status;
  }
  var cut = 2 + room;
  // Back off continuation bytes (10xxxxxx), at most the 3 of the longest
  // character, so the cut lands on the first byte of a character.
  for (var i = 0; i < 3 && cut > 2 && encoded[cut] & 0xC0 == 0x80; i++) {
    cut--;
  }
  final code = status.known;
  if ((code == StatusCode.moved || code == StatusCode.relocated) &&
      cut < 2 + _slotMoveFields) {
    return Status(status.code);
  }
  return Status.decode(Uint8List.sublistView(encoded, 0, cut));
}

/// Fixed fields of `MOVED` and `RELOCATED` before the reason: `u48 owner`,
/// `u32 epoch` (`MovedStatus.fieldsLength`).
const int _slotMoveFields = 10;

/// [status] shortened (see [truncateStatus]) so that a frame carrying it
/// after [overhead] bytes of headers fits both the 1024 byte bound this
/// implementation keeps every CLOSE and GOAWAY status payload within and
/// the peer's announced frame limit [peerMaxFrameSize] (0 when unknown).
/// The local-only `CONNECTION_LOST` code becomes `UNAVAILABLE` on the wire;
/// the bytes after the code are kept as they are, as for any relayed status.
@internal
Status fitStatus(Status status, int overhead, int peerMaxFrameSize) {
  if (status.known == StatusCode.connectionLost) {
    final bytes = status.encode();
    bytes[0] = StatusCode.unavailable.code & 0xFF;
    bytes[1] = StatusCode.unavailable.code >> 8;
    status = Status.decode(bytes);
  }
  var max = maxControlPayloadLength;
  if (peerMaxFrameSize > 0 && peerMaxFrameSize - overhead < max) {
    max = peerMaxFrameSize - overhead;
  }
  return truncateStatus(status, max);
}

/// A CLOSE or GOAWAY status for a binding's wire: application codes are
/// refused, `CONNECTION_LOST` becomes `UNAVAILABLE`, and the payload is
/// shortened to [maxControlPayloadLength] bytes without changing fixed
/// fields or splitting a UTF-8 character. [status] itself is not mutated.
Status statusForWire(Status status) {
  checkMuxStatus(status);
  return fitStatus(status, 0, 0);
}

/// Maps a status received in CLOSE or GOAWAY for local use: application
/// codes (256 and above), which the peer must not send there, become
/// [StatusCode.unknown] with the original code kept in the reason, so the
/// status can be relayed in a CLOSE of our own.
Status statusFromWire(Status status) => status.isApplicationCode
    ? Status.of(StatusCode.unknown, status.toString())
    : status;

/// Decodes a status payload carried by CLOSE or GOAWAY, mapping malformed
/// payloads to [ProtocolException]. [what] names the carrier for the error.
Status decodeStatusPayload(Uint8List payload, String what) {
  try {
    return Status.decode(payload);
  } on FormatException catch (e) {
    throw ProtocolException('malformed $what status: ${e.message}');
  }
}

/// The payload of an IDENT control message: `u16` credential length, the
/// credential, `len8` intent, `len8` receiver, then the proof (the rest).
///
/// See the wiki page "Polyverse Switchboard Identity and Credentials", section
/// "Connection identity". The credential is carried as bytes; the mux
/// connection decodes and verifies it with its verifier.
class MuxIdent {
  /// An IDENT payload. [intent], [receiver] and [proof] default to empty
  /// (no intent, no named receiver, a bearer credential). The lists are
  /// used as given, not copied.
  MuxIdent({
    required this.credential,
    Uint8List? intent,
    Uint8List? receiver,
    Uint8List? proof,
  }) : intent = intent ?? Uint8List(0),
       receiver = receiver ?? Uint8List(0),
       proof = proof ?? Uint8List(0);

  /// Length of a NONCE.
  static const int nonceLength = 32;

  /// Longest intent.
  static const int maxIntentLength = 64;

  /// Longest receiver: an identity string of a credential.
  static const int maxReceiverLength = 255;

  /// Length of a proof: an Ed25519 signature.
  static const int proofLength = 64;

  /// The label the proof starts with: `SWBIDENT` in ASCII.
  static final Uint8List proofLabel = Uint8List.fromList('SWBIDENT'.codeUnits);

  /// The label an exporter-bound proof starts with instead: `SWBIDTLS` in
  /// ASCII. The two labels keep a signature of one form from verifying as
  /// the other.
  static final Uint8List exporterProofLabel = Uint8List.fromList(
    'SWBIDTLS'.codeUnits,
  );

  /// Length of the TLS exporter an exporter-bound proof signs.
  static const int exporterLength = 32;

  /// The encoded credential.
  final Uint8List credential;

  /// Opaque intent, at most [maxIntentLength] bytes; empty unless the
  /// connection answers a `CONNECT`.
  final Uint8List intent;

  /// The identity (UTF-8) the sender expects the receiver to have, at most
  /// [maxReceiverLength] bytes; empty when the sender names none.
  final Uint8List receiver;

  /// The holder's Ed25519 signature of [proofMessage], or empty for a
  /// bearer credential.
  final Uint8List proof;

  /// What the holder signs: [proofLabel], the nonce the IDENT's sender sent
  /// ([senderNonce]), the nonce its receiver sent ([receiverNonce]), then
  /// the intent and the receiver as the IDENT carries them, each with its
  /// `len8` length, so that no byte can move from one field to the other.
  ///
  /// With an [exporter] (a binding that binds the proof to its TLS session,
  /// such as the QUIC and WebTransport binding when both sides can compute
  /// the exporter), the message is the exporter-bound one instead:
  /// [exporterProofLabel], the two nonces, the [exporterLength] bytes of
  /// the exporter, then the intent and the receiver as above. The frame
  /// mux never passes one.
  ///
  /// Throws [ArgumentError] for an [intent] or [receiver] longer than a
  /// `len8` field can carry, and for an [exporter] that is not
  /// [exporterLength] bytes long.
  static Uint8List proofMessage(
    List<int> senderNonce,
    List<int> receiverNonce,
    List<int> intent,
    List<int> receiver, {
    List<int>? exporter,
  }) {
    if (intent.length > 0xFF || receiver.length > 0xFF) {
      throw ArgumentError('intent or receiver longer than 255 bytes');
    }
    if (exporter != null && exporter.length != exporterLength) {
      throw ArgumentError.value(
        exporter.length,
        'exporter',
        'an exporter is $exporterLength bytes',
      );
    }
    final builder = BytesBuilder(copy: false)
      ..add(exporter == null ? proofLabel : exporterProofLabel)
      ..add(senderNonce)
      ..add(receiverNonce);
    if (exporter != null) {
      builder.add(exporter);
    }
    return (builder
          ..addByte(intent.length)
          ..add(intent)
          ..addByte(receiver.length)
          ..add(receiver))
        .toBytes();
  }

  /// Encodes the payload (after the control type byte).
  ///
  /// Throws [ArgumentError] for an intent over [maxIntentLength] bytes, a
  /// receiver over [maxReceiverLength] bytes, and a payload over
  /// [maxControlPayloadLength] bytes.
  Uint8List encode() {
    if (intent.length > maxIntentLength) {
      throw ArgumentError.value(
        intent.length,
        'intent',
        'at most $maxIntentLength bytes',
      );
    }
    if (receiver.length > maxReceiverLength) {
      throw ArgumentError.value(
        receiver.length,
        'receiver',
        'at most $maxReceiverLength bytes',
      );
    }
    final length =
        2 +
        credential.length +
        1 +
        intent.length +
        1 +
        receiver.length +
        proof.length;
    if (length > maxControlPayloadLength) {
      throw ArgumentError(
        'IDENT of $length bytes exceeds the control payload limit of '
        '$maxControlPayloadLength',
      );
    }
    return (ByteWriter(length)
          ..u16(credential.length)
          ..bytes(credential)
          ..u8(intent.length)
          ..bytes(intent)
          ..u8(receiver.length)
          ..bytes(receiver)
          ..bytes(proof))
        .toBytes();
  }

  /// Decodes an IDENT payload. The fields are views into [bytes].
  ///
  /// Throws [ProtocolException] on truncation and for an intent over
  /// [maxIntentLength] bytes. The proof is whatever follows the receiver;
  /// its length is checked when the credential is verified, and the
  /// receiver (any bytes) when it is compared with the receiver's
  /// identity.
  static MuxIdent decode(Uint8List bytes) {
    final reader = ByteReader(bytes);
    try {
      final credential = reader.take(
        reader.u16('credential length'),
        'credential',
      );
      final intentLength = reader.u8('intent length');
      if (intentLength > maxIntentLength) {
        throw ProtocolException('IDENT intent of $intentLength bytes');
      }
      final intent = reader.take(intentLength, 'intent');
      final receiver = reader.take(reader.u8('receiver length'), 'receiver');
      return MuxIdent(
        credential: credential,
        intent: intent,
        receiver: receiver,
        proof: reader.rest(),
      );
    } on FormatException catch (e) {
      throw ProtocolException('malformed IDENT: ${e.message}');
    }
  }

  @override
  String toString() =>
      'MuxIdent(credential of ${credential.length} bytes, '
      'intent ${hexString(intent)}, receiver of ${receiver.length} bytes, '
      'proof of ${proof.length} bytes)';
}

/// Verifies [ident], an `IDENT` the peer sent, as every binding must (wiki
/// page "Polyverse Switchboard Identity and Credentials", section
/// "Connection identity"), and returns the identity it establishes.
///
/// The checks: a receiver named in the `IDENT` must be [localIdentity]
/// (this side's identity; null for a side without a credential, which
/// refuses every `IDENT` that names a receiver), and with
/// [requireNamedIdent] it must name one; the credential must pass
/// [verifier]; for a credential with a holder key, the proof must be the
/// holder's Ed25519 signature of [MuxIdent.proofMessage] over
/// [senderNonce] (the peer's `NONCE`), [receiverNonce] (this side's
/// `NONCE`), the intent and the receiver, while a bearer credential must
/// carry no proof. The envelope rules (both nonces exchanged, lengths) are
/// the caller's: [MuxIdent.decode] checks the lengths.
///
/// With an [exporter] the proof must be the exporter-bound one
/// ([MuxIdent.proofMessage] with that exporter): a binding that binds
/// identification to its TLS session passes the session's exporter when
/// both sides announced they can compute it, and a nonce-bound proof then
/// fails, as an exporter-bound one fails without it. Throws
/// [ArgumentError] for an [exporter] that is not
/// [MuxIdent.exporterLength] bytes long. The frame mux never passes one.
///
/// The identity's [PeerIdentity.verifiedAt] is the time of
/// `package:clock`. Throws [SwitchboardException] with
/// [StatusCode.unauthenticated] and the reason when the `IDENT` is refused,
/// or with the status [verifier] refuses the credential with; the reason
/// is for logs, a binding ends the connection with a generic status (the
/// frame mux: GOAWAY `UNAUTHENTICATED`, "identification failed"). Other
/// errors of [verifier] propagate.
Future<PeerIdentity> verifyIdent(
  MuxIdent ident, {
  required CredentialVerifier verifier,
  required Uint8List senderNonce,
  required Uint8List receiverNonce,
  String? localIdentity,
  bool requireNamedIdent = false,
  Uint8List? exporter,
}) async {
  if (exporter != null && exporter.length != MuxIdent.exporterLength) {
    throw ArgumentError.value(
      exporter.length,
      'exporter',
      'an exporter is ${MuxIdent.exporterLength} bytes',
    );
  }
  final problem = identReceiverProblem(
    ident.receiver,
    localIdentity: localIdentity,
    requireNamedIdent: requireNamedIdent,
  );
  if (problem != null) {
    throw SwitchboardException.of(StatusCode.unauthenticated, problem);
  }
  final credential = await verifier.verify(
    Uint8List.fromList(ident.credential),
  );
  final holder = credential.holderKey;
  if (holder == null) {
    if (ident.proof.isNotEmpty) {
      throw SwitchboardException.of(
        StatusCode.unauthenticated,
        'a proof with a bearer credential',
      );
    }
  } else if (ident.proof.length != MuxIdent.proofLength ||
      !await ed25519Verify(
        holder,
        MuxIdent.proofMessage(
          senderNonce,
          receiverNonce,
          ident.intent,
          ident.receiver,
          exporter: exporter,
        ),
        ident.proof,
      )) {
    throw SwitchboardException.of(
      StatusCode.unauthenticated,
      'proof of possession does not verify',
    );
  }
  return PeerIdentity(
    credential: credential,
    intent: Uint8List.fromList(ident.intent),
    verifiedAt: clock.now(),
    // Checked against this side's identity: valid UTF-8 when not empty.
    receiver: utf8.decode(ident.receiver, allowMalformed: true),
  );
}

/// Why an `IDENT` naming [receiver] is refused by a side whose identity is
/// [localIdentity], or null when it is not: a receiver other than
/// [localIdentity], or none while [requireNamedIdent]. The first check of
/// [verifyIdent], which a binding may make before anything asynchronous.
String? identReceiverProblem(
  Uint8List receiver, {
  String? localIdentity,
  bool requireNamedIdent = false,
}) {
  if (receiver.isEmpty) {
    return requireNamedIdent ? 'the IDENT names no receiver' : null;
  }
  if (localIdentity == null ||
      !_bytesEqual(utf8.encode(localIdentity), receiver)) {
    return 'the IDENT names another receiver '
        '("${utf8.decode(receiver, allowMalformed: true)}")';
  }
  return null;
}

bool _bytesEqual(List<int> a, List<int> b) {
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
