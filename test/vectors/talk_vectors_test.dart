/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Opus 5.5 <noreply@anthropic.com>
*/

import 'dart:typed_data';

import 'package:switchboard/src/bytes.dart';
import 'package:switchboard/src/name.dart';
import 'package:switchboard/src/status.dart';
import 'package:switchboard/src/talk/talk_bulk.dart';
import 'package:switchboard/src/talk/talk_frame.dart';
import 'package:test/test.dart';

/// Positive vectors from the wiki page "Polyverse Switchboard Test Vectors", section
/// "Talk messages", with the frame each one must decode to.
final List<(String, String, TalkFrame)> positive = [
  (
    'Plain message "HELLO", payload 01',
    '01 48 45 4C 4C 4F 00 00 00 01',
    TalkFrame(
      kind: TalkKind.message,
      procedure: Name('HELLO'),
      payload: hexBytes('01'),
    ),
  ),
  (
    'Request "GET_PROF" id 1, empty payload',
    '03 47 45 54 5F 50 52 4F 46 01 00 00',
    TalkFrame(
      kind: TalkKind.message,
      procedure: Name('GET_PROF'),
      requestId: 1,
    ),
  ),
  (
    'Final response to 1, no procedure, payload FF',
    '04 01 00 00 FF',
    TalkFrame(kind: TalkKind.message, responseId: 1, payload: hexBytes('FF')),
  ),
  (
    'Stream request "LIST" id 2, unlimited window',
    '0B 4C 49 53 54 00 00 00 00 02 00 00 FF FF FF FF',
    TalkFrame(
      kind: TalkKind.message,
      procedure: Name('LIST'),
      requestId: 2,
      stream: true,
    ),
  ),
  (
    'Stream request "LIST" id 2, window 16',
    '0B 4C 49 53 54 00 00 00 00 02 00 00 10 00 00 00',
    TalkFrame(
      kind: TalkKind.message,
      procedure: Name('LIST'),
      requestId: 2,
      stream: true,
      window: 16,
    ),
  ),
  (
    'Stream item "ITEM" to 2, payload 07',
    '15 49 54 45 4D 00 00 00 00 02 00 00 07',
    TalkFrame(
      kind: TalkKind.streamItem,
      procedure: Name('ITEM'),
      responseId: 2,
      payload: hexBytes('07'),
    ),
  ),
  (
    'End of stream (final response) to 2, empty',
    '04 02 00 00',
    TalkFrame(kind: TalkKind.message, responseId: 2),
  ),
  (
    'Abort response to 1, NOT_FOUND, "nope"',
    '24 01 00 00 05 00 6E 6F 70 65',
    TalkFrame(
      kind: TalkKind.abort,
      responseId: 1,
      payload: Status.of(StatusCode.notFound, 'nope').encode(),
    ),
  ),
  (
    'Extend response to 1 (restart the default timeout)',
    '34 01 00 00',
    TalkFrame(kind: TalkKind.extend, responseId: 1),
  ),
  (
    'Extend response to 1, deadline 900000 ms, renew 300000 ms',
    '34 01 00 00 A0 BB 0D 00 E0 93 04 00',
    TalkFrame(
      kind: TalkKind.extend,
      responseId: 1,
      payload: TalkFrame.extendPayload(
        deadline: const Duration(minutes: 15),
        renew: const Duration(minutes: 5),
      ),
    ),
  ),
  (
    'Extend response to 1, deadline 900000 ms, renew unchanged',
    '34 01 00 00 A0 BB 0D 00 00 00 00 00',
    TalkFrame(
      kind: TalkKind.extend,
      responseId: 1,
      payload: TalkFrame.extendPayload(deadline: const Duration(minutes: 15)),
    ),
  ),
  (
    'Credit for own stream request 2, 8 more items',
    '32 02 00 00 08 00 00 00',
    TalkFrame(
      kind: TalkKind.extend,
      requestId: 2,
      payload: TalkFrame.creditPayload(8),
    ),
  ),
  (
    'The same credit forwarded by a message chain proxy whose own request '
        'toward the backend is 5',
    '32 05 00 00 08 00 00 00',
    TalkFrame(
      kind: TalkKind.extend,
      requestId: 5,
      payload: TalkFrame.creditPayload(8),
    ),
  ),
  (
    'Cancel own request 2',
    '22 02 00 00 01 00',
    TalkFrame(
      kind: TalkKind.abort,
      requestId: 2,
      payload: Status.of(StatusCode.cancelled).encode(),
    ),
  ),
  (
    'Channel abort, UNAUTHENTICATED, no reason',
    '20 10 00',
    TalkFrame(
      kind: TalkKind.abort,
      payload: Status.of(StatusCode.unauthenticated).encode(),
    ),
  ),
  (
    'Chained: response to 1 that is request 7 "MORE"',
    '07 4D 4F 52 45 00 00 00 00 07 00 00 01 00 00',
    TalkFrame(
      kind: TalkKind.message,
      procedure: Name('MORE'),
      requestId: 7,
      responseId: 1,
    ),
  ),
  (
    'Request id 0xFFFFFF "X"',
    '03 58 00 00 00 00 00 00 00 FF FF FF',
    TalkFrame(
      kind: TalkKind.message,
      procedure: Name('X'),
      requestId: 0xFFFFFF,
    ),
  ),
  (
    'Request "PUT" id 1 with a bulk payload: bulk number 1, 1048576 bytes',
    '43 50 55 54 00 00 00 00 00 01 00 00 01 00 00 00 00 00 10 00 00 00',
    TalkFrame(
      kind: TalkKind.message,
      procedure: Name('PUT'),
      requestId: 1,
      bulk: true,
      payload: TalkBulkReference(1, length: 1048576).encode(),
    ),
  ),
  (
    'Final response to 1 with a bulk payload: bulk number 1, length unknown',
    '44 01 00 00 01 00 00 00 FF FF FF FF FF FF',
    TalkFrame(
      kind: TalkKind.message,
      responseId: 1,
      bulk: true,
      payload: TalkBulkReference(1).encode(),
    ),
  ),
  (
    'Stream item to 2 with a bulk payload: bulk number 2, 0 bytes',
    '54 02 00 00 02 00 00 00 00 00 00 00 00 00',
    TalkFrame(
      kind: TalkKind.streamItem,
      responseId: 2,
      bulk: true,
      payload: TalkBulkReference(2, length: 0).encode(),
    ),
  ),
];

/// Negative vectors from the wiki (each MUST be a channel protocol error).
const List<(String, String)> negative = [
  ('BULK on an abort', '64 01 00 00 05 00'),
  ('BULK on an extend', '74 01 00 00'),
  (
    'BULK with a 4-byte reference',
    '43 50 55 54 00 00 00 00 00 01 00 00 01 00 00 00',
  ),
  ('STREAM without HAS_REQUEST', '09 48 45 4C 4C 4F 00 00 00'),
  (
    'Stream request truncated before its window',
    '0B 4C 49 53 54 00 00 00 00 02 00 00 FF FF',
  ),
  ('Request id 0', '03 48 45 4C 4C 4F 00 00 00 00 00 00'),
  ('STREAM_ITEM without HAS_RESPONSE', '11 48 45 4C 4C 4F 00 00 00'),
  ('EXTEND with both ids', '36 01 00 00 02 00 00'),
  ('EXTEND with neither id', '30'),
  ('Credit with an 8-byte payload', '32 02 00 00 A0 BB 0D 00 E0 93 04 00'),
  ('Credit of 0 items', '32 02 00 00 00 00 00 00'),
  ('EXTEND with a 4-byte payload', '34 01 00 00 A0 BB 0D 00'),
  ('ABORT with both ids', '26 01 00 00 02 00 00 01 00'),
  ('Plain message without procedure', '00 01'),
  ('Truncated procedure', '01 48 45'),
];

/// Further violations of the flag rules in talk.md that are not wiki
/// vectors.
const List<(String, String)> negativeExtra = [
  ('Reserved bit 0x80', '81 48 45 4C 4C 4F 00 00 00'),
  ('BULK with an empty reference', '41 48 45 4C 4C 4F 00 00 00'),
  (
    'BULK with an 11-byte reference',
    '41 48 45 4C 4C 4F 00 00 00 01 00 00 00 00 00 00 00 00 00 00',
  ),
  (
    'BULK reference to bulk number 0',
    '41 48 45 4C 4C 4F 00 00 00 00 00 00 00 05 00 00 00 00 00',
  ),
  ('BULK on a cancel', '62 02 00 00 01 00'),
  ('BULK on a channel abort', '60 10 00'),
  ('Empty input', ''),
  ('Response id 0', '04 00 00 00'),
  ('Chained with response id 0', '06 01 00 00 00 00 00'),
  ('EXTEND with STREAM and no request', '3C 01 00 00'),
  ('Credit with STREAM', '3A 02 00 00 08 00 00 00'),
  ('Credit with STREAM and a window', '3A 02 00 00 FF FF FF FF 08 00 00 00'),
  ('Cancel with STREAM', '2A 02 00 00 01 00'),
  ('Cancel with STREAM and a window', '2A 02 00 00 FF FF FF FF 01 00'),
  ('Credit with an empty payload', '32 02 00 00'),
  ('Credit with a 3-byte payload', '32 02 00 00 08 00 00'),
  ('Credit with a 5-byte payload', '32 02 00 00 08 00 00 00 00'),
  (
    'Credit with a procedure and 0 items',
    '33 41 00 00 00 00 00 00 00 02 00 00 '
        '00 00 00 00',
  ),
  ('Chained stream request without its window', '0E 04 00 00 03 00 00'),
  (
    'Item that is a stream request, truncated window',
    '1E 05 00 00 03 00 00 04 00 00',
  ),
  ('EXTEND with a 2-byte payload', '34 01 00 00 AA BB'),
  ('EXTEND with a 9-byte payload', '34 01 00 00 01 00 00 00 01 00 00 00 00'),
  ('STREAM_ITEM with only a request id', '13 48 45 4C 4C 4F 00 00 00 01 00 00'),
  ('Request without procedure', '02 01 00 00'),
  ('ABORT with a one byte payload', '24 01 00 00 05'),
  ('Truncated request id', '03 48 45 4C 4C 4F 00 00 00 01 00'),
  ('Truncated response id', '04 01 00'),
  ('Procedure with a byte after the padding', '01 61 00 62 00 00 00 00 00'),
];

void main() {
  group('Talk vectors, positive', () {
    for (final (name, hex, expected) in positive) {
      test(name, () {
        final bytes = hexBytes(hex);
        final frame = TalkFrame.decode(bytes);
        expect(frame, expected);
        expect(hexString(frame.encode()), hexString(bytes));
        expect(hexString(expected.encode()), hexString(bytes));
      });
    }

    test('bulk references decode', () {
      expect(
        TalkFrame.decode(
          hexBytes(
            '43 50 55 54 00 00 00 00 00 01 00 00 01 00 00 00 00 00 10 00 00 00',
          ),
        ).bulkReference,
        TalkBulkReference(1, length: 1048576),
      );
      final unknown = TalkFrame.decode(
        hexBytes('44 01 00 00 01 00 00 00 FF FF FF FF FF FF'),
      ).bulkReference;
      expect(unknown.number, 1);
      expect(unknown.length, isNull);
      final empty = TalkFrame.decode(
        hexBytes('54 02 00 00 02 00 00 00 00 00 00 00 00 00'),
      ).bulkReference;
      expect(empty.number, 2);
      expect(empty.length, 0);
      // The largest values survive the 48-bit arithmetic on the web too.
      final largest = TalkBulkReference(
        TalkBulkReference.maxNumber,
        length: TalkBulkReference.maxLength,
      );
      expect(hexString(largest.encode()), 'FF FF FF FF FE FF FF FF FF FF');
      expect(TalkBulkReference.decode(largest.encode()), largest);
      expect(
        () => TalkFrame.decode(hexBytes('04 01 00 00')).bulkReference,
        throwsStateError,
      );
    });

    test('abort statuses decode', () {
      expect(
        TalkFrame.decode(hexBytes('24 01 00 00 05 00 6E 6F 70 65')).status,
        Status.of(StatusCode.notFound, 'nope'),
      );
      expect(
        TalkFrame.decode(hexBytes('22 02 00 00 01 00')).status.known,
        StatusCode.cancelled,
      );
      expect(
        TalkFrame.decode(hexBytes('20 10 00')).status.known,
        StatusCode.unauthenticated,
      );
      // An empty abort payload means UNKNOWN (talk.md, ABORT).
      expect(
        TalkFrame.decode(hexBytes('24 01 00 00')).status,
        Status.of(StatusCode.unknown),
      );
      expect(TalkFrame.decode(hexBytes('20')).status.known, StatusCode.unknown);
    });
  });

  group('Talk vectors, negative', () {
    for (final (name, hex) in negative) {
      test(name, () {
        expect(
          () => TalkFrame.decode(hexBytes(hex)),
          throwsA(
            isA<ProtocolException>().having(
              (e) => e.code,
              'code',
              StatusCode.protocolError,
            ),
          ),
        );
      });
    }
  });

  group('Talk decoding rules beyond the wiki vectors', () {
    for (final (name, hex) in negativeExtra) {
      test(name, () {
        expect(
          () => TalkFrame.decode(hexBytes(hex)),
          throwsA(isA<ProtocolException>()),
        );
      });
    }

    test('EXTEND values decode', () {
      expect(
        TalkFrame.decode(hexBytes('34 01 00 00 A0 BB 0D 00 E0 93 04 00'))
            .extension,
        (
          deadline: const Duration(minutes: 15),
          renew: const Duration(minutes: 5),
        ),
      );
      expect(
        TalkFrame.decode(hexBytes('34 01 00 00 00 00 00 00 E0 93 04 00'))
            .extension,
        (deadline: null, renew: const Duration(minutes: 5)),
      );
      // An empty payload and two zero fields both declare nothing; the
      // payload tells a restart of the default timeout from "unchanged".
      expect(TalkFrame.decode(hexBytes('34 01 00 00')).extension, (
        deadline: null,
        renew: null,
      ));
      final zeros = TalkFrame.decode(
        hexBytes('34 01 00 00 00 00 00 00 00 00 00 00'),
      );
      expect(zeros.extension, (deadline: null, renew: null));
      expect(zeros.payload, hasLength(8));
      expect(
        TalkFrame.decode(hexBytes('34 01 00 00 FF FF FF FF 01 00 00 00'))
            .extension,
        (
          deadline: const Duration(milliseconds: 0xFFFFFFFF),
          renew: const Duration(milliseconds: 1),
        ),
      );
      expect(
        () => TalkFrame(kind: TalkKind.message, responseId: 1).extension,
        throwsStateError,
      );
    });

    test('EXTEND values encode in whole milliseconds, rounded up', () {
      String payload({Duration? deadline, Duration? renew}) =>
          hexString(TalkFrame.extendPayload(deadline: deadline, renew: renew));
      expect(payload(), '');
      expect(
        payload(deadline: const Duration(microseconds: 1500)),
        '02 00 00 00 00 00 00 00',
      );
      // Under 1 ms is 1 ms: 0 would mean "unchanged".
      expect(payload(renew: Duration.zero), '00 00 00 00 01 00 00 00');
      expect(
        payload(deadline: const Duration(days: 60)),
        'FF FF FF FF 00 00 00 00',
      );
      expect(
        () => payload(deadline: const Duration(seconds: -1)),
        throwsArgumentError,
      );
    });

    test('a missing procedure reads as the empty name', () {
      final frame = TalkFrame.decode(hexBytes('04 01 00 00'));
      expect(frame.procedure, isNull);
    });

    test('chained stream request and item request decode', () {
      // Final response to 3 that is stream request 4, unlimited window.
      final chained = TalkFrame.decode(
        hexBytes('0E 04 00 00 03 00 00 FF FF FF FF'),
      );
      expect(chained.stream, isTrue);
      expect(chained.requestId, 4);
      expect(chained.responseId, 3);
      expect(chained.window, TalkFrame.unlimitedWindow);
      // Stream item to 3 that is request 5.
      final item = TalkFrame.decode(hexBytes('16 05 00 00 03 00 00 AA'));
      expect(item.kind, TalkKind.streamItem);
      expect(item.requestId, 5);
      expect(item.responseId, 3);
      expect(hexString(item.payload), 'AA');
    });

    test('payload is a view, not a copy', () {
      final bytes = hexBytes('04 01 00 00 01 02 03');
      final frame = TalkFrame.decode(bytes);
      expect(hexString(frame.payload), '01 02 03');
      bytes[4] = 0x7F;
      expect(frame.payload[0], 0x7F);
    });
  });

  group('Item windows on the wire', () {
    /// Decodes [hex], checks it encodes back to the same bytes, and
    /// returns the frame.
    TalkFrame roundTrip(String hex) {
      final frame = TalkFrame.decode(hexBytes(hex));
      expect(hexString(frame.encode()), hexString(hexBytes(hex)));
      return frame;
    }

    test('a stream request carries its window after its ids', () {
      // Window 0: nothing until a grant.
      final zero = roundTrip('0B 4C 49 53 54 00 00 00 00 02 00 00 00 00 00 00');
      expect(zero.window, 0);
      expect(zero.payload, isEmpty);
      // The largest finite window, with a payload after it.
      final largest = roundTrip(
        '0B 4C 49 53 54 00 00 00 00 02 00 00 FE FF FF FF AA BB',
      );
      expect(largest.window, TalkFrame.unlimitedWindow - 1);
      expect(hexString(largest.payload), 'AA BB');
      expect(largest.toString(), contains('window=4294967294'));
      final unlimited = roundTrip(
        '0B 4C 49 53 54 00 00 00 00 02 00 00 FF FF FF FF',
      );
      expect(unlimited.window, TalkFrame.unlimitedWindow);
      // The window is part of the frame.
      expect(zero, isNot(unlimited));
      // Built rather than decoded.
      expect(
        hexString(
          TalkFrame(
            kind: TalkKind.message,
            procedure: Name('LIST'),
            requestId: 2,
            stream: true,
            window: 3,
            payload: hexBytes('01'),
          ).encode(),
        ),
        '0B 4C 49 53 54 00 00 00 00 02 00 00 03 00 00 00 01',
      );
    });

    test('a chained response that is a stream request carries its window '
        'after both ids', () {
      // Final response to 3 that is stream request 4, window 2.
      final chained = roundTrip('0E 04 00 00 03 00 00 02 00 00 00 CC');
      expect(chained.kind, TalkKind.message);
      expect(chained.requestId, 4);
      expect(chained.responseId, 3);
      expect(chained.stream, isTrue);
      expect(chained.window, 2);
      expect(hexString(chained.payload), 'CC');
    });

    test('an item that is a stream request carries its window after both '
        'ids', () {
      // Stream item to 3 that is stream request 5 "SUB", window 4.
      final item = roundTrip(
        '1F 53 55 42 00 00 00 00 00 05 00 00 03 00 00 04 00 00 00 AA',
      );
      expect(item.kind, TalkKind.streamItem);
      expect(item.procedure, Name('SUB'));
      expect(item.requestId, 5);
      expect(item.responseId, 3);
      expect(item.window, 4);
      expect(hexString(item.payload), 'AA');
      // The same, with a bulk payload: the reference follows the window.
      final bulk = roundTrip(
        '5E 05 00 00 03 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00 00',
      );
      expect(bulk.bulk, isTrue);
      expect(bulk.window, 0);
      expect(bulk.bulkReference, TalkBulkReference(2, length: 0));
      // withBulk keeps the window.
      final windowed = TalkFrame(
        kind: TalkKind.message,
        procedure: Name('PUT'),
        requestId: 1,
        stream: true,
        window: 7,
      ).withBulk(TalkBulkReference(1));
      expect(windowed.window, 7);
      expect(TalkFrame.decode(windowed.encode()), windowed);
    });

    test('a credit names a request of its sender and counts items', () {
      final credit = roundTrip('32 02 00 00 08 00 00 00');
      expect(credit.kind, TalkKind.extend);
      expect(credit.isCredit, isTrue);
      expect(credit.requestId, 2);
      expect(credit.hasResponse, isFalse);
      expect(credit.creditItems, 8);
      // A credit is not an extension, and an extension not a credit.
      expect(() => credit.extension, throwsStateError);
      final extend = TalkFrame.decode(hexBytes('34 01 00 00'));
      expect(extend.isCredit, isFalse);
      expect(() => extend.creditItems, throwsStateError);
      // A procedure is optional and ignored.
      final named = roundTrip(
        '33 41 00 00 00 00 00 00 00 02 00 00 01 00 00 00',
      );
      expect(named.creditItems, 1);
      // The largest count decodes on every platform; that it overflows
      // any window is the channel's to say.
      expect(
        roundTrip('32 FF FF FF FF FF FF FF').creditItems,
        TalkFrame.unlimitedWindow,
      );
      expect(hexString(TalkFrame.creditPayload(1)), '01 00 00 00');
      expect(hexString(TalkFrame.creditPayload(0xFFFFFFFF)), 'FF FF FF FF');
      expect(() => TalkFrame.creditPayload(0), throwsArgumentError);
      expect(() => TalkFrame.creditPayload(0x100000000), throwsArgumentError);
    });

    test('frames that are not stream requests carry no window', () {
      for (final hex in [
        '03 47 45 54 5F 50 52 4F 46 01 00 00',
        '15 49 54 45 4D 00 00 00 00 02 00 00 07',
        '07 4D 4F 52 45 00 00 00 00 07 00 00 01 00 00',
        '16 05 00 00 03 00 00 AA',
      ]) {
        final frame = roundTrip(hex);
        expect(frame.stream, isFalse);
        expect(frame.window, TalkFrame.unlimitedWindow);
      }
    });
  });

  group('TalkFrame.encode refuses invalid frames', () {
    final cases = <(String, TalkFrame Function())>[
      (
        'plain message without procedure',
        () => TalkFrame(kind: TalkKind.message),
      ),
      (
        'request without procedure',
        () => TalkFrame(kind: TalkKind.message, requestId: 1),
      ),
      (
        'stream without request',
        () => TalkFrame(
          kind: TalkKind.message,
          procedure: Name('A'),
          stream: true,
        ),
      ),
      ('item without response', () => TalkFrame(kind: TalkKind.streamItem)),
      (
        'abort with both ids',
        () => TalkFrame(kind: TalkKind.abort, requestId: 1, responseId: 1),
      ),
      ('extend without an id', () => TalkFrame(kind: TalkKind.extend)),
      (
        'extend with both ids',
        () => TalkFrame(kind: TalkKind.extend, requestId: 1, responseId: 1),
      ),
      (
        'credit without a payload',
        () => TalkFrame(kind: TalkKind.extend, requestId: 1),
      ),
      (
        'credit with an extend payload',
        () => TalkFrame(
          kind: TalkKind.extend,
          requestId: 1,
          payload: TalkFrame.extendPayload(deadline: const Duration(hours: 1)),
        ),
      ),
      (
        'credit of 0 items',
        () => TalkFrame(
          kind: TalkKind.extend,
          requestId: 1,
          payload: Uint8List(4),
        ),
      ),
      (
        'credit with STREAM',
        () => TalkFrame(
          kind: TalkKind.extend,
          requestId: 1,
          stream: true,
          payload: TalkFrame.creditPayload(1),
        ),
      ),
      (
        'cancel with STREAM',
        () => TalkFrame(kind: TalkKind.abort, requestId: 1, stream: true),
      ),
      (
        'window without STREAM',
        () => TalkFrame(
          kind: TalkKind.message,
          procedure: Name('A'),
          requestId: 1,
          window: 4,
        ),
      ),
      (
        'negative window',
        () => TalkFrame(
          kind: TalkKind.message,
          procedure: Name('A'),
          requestId: 1,
          stream: true,
          window: -1,
        ),
      ),
      (
        'window over u32',
        () => TalkFrame(
          kind: TalkKind.message,
          procedure: Name('A'),
          requestId: 1,
          stream: true,
          window: 0x100000000,
        ),
      ),
      (
        'extend with a 1-byte payload',
        () => TalkFrame(
          kind: TalkKind.extend,
          responseId: 1,
          payload: Uint8List(1),
        ),
      ),
      (
        'request id over u24',
        () => TalkFrame(
          kind: TalkKind.message,
          procedure: Name('A'),
          requestId: 0x1000000,
        ),
      ),
      (
        'negative response id',
        () => TalkFrame(kind: TalkKind.message, responseId: -1),
      ),
    ];
    for (final (name, build) in cases) {
      test(name, () {
        expect(() => build().encode(), throwsArgumentError);
      });
    }
  });
}
