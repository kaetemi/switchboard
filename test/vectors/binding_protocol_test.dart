/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  GPT-6 Astra <noreply@openai.com>
*/

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:stream_channel/stream_channel.dart';
import 'package:switchboard/core.dart';
import 'package:test/test.dart';

import '../mux_harness.dart' as h;

class HeldVerifier extends CredentialVerifier {
  final result = Completer<Credential>();
  final entered = Completer<void>();
  int calls = 0;

  @override
  Future<Credential> verify(Uint8List bytes, {DateTime? now}) {
    calls++;
    if (!entered.isCompleted) entered.complete();
    return result.future;
  }
}

Uint8List control(MuxControlType type, Uint8List payload) =>
    MuxControlMessage(type.code, payload).toFrame().encode();

void main() {
  for (final succeeds in [false, true]) {
    test(
      'IDENT verification gates OPEN, PING and a queued IDENT: $succeeds',
      () async {
        final verifier = HeldVerifier();
        final (connection, raw) = h.rawPair(
          muxIsInitiator: false,
          options: h.rawOptions.copyWith(identityVerifier: verifier),
        );
        addTearDown(connection.close);
        final openedAs = <String?>[];
        connection.incoming.listen((_) {
          openedAs.add(connection.peerIdentity?.identity);
        });
        raw.transport.sink.add(control(MuxControlType.nonce, Uint8List(32)));
        await raw.nextControl(MuxControlType.nonce);
        final ident = MuxIdent(credential: Uint8List(0)).encode();
        raw.transport.sink
          ..add(control(MuxControlType.ident, ident))
          ..add(MuxFrame.open(2, Uint8List(1)).encode())
          ..add(control(MuxControlType.ping, Uint8List.fromList([7])))
          ..add(control(MuxControlType.ident, ident));
        await verifier.entered.future;
        await Future<void>.delayed(Duration.zero);
        expect(openedAs, isEmpty);
        expect(verifier.calls, 1);
        if (succeeds) {
          final issuer = CredentialIssuer.hmac(Name('test'), Uint8List(32));
          verifier.result.complete(
            await issuer.issue(kind: CredentialKind.node, identity: 'verified'),
          );
          final pong = await raw.nextControl(MuxControlType.pong);
          expect(pong.payload, [7]);
          // Opening and PONG delivery both happen after the new identity.
          expect(openedAs, ['verified']);
          await Future<void>.delayed(Duration.zero);
          expect(verifier.calls, 2);
        } else {
          verifier.result.completeError(
            SwitchboardException.of(StatusCode.unauthenticated, 'refused'),
          );
          final frames = await raw.rest();
          expect(
            frames.any((frame) => h.isControl(frame, MuxControlType.pong)),
            isFalse,
          );
          expect(openedAs, isEmpty);
          expect(verifier.calls, 1);
          expect(await connection.done, h.hasCode(StatusCode.unauthenticated));
        }
      },
    );
  }

  test('wrong receiver closes synchronously without entering the verifier', () {
    final input = StreamController<Uint8List>(sync: true);
    final output = StreamController<Uint8List>(sync: true);
    final verifier = HeldVerifier();
    final connection = MuxConnection(
      StreamChannel(input.stream, output.sink),
      isInitiator: false,
      options: h.rawOptions.copyWith(identityVerifier: verifier),
    )..localIdentity = 'receiver';
    addTearDown(connection.close);
    addTearDown(input.close);
    addTearDown(output.close);
    output.stream.listen((_) {});
    input.add(control(MuxControlType.nonce, Uint8List(32)));
    expect(connection.isOpen, isTrue);
    input.add(
      control(
        MuxControlType.ident,
        MuxIdent(
          credential: Uint8List(0),
          receiver: Uint8List.fromList(utf8.encode('somebody-else')),
        ).encode(),
      ),
    );
    expect(connection.isOpen, isFalse);
    expect(verifier.calls, 0);
  });

  test(
    'public binding status helpers preserve payloads and map local codes',
    () {
      final moved = MovedStatus(
        owner: 0x123456789ABC,
        epoch: 0xFEDCBA98,
        reason: 'é' * 900,
      );
      final wire = statusForWire(moved.toStatus());
      expect(wire.encode().length, lessThanOrEqualTo(maxControlPayloadLength));
      final decoded = MovedStatus.fromStatus(wire);
      expect(decoded.owner, moved.owner);
      expect(decoded.epoch, moved.epoch);
      expect(decoded.reason, isNot(contains('\uFFFD')));
      expect(
        statusForWire(Status.of(StatusCode.connectionLost)).known,
        StatusCode.unavailable,
      );
      expect(() => statusForWire(const Status(256)), throwsArgumentError);
      expect(statusFromWire(const Status(256)).known, StatusCode.unknown);
    },
  );
}
