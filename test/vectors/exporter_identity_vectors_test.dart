/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Opus 5.5 <noreply@anthropic.com>
  GPT-6 Astra <noreply@openai.com>
*/

// Exporter-bound IDENT vector X. The frame mux's vector suite stays unchanged.
import 'dart:typed_data';

import 'package:clock/clock.dart';
import 'package:switchboard/core.dart';
import 'package:test/test.dart';

import 'identity_vectors_test.dart'
    show
        authorityPublic,
        holderSeed,
        ed25519Credential,
        senderNonce,
        receiverNonce,
        intent,
        receiver,
        at,
        proof;

Matcher throwsCode(StatusCode code) =>
    throwsA(isA<SwitchboardException>().having((e) => e.code, 'code', code));

void main() {
  group('exporter-bound proof (QUIC and WebTransport binding, X)', () {
    // The inputs of P with an exporter of 32 bytes 33.
    final exporter = Uint8List(32)..fillRange(0, 32, 0x33);
    const message =
        '53 57 42 49 44 54 4C 53 11 11 11 11 11 11 11 11 '
        '11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 '
        '11 11 11 11 11 11 11 11 22 22 22 22 22 22 22 22 '
        '22 22 22 22 22 22 22 22 22 22 22 22 22 22 22 22 '
        '22 22 22 22 22 22 22 22 33 33 33 33 33 33 33 33 '
        '33 33 33 33 33 33 33 33 33 33 33 33 33 33 33 33 '
        '33 33 33 33 33 33 33 33 04 01 02 03 04 08 63 6F '
        '6E 73 75 6D 65 72';
    const x =
        '14 92 28 9E 3F C3 B3 C6 91 F3 B1 96 EE F2 E5 ED '
        'C1 A3 C1 86 A3 D8 9A 0E 4B 10 B8 93 4C 7E 8B 4B '
        'CA 10 FA 3E 47 92 D2 BD 69 67 22 21 92 FC FC 44 '
        '96 73 EB 16 BA 43 02 AF 58 FE 19 A6 AF 53 7E 03';

    final verifier = CredentialVerifier(
      ed25519Keys: {Name('auth1'): hexBytes(authorityPublic)},
    );

    MuxIdent ident(String proof) => MuxIdent(
      credential: hexBytes(ed25519Credential),
      intent: intent,
      receiver: receiver,
      proof: hexBytes(proof),
    );

    /// verifyIdent at the vectors' time, as `consumer` receiving.
    Future<PeerIdentity> verify(
      String proof, {
      Uint8List? exporter,
      bool swapped = false,
    }) => withClock(
      Clock.fixed(at),
      () => verifyIdent(
        ident(proof),
        verifier: verifier,
        senderNonce: swapped ? receiverNonce : senderNonce,
        receiverNonce: swapped ? senderNonce : receiverNonce,
        localIdentity: 'consumer',
        exporter: exporter,
      ),
    );

    test('the signed message and its signature', () async {
      final signed = MuxIdent.proofMessage(
        senderNonce,
        receiverNonce,
        intent,
        receiver,
        exporter: exporter,
      );
      expect(hexString(signed), message);
      expect(signed, hasLength(118));
      expect(MuxIdent.exporterProofLabel, 'SWBIDTLS'.codeUnits);
      final holder = await HolderKey.fromSeed(holderSeed);
      expect(hexString(await holder.sign(signed)), x);
    });

    test('X verifies as an exporter-bound proof', () async {
      final identity = await verify(x, exporter: exporter);
      expect(identity.identity, 'worker-a1');
      expect(identity.receiver, 'consumer');
      expect(identity.intent, intent);
      // P verifies as a nonce-bound one, the form it was made for.
      expect((await verify(proof)).identity, 'worker-a1');
    });

    test('X does not verify as a nonce-bound proof', () async {
      await expectLater(verify(x), throwsCode(StatusCode.unauthenticated));
    });

    test('P does not verify as an exporter-bound proof', () async {
      await expectLater(
        verify(proof, exporter: exporter),
        throwsCode(StatusCode.unauthenticated),
      );
    });

    test('X with the nonces swapped (sent back to its sender) does not '
        'verify', () async {
      await expectLater(
        verify(x, exporter: exporter, swapped: true),
        throwsCode(StatusCode.unauthenticated),
      );
    });

    test('X with another exporter does not verify', () async {
      await expectLater(
        verify(x, exporter: Uint8List(32)..fillRange(0, 32, 0x34)),
        throwsCode(StatusCode.unauthenticated),
      );
    });

    test('an exporter of another length is refused', () {
      expect(
        () => MuxIdent.proofMessage(
          senderNonce,
          receiverNonce,
          intent,
          receiver,
          exporter: Uint8List(31),
        ),
        throwsArgumentError,
      );
      expect(() => verify(x, exporter: Uint8List(33)), throwsArgumentError);
    });
  });

  test('secure random bytes support lengths beyond the Web Crypto quota', () {
    for (final length in [0, 32, 65536, 65537]) {
      expect(secureRandomBytes(length), hasLength(length));
    }
  });
}
