/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
Authors:
  Jan Boon <jan.boon@kaetemi.be>
  Claude Opus 5.5 <noreply@anthropic.com>
*/

import 'package:meta/meta.dart';

import '../mux/binding_connection.dart';
import '../mux/mux_connection.dart';
import '../status.dart';
import 'switchboard.dart';

/// A way for a [Switchboard] to reach the endpoints of further URI schemes:
/// a binding of the connection contract ([MuxConnection]) to a transport
/// other than the node's built-in ones (`ws`, `wss`, `tcp`, `mem`, all
/// carried by the frame mux), such as one giving every channel a QUIC
/// stream of its own. Registered with [Switchboard.registerBinding] (or
/// the constructor's `bindings`).
///
/// The node dials an endpoint of one of [schemes] through [dial], and then
/// treats the connection exactly like one it dialled itself: pooled by
/// [normalise]d endpoint, its policy chosen by [Switchboard.connect]'s or
/// [Switchboard.dial]'s `policy`, [Switchboard.endpointPolicy] and
/// [Switchboard.outgoingPolicy], the node identifying on it as
/// [Switchboard.identifyOutgoing], [Switchboard.identifyFor] and
/// [Switchboard.expectedIdentityFor] say, the channels the peer opens
/// dispatched, reported on [Switchboard.connections] and sent GOAWAY by
/// [Switchboard.close].
///
/// The connections a binding accepts, from a listener of its own, go to
/// [Switchboard.adopt], which treats them as a listener of the node would
/// its own. A binding's listener endpoints are published explicitly
/// (`MeshNode.publish`'s `endpoints`); the node does not list them in
/// [Switchboard.listeningEndpoints], and does not recognise them as its
/// own ([Switchboard.isOwnEndpoint]).
///
/// See `doc/dart-design.md`, section "Binding seam".
abstract class SwitchboardBinding {
  /// For subclasses.
  const SwitchboardBinding();

  /// The URI schemes whose endpoints this binding dials, in lower case.
  /// Read once, when the binding is registered.
  Set<String> get schemes;

  /// Establishes a connection to [endpoint], whose scheme is one of
  /// [schemes], as its initiator ([MuxConnection.isInitiator] true), and
  /// returns it once it can open channels.
  ///
  /// The connection is configured with [settings]: the node's
  /// [BindingDialSettings.options] (its identity verifier among them) and
  /// [BindingDialSettings.localIdentity], set before the connection handles
  /// anything from the peer. [BindingDialSettings.timeout] bounds the
  /// attempt; the node also gives up on it then, and closes a connection
  /// that arrives later.
  ///
  /// Fails with [SwitchboardException]: [StatusCode.unavailable] when the
  /// endpoint cannot be reached, [StatusCode.invalidArgument] for an
  /// endpoint the binding cannot use. The node reports any other error as
  /// [StatusCode.unavailable].
  Future<MuxConnection> dial(Uri endpoint, BindingDialSettings settings);

  /// [endpoint] as the node's pool tells endpoints apart, and as
  /// [Switchboard.endpointPolicy] and [Switchboard.expectedIdentityFor]
  /// are asked about it: two spellings of one endpoint must give equal
  /// results. By default the scheme and host lower-cased and the port if
  /// given, without path, query or fragment; a binding whose endpoints
  /// differ by path (WebTransport, say) keeps it.
  Uri normalise(Uri endpoint) => Uri(
    scheme: endpoint.scheme.toLowerCase(),
    host: endpoint.host.toLowerCase(),
    port: endpoint.hasPort ? endpoint.port : null,
  );
}

/// What a [Switchboard] gives a [SwitchboardBinding] to dial with.
@immutable
class BindingDialSettings {
  /// Settings of one dial.
  const BindingDialSettings({
    required this.options,
    required this.timeout,
    this.localIdentity,
  });

  /// The node's [Switchboard.muxOptions], for the connection's
  /// [MuxConnection.options].
  final MuxOptions options;

  /// The node's [Switchboard.connectTimeout]; [Duration.zero] or less for
  /// no bound.
  final Duration timeout;

  /// The node's identity, for the connection's
  /// [MuxConnection.localIdentity] ([BindingConnection.localIdentity]);
  /// null for a node without a credential.
  final String? localIdentity;
}
