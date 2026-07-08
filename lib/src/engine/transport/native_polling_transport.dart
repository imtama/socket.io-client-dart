// Author: Custom Implementation

import 'dart:async';
import 'dart:convert';
import 'dart:io' as io;
import 'dart:typed_data';

import 'package:logging/logging.dart';
import 'package:socket_io_client/src/engine/transport.dart';
import 'package:socket_io_common/src/engine/parser/parser.dart';
import 'package:socket_io_common/src/util/event_emitter.dart';

final Logger _logger = Logger('socket_io:transport.NativePollingTransport');

class NativePollingTransport extends Transport {
  @override
  String? name = 'polling';

  bool polling = false;
  NativeRequest? pollXhr;
  
  final Set<NativeRequest> _activeRequests = {};
  late final io.HttpClient _httpClient;

  NativePollingTransport(Map opts) : super(opts) {
    final forceBase64 = opts.containsKey('forceBase64') && opts['forceBase64'] == true;
    supportsBinary = !forceBase64;
    
    _httpClient = io.HttpClient(context: io.SecurityContext.defaultContext);
    
    if (opts['rejectUnauthorized'] == false) {
      _httpClient.badCertificateCallback = (cert, host, port) => true;
    }
  }

  @override
  void doOpen() {
    poll();
  }

  @override
  void pause(dynamic onPause) {
    var self = this;
    readyState = 'pausing';

    void pause() {
      _logger.fine('paused');
      self.readyState = 'paused';
      onPause();
    }

    if (polling == true || writable != true) {
      var total = 0;

      if (polling == true) {
        _logger.fine('we are currently polling - waiting to pause');
        total++;
        once('pollComplete', (_) {
          _logger.fine('pre-pause polling complete');
          if (--total == 0) pause();
        });
      }

      if (writable != true) {
        _logger.fine('we are currently writing - waiting to pause');
        total++;
        once('drain', (_) {
          _logger.fine('pre-pause writing complete');
          if (--total == 0) pause();
        });
      }
    } else {
      pause();
    }
  }

  void poll() {
    _logger.fine('polling');
    polling = true;
    doPoll();
    emitReserved('poll');
  }

  @override
  void onData(dynamic data) {
    var self = this;
    _logger.fine('polling got data $data');
    void callback(packet, [index, total]) {
      if ('opening' == self.readyState && packet['type'] == 'open') {
        self.onOpen();
      }

      if ('close' == packet['type']) {
        self.onClose({'description': "transport closed by the server"});
        return;
      }

      self.onPacket(packet);
    }

    PacketParser.decodePayload(data, socket!.binaryType).forEach(callback);

    if ('closed' != readyState) {
      polling = false;
      emitReserved('pollComplete');

      if ('open' == readyState) {
        poll();
      } else {
        _logger.fine('ignoring poll - transport state "$readyState"');
      }
    }
  }

  @override
  void doClose() {
    var self = this;

    void close([_]) {
      _logger.fine('writing close packet');
      self.write([
        {'type': 'close'}
      ]);
      
      for (var req in _activeRequests.toList()) {
        req.abort();
      }
      _activeRequests.clear();
      
      _httpClient.close(force: true);
    }

    if ('open' == readyState) {
      _logger.fine('transport open - closing');
      close();
    } else {
      _logger.fine('transport not open - deferring close');
      once('open', close);
    }
  }

  @override
  void write(List packets) {
    var self = this;
    writable = false;

    PacketParser.encodePayload(packets, callback: (data) {
      self.doWrite(data, (_) {
        self.writable = true;
        self.emitReserved('drain');
      });
    });
  }

  String uri() {
    final query = this.query ?? {};
    var schema = opts['secure'] ? 'https' : 'http';

    if (opts['timestampRequests'] != null) {
      query[opts['timestampParam']] =
          DateTime.now().millisecondsSinceEpoch.toRadixString(36);
    }

    if (supportsBinary == false && !query.containsKey('sid')) {
      query['b64'] = 1;
    }

    return createUri(schema, query);
  }

  NativeRequest request([Map? opts]) {
    opts = opts ?? {};
    final mergedOpts = {
      ...opts,
      ...this.opts,
      'withCredentials': this.opts['withCredentials'] ?? false,
    };
    
    final req = NativeRequest(_httpClient, uri(), mergedOpts);
    _activeRequests.add(req);
    
    req.on('success', (_) => _activeRequests.remove(req));
    req.on('error', (_) => _activeRequests.remove(req));
    
    return req;
  }

  void doWrite(dynamic data, dynamic fn) {
    if (readyState == 'closed') return;
    
    var isBinary = data is! String;
    var req = request({'method': 'POST', 'data': data, 'isBinary': isBinary});
    req.on('success', fn);
    req.on('error', (err) {
      onError('xhr post error', err);
    });
  }

  void doPoll() {
    if (readyState == 'closed') return;
    
    _logger.fine('native poll');
    var req = request();
    req.on('data', (data) {
      onData(data);
    });
    req.on('error', (xhrStatus) {
      onError('xhr poll error', xhrStatus);
    });
    pollXhr = req;
  }
}

class NativeRequest extends EventEmitter {
  final io.HttpClient client;
  final String uri;
  final Map opts;
  late final String method;
  final dynamic data;
  final bool isBinary;

  io.HttpClientRequest? _request;
  bool _aborted = false;

  NativeRequest(this.client, this.uri, this.opts)
      : method = opts['method'] ?? 'GET',
        data = opts['data'],
        isBinary = opts['isBinary'] ?? false {
    _start();
  }

  void _start() async {
    final timeoutMs = opts['requestTimeout'] ?? 30000;

    try {
      Future<void> executeRequest() async {
        final targetUri = Uri.parse(uri);
        
        if (method == 'GET') {
          _request = await client.getUrl(targetUri);
        } else {
          _request = await client.postUrl(targetUri);
        }

        if (_aborted) {
          _request?.abort();
          return;
        }

        if (opts.containsKey('extraHeaders')) {
          final extra = opts['extraHeaders'];
          if (extra is Map) {
            extra.forEach((k, v) {
              _request!.headers.set(k.toString(), v.toString());
            });
          }
        }

        if (method == 'POST') {
          if (isBinary) {
            _request!.headers.set('Content-type', 'application/octet-stream');
          } else {
            _request!.headers.set('Content-type', 'text/plain;charset=UTF-8');
          }
        }
        _request!.headers.set('Accept', '*/*');

        if (method == 'POST' && data != null) {
          List<int>? bodyBytes;
          if (data is String) {
            bodyBytes = utf8.encode(data);
          } else if (data is List<int>) {
            bodyBytes = data;
          } else if (data is ByteBuffer) {
            bodyBytes = data.asUint8List();
          }

          if (bodyBytes != null) {
            _request!.headers.contentLength = bodyBytes.length;
            _request!.add(bodyBytes);
          }
        }

        await _readResponse();
      }

      if (timeoutMs > 0) {
        await executeRequest().timeout(
          Duration(milliseconds: timeoutMs),
          onTimeout: () {
            _request?.abort();
            throw TimeoutException('Request timed out');
          },
        );
      } else {
        await executeRequest();
      }
      
    } on TimeoutException catch (e) {
      if (!_aborted) {
        _aborted = true;
        emitReserved('error', 'timeout: $e');
      }
    } catch (e) {
      if (!_aborted) {
        _aborted = true;
        emitReserved('error', e);
      }
    } finally {
      _request = null;
    }
  }

  Future<void> _readResponse() async {
    final response = await _request!.close();
    if (_aborted) return;

    if (response.statusCode == 200 || response.statusCode == 1223) {
      final contentType = response.headers.contentType?.value;
      if (contentType == 'application/octet-stream') {
        final bytes = await response.fold<List<int>>([], (p, e) => p..addAll(e));
        if (!_aborted) emitReserved('data', bytes);
      } else {
        final content = await response.transform(utf8.decoder).join();
        if (!_aborted) emitReserved('data', content);
      }
      if (!_aborted) emitReserved('success');
    } else {
      if (!_aborted) emitReserved('error', response.statusCode);
    }
  }

  void abort() {
    if (_aborted) return;
    _aborted = true;
    try {
      _request?.abort();
    } catch (_) {}
  }
}