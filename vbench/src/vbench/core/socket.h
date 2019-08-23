// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "string.h"
#include "stream.h"
#include <vespa/vespalib/data/simple_buffer.h>
#include <vespa/vespalib/net/socket_handle.h>
#include <vespa/vespalib/net/server_socket.h>
#include <vespa/vespalib/net/crypto_engine.h>
#include <vespa/vespalib/net/sync_crypto_socket.h>
#include <memory>
#ifdef __APPLE__
#include <poll.h>
#endif

namespace vbench {


class Socket : public Stream
{
public:
    using Input = vespalib::Input;
    using Memory = vespalib::Memory;
    using Output = vespalib::Output;
    using SimpleBuffer = vespalib::SimpleBuffer;
    using WritableMemory = vespalib::WritableMemory;
    using CryptoEngine = vespalib::CryptoEngine;
    using SyncCryptoSocket = vespalib::SyncCryptoSocket;
private:
    SyncCryptoSocket::UP   _socket;
    SimpleBuffer           _input;
    SimpleBuffer           _output;
    Taint                  _taint;
    bool                   _eof;

public:
    Socket(SyncCryptoSocket::UP socket);
    Socket(CryptoEngine &crypto, const string &host, int port);
    ~Socket();
    bool eof() const override { return _eof; }
    Memory obtain() override;
    Input &evict(size_t bytes) override;
    WritableMemory reserve(size_t bytes) override;
    Output &commit(size_t bytes) override;
    const Taint &tainted() const override { return _taint; }
};

struct ServerSocket {
    using CryptoEngine = vespalib::CryptoEngine;
    using SyncCryptoSocket = vespalib::SyncCryptoSocket;
    vespalib::ServerSocket server_socket;
#ifdef __APPLE__
    bool closed;
#endif
    ServerSocket() : server_socket(0)
#ifdef __APPLE__
                   , closed(false)
#endif
    {}
    int port() const { return server_socket.address().port(); }
    Stream::UP accept(CryptoEngine &crypto) {
#ifdef __APPLE__
        server_socket.set_blocking(false);
        while (!closed) {
            pollfd fds;
            fds.fd = server_socket.get_fd();
            fds.events = POLLIN;
            fds.revents = 0;
            int res = poll(&fds, 1, 10);
            if (res < 1 || fds.revents == 0) {
                continue;
            }
            break;
        }
        if (closed) {
            return Stream::UP();
        }
#endif
        vespalib::SocketHandle handle = server_socket.accept();
#ifdef __APPLE__
        handle.set_blocking(true);
#endif
        if (handle.valid()) {
            return std::make_unique<Socket>(SyncCryptoSocket::create(crypto, std::move(handle), true));
        } else {
            return Stream::UP();
        }
    }
    void close() {
#ifdef __APPLE__
        closed = true;
#endif
        server_socket.shutdown();
    }
};

} // namespace vbench
