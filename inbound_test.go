/*
 * Copyright (c) 2020 Percipia
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Contributor(s):
 * Glenn O. Larsen <glenn.larsen@gmail.com>
 */
package eslgo

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInboundDial_ReturnsErrorWhenConnectionClosesBeforeAuthRequest(t *testing.T) {
	listener := listenTCP(t)
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		conn.Close()
	}()

	opts := DefaultInboundOptions
	opts.AuthTimeout = 100 * time.Millisecond
	opts.Logger = NilLogger{}
	disconnected := make(chan struct{})
	opts.OnDisconnect = func() {
		close(disconnected)
	}

	started := time.Now()
	conn, err := opts.Dial(listener.Addr().String())

	assert.Nil(t, conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "before auth request")
	assert.Less(t, time.Since(started), time.Second)
	assertOnDisconnect(t, disconnected)
}

func TestInboundDial_ReturnsErrorWhenAuthRequestTimesOut(t *testing.T) {
	listener := listenTCP(t)
	defer listener.Close()

	done := make(chan struct{})
	defer close(done)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		<-done
	}()

	opts := DefaultInboundOptions
	opts.AuthTimeout = 100 * time.Millisecond
	opts.Logger = NilLogger{}
	disconnected := make(chan struct{})
	opts.OnDisconnect = func() {
		close(disconnected)
	}

	started := time.Now()
	conn, err := opts.Dial(listener.Addr().String())

	assert.Nil(t, conn)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
	assert.Less(t, time.Since(started), time.Second)
	assertOnDisconnect(t, disconnected)
}

func assertOnDisconnect(t *testing.T, disconnected <-chan struct{}) {
	t.Helper()
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("expected OnDisconnect to be called")
	}
}

func listenTCP(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	return listener
}

// TestInboundClose_NoRaceOnResponseChannels drops the socket after auth.
// disconnectLoop then calls Close, which deletes responseChannels, while
// authLoop is still selecting on that map.
func TestInboundClose_NoRaceOnResponseChannels(t *testing.T) {
	for i := 0; i < 40; i++ {
		dropAuthenticatedSocket(t)
	}
}

func dropAuthenticatedSocket(t *testing.T) {
	t.Helper()
	listener := listenTCP(t)
	defer listener.Close()

	release := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := conn.Write([]byte("Content-Type: auth/request\r\n\r\n")); err != nil {
			return
		}
		buf := make([]byte, 256)
		if _, err := conn.Read(buf); err != nil {
			return
		}
		if _, err := conn.Write([]byte("Content-Type: command/reply\r\nReply-Text: +OK accepted\r\n\r\n")); err != nil {
			return
		}
		<-release
		_, _ = conn.Write([]byte("Content-Type: text/disconnect-notice\r\n\r\n"))
	}()

	opts := DefaultInboundOptions
	opts.Logger = NilLogger{}
	opts.AuthTimeout = time.Second
	disconnected := make(chan struct{})
	opts.OnDisconnect = func() {
		close(disconnected)
	}

	conn, err := opts.Dial(listener.Addr().String())
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	close(release)
	assertOnDisconnect(t, disconnected)
}

// TestDisconnectLoop_AfterCloseDoesNotNotify checks that a disconnectLoop
// started after Close returns without calling onDisconnect, as it did when it
// read the deleted map entry as a nil channel.
func TestDisconnectLoop_AfterCloseDoesNotNotify(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	connection := newConnection(client, false, Options{Context: context.Background(), Logger: NilLogger{}})
	connection.Close()

	called := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		connection.disconnectLoop(func() { called <- struct{}{} })
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disconnectLoop did not return after Close")
	}
	assert.Empty(t, called)
}

// TestOutboundDummyLoop_NoRaceOnClose closes an outbound connection while
// dummyLoop is selecting on responseChannels.
func TestOutboundDummyLoop_NoRaceOnClose(t *testing.T) {
	for i := 0; i < 40; i++ {
		server, client := net.Pipe()
		connection := newConnection(client, true, Options{Context: context.Background(), Logger: NilLogger{}})
		done := make(chan struct{})
		go func() {
			connection.dummyLoop()
			close(done)
		}()
		connection.Close()
		<-done
		server.Close()
	}
}
