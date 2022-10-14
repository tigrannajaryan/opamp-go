package server

import (
	"context"
	"encoding/binary"
	"net"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/server/types"
)

// wsConnection represents a persistent OpAMP connection over a WebSocket.
type wsConnection struct {
	wsConn *websocket.Conn
}

var _ types.Connection = (*wsConnection)(nil)

func (c wsConnection) RemoteAddr() net.Addr {
	return c.wsConn.RemoteAddr()
}

// Message header is currently uint64 zero value.
const msgHeader = uint64(0)

func (c wsConnection) Send(_ context.Context, message *protobufs.ServerToAgent) error {
	bytes, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	writer, err := c.wsConn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return err
	}

	// Encode header as a varint.
	hdrBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(hdrBuf, msgHeader)
	hdrBuf = hdrBuf[:n]

	// Write the header bytes.
	_, err = writer.Write(hdrBuf)
	if err != nil {
		writer.Close()
		return err
	}

	// Write the encoded data.
	_, err = writer.Write(bytes)
	if err != nil {
		writer.Close()
		return err
	}
	return writer.Close()
}

func (c wsConnection) Disconnect() error {
	return c.wsConn.Close()
}
