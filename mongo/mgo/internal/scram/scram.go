package scram

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"hash"
	"strconv"
	"strings"
)

type Client struct {
	newHash func() hash.Hash

	user string
	pass string
	step int
	out  bytes.Buffer
	err  error

	clientNonce []byte
	serverNonce []byte
	saltedPass  []byte
	authMsg     bytes.Buffer
}

func NewClient(newHash func() hash.Hash, user, pass string) *Client {
	c := &Client{
		newHash: newHash,
		user:    user,
		pass:    pass,
	}
	c.out.Grow(256)
	c.authMsg.Grow(256)
	return c
}

func (c *Client) Out() []byte {
	if c.out.Len() == 0 {
		return nil
	}
	return c.out.Bytes()
}

func (c *Client) Err() error {
	return c.err
}

func (c *Client) SetNonce(nonce []byte) {
	c.clientNonce = nonce
}

var escaper = strings.NewReplacer("=", "=3D", ",", "=2C")

func (c *Client) Step(in []byte) bool {
	c.out.Reset()
	if c.step > 2 || c.err != nil {
		return false
	}
	c.step++
	switch c.step {
	case 1:
		c.err = c.step1(in)
	case 2:
		c.err = c.step2(in)
	case 3:
		c.err = c.step3(in)
	}
	return c.step > 2 || c.err != nil
}

func (c *Client) step1(in []byte) error {
	if len(c.clientNonce) == 0 {
		const nonceLen = 6
		buf := make([]byte, nonceLen + b64.EncodedLen(nonceLen))
		if _, err := rand.Read(buf[:nonceLen]); err != nil {
			return fmt.Errorf("cannot read random SCRAM-SHA-1 nonce from operating system: %v", err)
		}
		c.clientNonce = buf[nonceLen:]
		b64.Encode(c.clientNonce, buf[:nonceLen])
	}
	c.authMsg.WriteString("n=")
	escaper.WriteString(&c.authMsg, c.user)
	c.authMsg.WriteString(",r=")
	c.authMsg.Write(c.clientNonce)

	c.out.WriteString("n,,")
	c.out.Write(c.authMsg.Bytes())
	return nil
}

var b64 = base64.StdEncoding

func (c *Client) step2(in []byte) error {
	c.authMsg.WriteByte(',')
	c.authMsg.Write(in)

	fields := bytes.Split(in, []byte(","))
	if len(fields) != 3 {
		return fmt.Errorf("expected 3 fields in first SCRAM-SHA-1 server message, got %d: %q", len(fields), in)
	}
	if !bytes.HasPrefix(fields[0], []byte("r=")) || len(fields[0]) < 2 {
		return fmt.Errorf("server sent an invalid SCRAM-SHA-1 nonce: %q", fields[0])
	}
	if !bytes.HasPrefix(fields[1], []byte("s=")) || len(fields[1]) < 6 {
		return fmt.Errorf("server sent an invalid SCRAM-SHA-1 salt: %q", fields[1])
	}
	if !bytes.HasPrefix(fields[2], []byte("i=")) || len(fields[2]) < 6 {
		return fmt.Errorf("server sent an invalid SCRAM-SHA-1 iteration count: %q", fields[2])
	}

	c.serverNonce = fields[0][2:]
	if !bytes.HasPrefix(c.serverNonce, c.clientNonce) {
		return fmt.Errorf("server SCRAM-SHA-1 nonce is not prefixed by client nonce: got %q, want %q+\"...\"", c.serverNonce, c.clientNonce)
	}

	salt := make([]byte, b64.DecodedLen(len(fields[1][2:])))
	n, err := b64.Decode(salt, fields[1][2:])
	if err != nil {
		return fmt.Errorf("cannot decode SCRAM-SHA-1 salt sent by server: %q", fields[1])
	}
	salt = salt[:n]
	iterCount, err := strconv.Atoi(string(fields[2][2:]))
	if err != nil {
		return fmt.Errorf("server sent an invalid SCRAM-SHA-1 iteration count: %q", fields[2])
	}
	c.saltPassword(salt, iterCount)

	c.authMsg.WriteString(",c=biws,r=")
	c.authMsg.Write(c.serverNonce)

	c.out.WriteString("c=biws,r=")
	c.out.Write(c.serverNonce)
	c.out.WriteString(",p=")
	c.out.Write(c.clientProof())
	return nil
}

func (c *Client) step3(in []byte) error {
	var isv, ise bool
	var fields = bytes.Split(in, []byte(","))
	if len(fields) == 1 {
		isv = bytes.HasPrefix(fields[0], []byte("v="))
		ise = bytes.HasPrefix(fields[0], []byte("e="))
	}
	if ise {
		return fmt.Errorf("SCRAM-SHA-1 authentication error: %s", fields[0][2:])
	} else if !isv {
		return fmt.Errorf("unsupported SCRAM-SHA-1 final message from server: %q", in)
	}
	if !bytes.Equal(c.serverSignature(), fields[0][2:]) {
		return fmt.Errorf("cannot authenticate SCRAM-SHA-1 server signature: %q", fields[0][2:])
	}
	return nil
}

func (c *Client) saltPassword(salt []byte, iterCount int) {
	mac := hmac.New(c.newHash, []byte(c.pass))
	mac.Write(salt)
	mac.Write([]byte{0, 0, 0, 1})
	ui := mac.Sum(nil)
	hi := make([]byte, len(ui))
	copy(hi, ui)
	for i := 1; i < iterCount; i++ {
		mac.Reset()
		mac.Write(ui)
		mac.Sum(ui[:0])
		for j, b := range ui {
			hi[j] ^= b
		}
	}
	c.saltedPass = hi
}

func (c *Client) clientProof() []byte {
	mac := hmac.New(c.newHash, c.saltedPass)
	mac.Write([]byte("Client Key"))
	clientKey := mac.Sum(nil)
	hash := c.newHash()
	hash.Write(clientKey)
	storedKey := hash.Sum(nil)
	mac = hmac.New(c.newHash, storedKey)
	mac.Write(c.authMsg.Bytes())
	clientProof := mac.Sum(nil)
	for i, b := range clientKey {
		clientProof[i] ^= b
	}
	clientProof64 := make([]byte, b64.EncodedLen(len(clientProof)))
	b64.Encode(clientProof64, clientProof)
	return clientProof64
}

func (c *Client) serverSignature() []byte {
	mac := hmac.New(c.newHash, c.saltedPass)
	mac.Write([]byte("Server Key"))
	serverKey := mac.Sum(nil)

	mac = hmac.New(c.newHash, serverKey)
	mac.Write(c.authMsg.Bytes())
	serverSignature := mac.Sum(nil)

	encoded := make([]byte, b64.EncodedLen(len(serverSignature)))
	b64.Encode(encoded, serverSignature)
	return encoded
}
