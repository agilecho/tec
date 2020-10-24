package mgo

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"tec/mongo/mgo/bson"
	"tec/mongo/mgo/internal/scram"
)

type authCmd struct {
	Authenticate int

	Nonce string
	User  string
	Key   string
}

type authResult struct {
	ErrMsg string
	Ok     bool
}

type getNonceCmd struct {
	GetNonce int
}

type getNonceResult struct {
	Nonce string
	Err   string "$err"
	Code  int
}

type logoutCmd struct {
	Logout int
}

type saslCmd struct {
	Start          int    `bson:"saslStart,omitempty"`
	Continue       int    `bson:"saslContinue,omitempty"`
	ConversationId int    `bson:"conversationId,omitempty"`
	Mechanism      string `bson:"mechanism,omitempty"`
	Payload        []byte
}

type saslResult struct {
	Ok    bool `bson:"ok"`
	NotOk bool `bson:"code"`
	Done  bool

	ConversationId int `bson:"conversationId"`
	Payload        []byte
	ErrMsg         string
}

type saslStepper interface {
	Step(serverData []byte) (clientData []byte, done bool, err error)
	Close()
}

func (socket *mongoSocket) getNonce() (nonce string, err error) {
	socket.Lock()
	for socket.cachedNonce == "" && socket.dead == nil {
		socket.gotNonce.Wait()
	}

	if socket.cachedNonce == "mongos" {
		socket.Unlock()
		return "", errors.New("Can't authenticate with mongos; see http://j.mp/mongos-auth")
	}

	nonce, err = socket.cachedNonce, socket.dead
	socket.cachedNonce = ""
	socket.Unlock()
	if err != nil {
		nonce = ""
	}
	return
}

func (socket *mongoSocket) resetNonce() {
	op := &queryOp{}
	op.query = &getNonceCmd{GetNonce: 1}
	op.collection = "admin.$cmd"
	op.limit = -1
	op.replyFunc = func(err error, reply *replyOp, docNum int, docData []byte) {
		if err != nil {
			socket.kill(errors.New("getNonce: "+err.Error()), true)
			return
		}
		result := &getNonceResult{}
		err = bson.Unmarshal(docData, &result)
		if err != nil {
			socket.kill(errors.New("Failed to unmarshal nonce: "+err.Error()), true)
			return
		}
		if result.Code == 13390 {
			result.Nonce = "mongos"
		} else if result.Nonce == "" {
			var msg string
			if result.Err != "" {
				msg = fmt.Sprintf("Got an empty nonce: %s (%d)", result.Err, result.Code)
			} else {
				msg = "Got an empty nonce"
			}
			socket.kill(errors.New(msg), true)
			return
		}
		socket.Lock()
		if socket.cachedNonce != "" {
			socket.Unlock()
			panic("resetNonce: nonce already cached")
		}
		socket.cachedNonce = result.Nonce
		socket.gotNonce.Signal()
		socket.Unlock()
	}
	err := socket.Query(op)
	if err != nil {
		socket.kill(errors.New("resetNonce: "+err.Error()), true)
	}
}

func (socket *mongoSocket) Login(cred Credential) error {
	socket.Lock()
	if cred.Mechanism == "" && socket.serverInfo.MaxWireVersion >= 3 {
		cred.Mechanism = "SCRAM-SHA-1"
	}
	for _, sockCred := range socket.creds {
		if sockCred == cred {
			socket.Unlock()
			return nil
		}
	}
	if socket.dropLogout(cred) {
		socket.creds = append(socket.creds, cred)
		socket.Unlock()
		return nil
	}
	socket.Unlock()
	var err error
	switch cred.Mechanism {
	case "", "MONGODB-CR", "MONGO-CR":
		err = socket.loginClassic(cred)
	case "PLAIN":
		err = socket.loginPlain(cred)
	case "MONGODB-X509":
		err = socket.loginX509(cred)
	default:
		err = socket.loginSASL(cred)
	}

	return err
}

func (socket *mongoSocket) loginClassic(cred Credential) error {
	nonce, err := socket.getNonce()
	if err != nil {
		return err
	}
	defer socket.resetNonce()

	psum := md5.New()
	psum.Write([]byte(cred.Username + ":mongo:" + cred.Password))

	ksum := md5.New()
	ksum.Write([]byte(nonce + cred.Username))
	ksum.Write([]byte(hex.EncodeToString(psum.Sum(nil))))

	key := hex.EncodeToString(ksum.Sum(nil))

	cmd := authCmd{Authenticate: 1, User: cred.Username, Nonce: nonce, Key: key}
	res := authResult{}
	return socket.loginRun(cred.Source, &cmd, &res, func() error {
		if !res.Ok {
			return errors.New(res.ErrMsg)
		}
		socket.Lock()
		socket.dropAuth(cred.Source)
		socket.creds = append(socket.creds, cred)
		socket.Unlock()
		return nil
	})
}

type authX509Cmd struct {
	Authenticate int
	User         string
	Mechanism    string
}

func (socket *mongoSocket) loginX509(cred Credential) error {
	cmd := authX509Cmd{Authenticate: 1, User: cred.Username, Mechanism: "MONGODB-X509"}
	res := authResult{}
	return socket.loginRun(cred.Source, &cmd, &res, func() error {
		if !res.Ok {
			return errors.New(res.ErrMsg)
		}
		socket.Lock()
		socket.dropAuth(cred.Source)
		socket.creds = append(socket.creds, cred)
		socket.Unlock()
		return nil
	})
}

func (socket *mongoSocket) loginPlain(cred Credential) error {
	cmd := saslCmd{Start: 1, Mechanism: "PLAIN", Payload: []byte("\x00" + cred.Username + "\x00" + cred.Password)}
	res := authResult{}
	return socket.loginRun(cred.Source, &cmd, &res, func() error {
		if !res.Ok {
			return errors.New(res.ErrMsg)
		}
		socket.Lock()
		socket.dropAuth(cred.Source)
		socket.creds = append(socket.creds, cred)
		socket.Unlock()
		return nil
	})
}

func (socket *mongoSocket) loginSASL(cred Credential) error {
	var sasl saslStepper
	var err error

	if cred.Mechanism == "SCRAM-SHA-1" {
		sasl = saslNewScram(cred)
	}

	if err != nil {
		return err
	}

	defer sasl.Close()

	locked := false
	lock := func(b bool) {
		if locked != b {
			locked = b
			if b {
				socket.Lock()
			} else {
				socket.Unlock()
			}
		}
	}

	lock(true)
	defer lock(false)

	start := 1
	cmd := saslCmd{}
	res := saslResult{}
	for {
		payload, done, err := sasl.Step(res.Payload)
		if err != nil {
			return err
		}
		if done && res.Done {
			socket.dropAuth(cred.Source)
			socket.creds = append(socket.creds, cred)
			break
		}
		lock(false)

		cmd = saslCmd{
			Start:          start,
			Continue:       1 - start,
			ConversationId: res.ConversationId,
			Mechanism:      cred.Mechanism,
			Payload:        payload,
		}
		start = 0
		err = socket.loginRun(cred.Source, &cmd, &res, func() error {
			lock(true)
			if !res.Ok || res.NotOk {
				return fmt.Errorf("server returned error on SASL authentication step: %s", res.ErrMsg)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if done && res.Done {
			socket.dropAuth(cred.Source)
			socket.creds = append(socket.creds, cred)
			break
		}
	}

	return nil
}

func saslNewScram(cred Credential) *saslScram {
	credsum := md5.New()
	credsum.Write([]byte(cred.Username + ":mongo:" + cred.Password))
	client := scram.NewClient(sha1.New, cred.Username, hex.EncodeToString(credsum.Sum(nil)))
	return &saslScram{cred: cred, client: client}
}

type saslScram struct {
	cred   Credential
	client *scram.Client
}

func (s *saslScram) Close() {}

func (s *saslScram) Step(serverData []byte) (clientData []byte, done bool, err error) {
	more := s.client.Step(serverData)
	return s.client.Out(), !more, s.client.Err()
}

func (socket *mongoSocket) loginRun(db string, query, result interface{}, f func() error) error {
	var mutex sync.Mutex
	var replyErr error
	mutex.Lock()

	op := queryOp{}
	op.query = query
	op.collection = db + ".$cmd"
	op.limit = -1
	op.replyFunc = func(err error, reply *replyOp, docNum int, docData []byte) {
		defer mutex.Unlock()

		if err != nil {
			replyErr = err
			return
		}

		err = bson.Unmarshal(docData, result)
		if err != nil {
			replyErr = err
		} else {
			replyErr = f()
		}
	}

	err := socket.Query(&op)
	if err != nil {
		return err
	}
	mutex.Lock()
	return replyErr
}

func (socket *mongoSocket) Logout(db string) {
	socket.Lock()
	cred, found := socket.dropAuth(db)
	if found {
		socket.logout = append(socket.logout, cred)
	}
	socket.Unlock()
}

func (socket *mongoSocket) LogoutAll() {
	socket.Lock()
	if l := len(socket.creds); l > 0 {
		socket.logout = append(socket.logout, socket.creds...)
		socket.creds = socket.creds[0:0]
	}
	socket.Unlock()
}

func (socket *mongoSocket) flushLogout() (ops []interface{}) {
	socket.Lock()
	if l := len(socket.logout); l > 0 {
		for i := 0; i != l; i++ {
			op := queryOp{}
			op.query = &logoutCmd{1}
			op.collection = socket.logout[i].Source + ".$cmd"
			op.limit = -1
			ops = append(ops, &op)
		}
		socket.logout = socket.logout[0:0]
	}
	socket.Unlock()
	return
}

func (socket *mongoSocket) dropAuth(db string) (cred Credential, found bool) {
	for i, sockCred := range socket.creds {
		if sockCred.Source == db {
			copy(socket.creds[i:], socket.creds[i+1:])
			socket.creds = socket.creds[:len(socket.creds)-1]
			return sockCred, true
		}
	}
	return cred, false
}

func (socket *mongoSocket) dropLogout(cred Credential) (found bool) {
	for i, sockCred := range socket.logout {
		if sockCred == cred {
			copy(socket.logout[i:], socket.logout[i+1:])
			socket.logout = socket.logout[:len(socket.logout)-1]
			return true
		}
	}
	return false
}
