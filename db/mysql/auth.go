package mysql

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"sync"
)

var (
	serverPubKeyLock     sync.RWMutex
	serverPubKeyRegistry map[string]*rsa.PublicKey
)

func getServerPubKey(name string) (pubKey *rsa.PublicKey) {
	serverPubKeyLock.RLock()
	if v, ok := serverPubKeyRegistry[name]; ok {
		pubKey = v
	}
	serverPubKeyLock.RUnlock()
	return
}

type myRnd struct {
	seed1, seed2 uint32
}

const myRndMaxVal = 0x3FFFFFFF

func newMyRnd(seed1, seed2 uint32) *myRnd {
	return &myRnd{
		seed1: seed1 % myRndMaxVal,
		seed2: seed2 % myRndMaxVal,
	}
}

func (r *myRnd) NextByte() byte {
	r.seed1 = (r.seed1*3 + r.seed2) % myRndMaxVal
	r.seed2 = (r.seed1 + r.seed2 + 33) % myRndMaxVal

	return byte(uint64(r.seed1) * 31 / myRndMaxVal)
}

func pwHash(password []byte) (result [2]uint32) {
	var add uint32 = 7
	var tmp uint32

	result[0] = 1345345333
	result[1] = 0x12345671

	for _, c := range password {
		if c == ' ' || c == '\t' {
			continue
		}

		tmp = uint32(c)
		result[0] ^= (((result[0] & 63) + add) * tmp) + (result[0] << 8)
		result[1] += (result[1] << 8) ^ result[0]
		add += tmp
	}

	result[0] &= 0x7FFFFFFF
	result[1] &= 0x7FFFFFFF

	return
}

func scrambleOldPassword(scramble []byte, password string) []byte {
	scramble = scramble[:8]

	hashPw := pwHash([]byte(password))
	hashSc := pwHash(scramble)

	r := newMyRnd(hashPw[0]^hashSc[0], hashPw[1]^hashSc[1])

	var out [8]byte
	for i := range out {
		out[i] = r.NextByte() + 64
	}

	mask := r.NextByte()
	for i := range out {
		out[i] ^= mask
	}

	return out[:]
}

func scramblePassword(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	crypt := sha1.New()
	crypt.Write([]byte(password))
	stage1 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

func scrambleSHA256Password(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	crypt := sha256.New()
	crypt.Write([]byte(password))
	message1 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1)
	message1Hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1Hash)
	crypt.Write(scramble)
	message2 := crypt.Sum(nil)

	for i := range message1 {
		message1[i] ^= message2[i]
	}

	return message1
}

func encryptPassword(password string, seed []byte, pub *rsa.PublicKey) ([]byte, error) {
	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		j := i % len(seed)
		plain[i] ^= seed[j]
	}
	sha1 := sha1.New()
	return rsa.EncryptOAEP(sha1, rand.Reader, pub, plain, nil)
}

func (mc *mysqlConn) sendEncryptedPassword(seed []byte, pub *rsa.PublicKey) error {
	enc, err := encryptPassword(mc.cfg.Passwd, seed, pub)
	if err != nil {
		return err
	}
	return mc.writeAuthSwitchPacket(enc)
}

func (mc *mysqlConn) auth(authData []byte, plugin string) ([]byte, error) {
	switch plugin {
	case "caching_sha2_password":
		authResp := scrambleSHA256Password(authData, mc.cfg.Passwd)
		return authResp, nil

	case "mysql_old_password":
		if !mc.cfg.AllowOldPasswords {
			return nil, ErrOldPassword
		}
		if len(mc.cfg.Passwd) == 0 {
			return nil, nil
		}
		authResp := append(scrambleOldPassword(authData[:8], mc.cfg.Passwd), 0)
		return authResp, nil

	case "mysql_clear_password":
		if !mc.cfg.AllowCleartextPasswords {
			return nil, ErrCleartextPassword
		}
		return append([]byte(mc.cfg.Passwd), 0), nil

	case "mysql_native_password":
		if !mc.cfg.AllowNativePasswords {
			return nil, ErrNativePassword
		}
		authResp := scramblePassword(authData[:20], mc.cfg.Passwd)
		return authResp, nil

	case "sha256_password":
		if len(mc.cfg.Passwd) == 0 {
			return []byte{0}, nil
		}
		if mc.cfg.tls != nil || mc.cfg.Net == "unix" {
			return append([]byte(mc.cfg.Passwd), 0), nil
		}

		pubKey := mc.cfg.pubKey
		if pubKey == nil {
			return []byte{1}, nil
		}

		enc, err := encryptPassword(mc.cfg.Passwd, authData, pubKey)
		return enc, err

	default:
		errLog.Print("unknown auth plugin:", plugin)
		return nil, ErrUnknownPlugin
	}
}

func (mc *mysqlConn) handleAuthResult(oldAuthData []byte, plugin string) error {
	authData, newPlugin, err := mc.readAuthResult()
	if err != nil {
		return err
	}

	if newPlugin != "" {
		if authData == nil {
			authData = oldAuthData
		} else {
			copy(oldAuthData, authData)
		}

		plugin = newPlugin

		authResp, err := mc.auth(authData, plugin)
		if err != nil {
			return err
		}
		if err = mc.writeAuthSwitchPacket(authResp); err != nil {
			return err
		}

		authData, newPlugin, err = mc.readAuthResult()
		if err != nil {
			return err
		}

		if newPlugin != "" {
			return ErrMalformPkt
		}
	}

	switch plugin {
	case "caching_sha2_password":
		switch len(authData) {
		case 0:
			return nil
		case 1:
			switch authData[0] {
			case cachingSha2PasswordFastAuthSuccess:
				if err = mc.readResultOK(); err == nil {
					return nil
				}

			case cachingSha2PasswordPerformFullAuthentication:
				if mc.cfg.tls != nil || mc.cfg.Net == "unix" {
					err = mc.writeAuthSwitchPacket(append([]byte(mc.cfg.Passwd), 0))
					if err != nil {
						return err
					}
				} else {
					pubKey := mc.cfg.pubKey
					if pubKey == nil {
						data, err := mc.buf.takeSmallBuffer(4 + 1)
						if err != nil {
							return err
						}
						data[4] = cachingSha2PasswordRequestPublicKey
						mc.writePacket(data)

						if data, err = mc.readPacket(); err != nil {
							return err
						}

						block, _ := pem.Decode(data[1:])
						pkix, err := x509.ParsePKIXPublicKey(block.Bytes)
						if err != nil {
							return err
						}
						pubKey = pkix.(*rsa.PublicKey)
					}

					err = mc.sendEncryptedPassword(oldAuthData, pubKey)
					if err != nil {
						return err
					}
				}
				return mc.readResultOK()

			default:
				return ErrMalformPkt
			}
		default:
			return ErrMalformPkt
		}

	case "sha256_password":
		switch len(authData) {
		case 0:
			return nil
		default:
			block, _ := pem.Decode(authData)
			pub, err := x509.ParsePKIXPublicKey(block.Bytes)
			if err != nil {
				return err
			}

			err = mc.sendEncryptedPassword(oldAuthData, pub.(*rsa.PublicKey))
			if err != nil {
				return err
			}
			return mc.readResultOK()
		}

	default:
		return nil
	}

	return err
}
