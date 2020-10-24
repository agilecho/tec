package mysql

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
)

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

	return rsa.EncryptOAEP(sha1.New(), rand.Reader, pub, plain, nil)
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
	case "mysql_native_password":
		authResp := scramblePassword(authData[:20], mc.cfg.Passwd)
		return authResp, nil

	case "sha256_password":
		if len(mc.cfg.Passwd) == 0 {
			return []byte{0}, nil
		}
		if mc.cfg.Net == "unix" {
			return append([]byte(mc.cfg.Passwd), 0), nil
		}


			return []byte{1}, nil

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
