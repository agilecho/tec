package amqp

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
)

var errURIScheme = errors.New("AMQP scheme must be either 'amqp://' or 'amqps://'")
var errURIWhitespace = errors.New("URI must not contain whitespace")

var schemePorts = map[string]int{
	"amqp":  5672,
	"amqps": 5671,
}

var defaultURI = URI{
	Scheme:   "amqp",
	Host:     "localhost",
	Port:     5672,
	Username: "guest",
	Password: "guest",
	Vhost:    "/",
}

type URI struct {
	Scheme   string
	Host     string
	Port     int
	Username string
	Password string
	Vhost    string
}

func ParseURI(uri string) (URI, error) {
	builder := defaultURI

	if strings.Contains(uri, " ") == true {
		return builder, errURIWhitespace
	}

	u, err := url.Parse(uri)
	if err != nil {
		return builder, err
	}

	defaultPort, okScheme := schemePorts[u.Scheme]

	if okScheme {
		builder.Scheme = u.Scheme
	} else {
		return builder, errURIScheme
	}

	host := u.Hostname()
	port := u.Port()

	if host != "" {
		builder.Host = host
	}

	if port != "" {
		port32, err := strconv.ParseInt(port, 10, 32)
		if err != nil {
			return builder, err
		}
		builder.Port = int(port32)
	} else {
		builder.Port = defaultPort
	}

	if u.User != nil {
		builder.Username = u.User.Username()
		if password, ok := u.User.Password(); ok {
			builder.Password = password
		}
	}

	if u.Path != "" {
		if strings.HasPrefix(u.Path, "/") {
			if u.Host == "" && strings.HasPrefix(u.Path, "///") {
				if len(u.Path) > 3 {
					builder.Vhost = u.Path[3:]
				}
			} else if len(u.Path) > 1 {
				builder.Vhost = u.Path[1:]
			}
		} else {
			builder.Vhost = u.Path
		}
	}

	return builder, nil
}

func (uri URI) PlainAuth() *PlainAuth {
	return &PlainAuth{
		Username: uri.Username,
		Password: uri.Password,
	}
}

func (uri URI) AMQPlainAuth() *AMQPlainAuth {
	return &AMQPlainAuth{
		Username: uri.Username,
		Password: uri.Password,
	}
}

func (uri URI) String() string {
	authority, err := url.Parse("")
	if err != nil {
		return err.Error()
	}

	authority.Scheme = uri.Scheme

	if uri.Username != defaultURI.Username || uri.Password != defaultURI.Password {
		authority.User = url.User(uri.Username)

		if uri.Password != defaultURI.Password {
			authority.User = url.UserPassword(uri.Username, uri.Password)
		}
	}

	authority.Host = net.JoinHostPort(uri.Host, strconv.Itoa(uri.Port))

	if defaultPort, found := schemePorts[uri.Scheme]; !found || defaultPort != uri.Port {
		authority.Host = net.JoinHostPort(uri.Host, strconv.Itoa(uri.Port))
	} else {
		authority.Host = strings.TrimSuffix(net.JoinHostPort(uri.Host, ""), ":")
	}

	if uri.Vhost != defaultURI.Vhost {
		authority.Path = uri.Vhost
		authority.RawPath = url.QueryEscape(uri.Vhost)
	} else {
		authority.Path = "/"
	}

	return authority.String()
}
