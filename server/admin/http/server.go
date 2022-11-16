package adminserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"

	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/supplier"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("adminserver")
var mhsByCtxId sync.Map

type Server struct {
	server *http.Server
	p      crypto.PrivKey
	l      net.Listener
	h      host.Host
	e      *engine.Engine
}

func New(h host.Host, priv crypto.PrivKey, e *engine.Engine, cs *supplier.CarSupplier, o ...Option) (*Server, error) {
	mhsByCtxId = sync.Map{}

	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}

	l, err := net.Listen("tcp", opts.listenAddr)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter().StrictSlash(true)
	server := &http.Server{
		Handler:      r,
		ReadTimeout:  opts.readTimeout,
		WriteTimeout: opts.writeTimeout,
	}
	s := &Server{server, priv, l, h, e}

	s.e.RegisterMultihashLister(func(ctx context.Context, providerID peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		mhs, ok := mhsByCtxId.Load(string(contextID))
		if !ok {
			return nil, errors.New("unknown context id")
		}
		return provider.SliceMultihashIterator(mhs.([]multihash.Multihash)), nil
	})

	// Set protocol handlers
	r.HandleFunc("/admin/announce", s.announceHandler).
		Methods(http.MethodPost)
	r.HandleFunc("/admin/announcehttp", s.announceHttpHandler).
		Methods(http.MethodPost)

	r.HandleFunc("/admin/connect", s.connectHandler).
		Methods(http.MethodPost).
		Headers("Content-Type", "application/json")

	cHandler := &carHandler{cs}
	r.HandleFunc("/admin/import/car", cHandler.handleImport).
		Methods(http.MethodPost).
		Headers("Content-Type", "application/json")

	r.HandleFunc("/admin/remove/car", cHandler.handleRemove).
		Methods(http.MethodPost).
		Headers("Content-Type", "application/json")

	r.HandleFunc("/admin/list/car", cHandler.handleList).
		Methods(http.MethodGet)

	r.HandleFunc("/admin/randomAd", s.randomAdHandler).
		Methods(http.MethodPost)

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("admin http server listening", "addr", s.l.Addr())
	return s.server.Serve(s.l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	log.Info("admin http server shutdown")
	return s.server.Shutdown(ctx)
}
