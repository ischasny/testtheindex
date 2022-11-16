package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/network"
	gstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/filecoin-project/index-provider/cardatatransfer"
	"github.com/filecoin-project/index-provider/cmd/provider/internal/config"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/engine/policy"

	adminserver "github.com/filecoin-project/index-provider/server/admin/http"
	reframeserver "github.com/filecoin-project/index-provider/server/reframe/http"
	"github.com/filecoin-project/index-provider/supplier"
	leveldb "github.com/ipfs/go-ds-leveldb"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/kubo/core/bootstrap"
	"github.com/ipld/go-car/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("command/reference-provider")

var (
	ErrDaemonStart  = errors.New("daemon did not start correctly")
	ErrDaemonStop   = errors.New("daemon did not stop gracefully")
	ErrCleanupFiles = errors.New("unable to cleanup temporary files correctly")
)

const (
	// shutdownTimeout is the duration that a graceful shutdown has to complete
	shutdownTimeout = 5 * time.Second
)

var DaemonCmd = &cli.Command{
	Name:   "daemon",
	Usage:  "Starts a reference provider",
	Flags:  daemonFlags,
	Action: daemonCommand,
}

func daemonCommand(cctx *cli.Context) error {
	err := logging.SetLogLevel("*", cctx.String("log-level"))
	if err != nil {
		return err
	}

	cfg, err := config.Load("")
	if err != nil {
		if err == config.ErrNotInitialized {
			return errors.New("reference provider is not initialized\nTo initialize, run using the \"init\" command")
		}
		return fmt.Errorf("cannot load config file: %w", err)
	}

	// Initialize libp2p host
	ctx, cancelp2p := context.WithCancel(cctx.Context)
	defer cancelp2p()

	peerID, privKey, err := cfg.Identity.DecodeOrCreate(cctx.App.Writer)
	if err != nil {
		return err
	}

	syncPolicy, err := policy.New(cfg.Ingest.SyncPolicy.Allow, cfg.Ingest.SyncPolicy.Except)
	if err != nil {
		return err
	}

	p2pmaddr, err := multiaddr.NewMultiaddr(cfg.ProviderServer.ListenMultiaddr)
	if err != nil {
		return fmt.Errorf("bad p2p address in config %s: %s", cfg.ProviderServer.ListenMultiaddr, err)
	}
	h, err := libp2p.New(
		// Use the keypair generated during init
		libp2p.Identity(privKey),
		// Listen to p2p addr specified in config
		libp2p.ListenAddrs(p2pmaddr),
	)
	if err != nil {
		return err
	}
	log.Infow("libp2p host initialized", "host_id", h.ID(), "multiaddr", p2pmaddr)

	// Initialize datastore
	if cfg.Datastore.Type != "levelds" {
		return fmt.Errorf("only levelds datastore type supported, %q not supported", cfg.Datastore.Type)
	}
	dataStorePath, err := config.Path("", cfg.Datastore.Dir)
	if err != nil {
		return err
	}
	err = checkWritable(dataStorePath)
	if err != nil {
		return err
	}
	ds, err := leveldb.NewDatastore(dataStorePath, nil)
	if err != nil {
		return err
	}

	gsnet := gsnet.NewFromLibp2pHost(h)
	dtNet := dtnetwork.NewFromLibp2pHost(h)
	gs := gsimpl.New(context.Background(), gsnet, cidlink.DefaultLinkSystem())
	tp := gstransport.NewTransport(h.ID(), gs)
	dt, err := datatransfer.NewDataTransfer(ds, dtNet, tp)
	if err != nil {
		return err
	}
	err = dt.Start(context.Background())
	if err != nil {
		return err
	}

	// Starting provider core
	eng, err := engine.New(
		engine.WithDatastore(ds),
		engine.WithDataTransfer(dt),
		engine.WithDirectAnnounce(cfg.DirectAnnounce.URLs...),
		engine.WithHost(h),
		engine.WithEntriesCacheCapacity(cfg.Ingest.LinkCacheSize),
		engine.WithChainedEntries(cfg.Ingest.LinkedChunkSize),
		engine.WithTopicName(cfg.Ingest.PubSubTopic),
		engine.WithPublisherKind(engine.PublisherKind(cfg.Ingest.PublisherKind)),
		engine.WithSyncPolicy(syncPolicy))
	if err != nil {
		return err
	}

	err = eng.Start(ctx)
	if err != nil {
		return err
	}

	// Instantiate CAR supplier and register it as the multihash lister onto the engine.
	cs := supplier.NewCarSupplier(eng, ds, car.ZeroLengthSectionAsEOF(carZeroLengthAsEOFFlagValue))

	// Start serving CAR files for retrieval requests
	err = cardatatransfer.StartCarDataTransfer(dt, cs)
	if err != nil {
		return err
	}

	// TODO: unclear why the admin config takes multiaddr if it is always converted to net addr; simplify.
	addr, err := cfg.AdminServer.ListenNetAddr()
	if err != nil {
		return err
	}

	adminSvr, err := adminserver.New(
		h,
		privKey,
		eng,
		cs,
		adminserver.WithListenAddr(addr),
		adminserver.WithReadTimeout(time.Duration(cfg.AdminServer.ReadTimeout)),
		adminserver.WithWriteTimeout(time.Duration(cfg.AdminServer.WriteTimeout)),
	)

	if err != nil {
		return err
	}
	log.Infow("admin server initialized", "address", cfg.AdminServer.ListenMultiaddr)

	adminErrChan := make(chan error, 1)
	fmt.Fprintf(cctx.App.ErrWriter, "Starting admin server on %s ...", cfg.AdminServer.ListenMultiaddr)
	go func() {
		adminErrChan <- adminSvr.Start()
	}()

	reframeErrChan := make(chan error, 1)
	// If there are bootstrap peers and bootstrapping is enabled, then try to
	// connect to the minimum set of peers.
	if len(cfg.Bootstrap.Peers) != 0 && cfg.Bootstrap.MinimumPeers != 0 {
		addrs, err := cfg.Bootstrap.PeerAddrs()
		if err != nil {
			return fmt.Errorf("bad bootstrap peer: %s", err)
		}

		bootCfg := bootstrap.BootstrapConfigWithPeers(addrs)
		bootCfg.MinPeerThreshold = cfg.Bootstrap.MinimumPeers

		bootstrapper, err := bootstrap.Bootstrap(peerID, h, nil, bootCfg)
		if err != nil {
			return fmt.Errorf("bootstrap failed: %s", err)
		}
		defer bootstrapper.Close()
	}

	// setting up reframe server
	var reframeSrv *reframeserver.Server
	if len(cfg.Reframe.ListenMultiaddr) != 0 {
		reframeAddr, err := cfg.Reframe.ListenNetAddr()
		if err != nil {
			return err
		}

		reframeSrv, err = reframeserver.New(
			time.Duration(cfg.Reframe.CidTtl),
			cfg.Reframe.ChunkSize,
			cfg.Reframe.SnapshotSize,
			cfg.Reframe.ProviderID,
			cfg.Reframe.Addrs,
			eng,
			ds,
			reframeserver.WithListenAddr(reframeAddr),
			reframeserver.WithReadTimeout(time.Duration(cfg.Reframe.ReadTimeout)),
			reframeserver.WithWriteTimeout(time.Duration(cfg.Reframe.WriteTimeout)),
		)

		if err != nil {
			return err
		}
		log.Infow("reframe server initialized", "address", cfg.Reframe.ListenMultiaddr)

		fmt.Fprintf(cctx.App.ErrWriter, "Starting reframe server on %s ...", cfg.Reframe.ListenMultiaddr)
		go func() {
			reframeErrChan <- reframeSrv.Start()
		}()
	}

	var finalErr error
	// Keep process running.
	select {
	case <-cctx.Done():
	case err = <-adminErrChan:
		log.Errorw("Failed to start admin server", "err", err)
		finalErr = ErrDaemonStart
	case err = <-reframeErrChan:
		log.Errorw("Failed to start reframe server", "err", err)
		finalErr = ErrDaemonStart
	}

	log.Infow("Shutting down daemon")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	go func() {
		// Wait for context to be canceled. If timeout, then exit with error.
		<-shutdownCtx.Done()
		if shutdownCtx.Err() == context.DeadlineExceeded {
			fmt.Println("Timed out on shutdown, terminating...")
			os.Exit(-1)
		}
	}()

	if err = eng.Shutdown(); err != nil {
		log.Errorf("Error closing provider core: %s", err)
		finalErr = ErrDaemonStop
	}

	if err = ds.Close(); err != nil {
		log.Errorf("Error closing provider datastore: %s", err)
		finalErr = ErrDaemonStop
	}

	// cancel libp2p server
	cancelp2p()

	if err = adminSvr.Shutdown(shutdownCtx); err != nil {
		log.Errorw("Error shutting down admin server: %s", err)
		finalErr = ErrDaemonStop
	}
	if reframeSrv != nil {
		if err = reframeSrv.Shutdown(shutdownCtx); err != nil {
			log.Errorw("Error shutting down reframe server.", "err", err)
			finalErr = ErrDaemonStop
		}
	}
	log.Infow("node stopped")
	return finalErr
}
