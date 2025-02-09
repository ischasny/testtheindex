package provider_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/cardatatransfer"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/index-provider/supplier"
	"github.com/filecoin-project/index-provider/testutil"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/dagsync"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

const testTopic = "test/topic"

type testCase struct {
	name             string
	serverConfigOpts func(*testing.T) []engine.Option
}

var testCases = []testCase{
	{
		name: "DT Publisher",
		serverConfigOpts: func(t *testing.T) []engine.Option {
			// Use env var to signal what publisher kind is being used.
			setPubKindEnvVarKey(t, engine.DataTransferPublisher)
			return []engine.Option{
				engine.WithTopicName(testTopic),
				engine.WithPublisherKind(engine.DataTransferPublisher),
			}
		},
	},
	{
		name: "HTTP Publisher",
		serverConfigOpts: func(t *testing.T) []engine.Option {
			// Use env var to signal what publisher kind is being used.
			setPubKindEnvVarKey(t, engine.HttpPublisher)
			return []engine.Option{
				engine.WithTopicName(testTopic),
				engine.WithPublisherKind(engine.HttpPublisher),
			}
		},
	},
}

// setPubKindEnvVarKey to signal to newTestServer, which publisher kind is being used so that
// the test server can be configured correctly.
func setPubKindEnvVarKey(t *testing.T, kind engine.PublisherKind) {
	// Set env var via direct call to os instead of t.SetEnv, because CI runs tests on 1.16 and
	// that function is only available after 1.17
	key := pubKindEnvVarKey(t)
	err := os.Setenv(key, string(kind))
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Unsetenv(key)
	})
}

func TestRetrievalRoundTrip(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testRetrievalRoundTripWithTestCase(t, tc)
		})
	}
}

func testRetrievalRoundTripWithTestCase(t *testing.T, tc testCase) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Initialize everything
	server := newTestServer(t, ctx, tc.serverConfigOpts(t)...)
	client := newTestClient(t)
	disseminateNetworkState(server.h, client.h)

	carBs := testutil.OpenSampleCar(t, "sample-v1-2.car")
	roots, err := carBs.Roots()
	require.NoError(t, err)
	require.Len(t, roots, 1)
	carBs.Close()

	contextID := []byte("applesauce")
	tp, err := cardatatransfer.TransportFromContextID(contextID)
	require.NoError(t, err)
	md := metadata.Default.New(tp)
	advCid, err := server.cs.Put(ctx, contextID, filepath.Join(testutil.ThisDir(t), "./testdata/sample-v1-2.car"), md)
	require.NoError(t, err)

	// TODO: Review after resolution of https://github.com/filecoin-project/go-legs/issues/95
	// For now instantiate a new host for subscriber so that dt constructed by test client works.
	subHost := newHost(t)
	sub, err := dagsync.NewSubscriber(subHost, client.store, client.lsys, testTopic, nil)
	require.NoError(t, err)

	headCid, err := sub.Sync(ctx, server.h.ID(), cid.Undef, nil, server.publisherAddr)
	require.NoError(t, err)
	require.Equal(t, advCid, headCid)

	// Close the subscriber so it doesn't interfere with the next data transfer.
	err = sub.Close()
	require.NoError(t, err)

	// Get first advertisement
	advNode, err := client.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: advCid}, schema.AdvertisementPrototype)
	require.NoError(t, err)
	adv, err := schema.UnwrapAdvertisement(advNode)
	require.NoError(t, err)

	dtm := &metadata.GraphsyncFilecoinV1{}
	err = dtm.UnmarshalBinary(adv.Metadata)
	require.NoError(t, err)

	proposal := &cardatatransfer.DealProposal{
		PayloadCID: roots[0],
		ID:         1,
		Params: cardatatransfer.Params{
			PieceCID: &dtm.PieceCID,
		},
	}
	done := make(chan bool, 1)
	unsub := client.dt.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		switch channelState.Status() {
		case datatransfer.Failed, datatransfer.Cancelled:
			done <- false
		case datatransfer.Completed:
			done <- true
		}
	})
	defer unsub()
	err = client.dt.RegisterVoucherResultType(&cardatatransfer.DealResponse{})
	require.NoError(t, err)
	err = client.dt.RegisterVoucherType(&cardatatransfer.DealProposal{}, nil)
	require.NoError(t, err)
	_, err = client.dt.OpenPullDataChannel(ctx, server.h.ID(), proposal, roots[0], selectorparse.CommonSelector_ExploreAllRecursively)
	require.NoError(t, err)

	select {
	case <-ctx.Done():
		require.FailNow(t, "context closed")
	case result := <-done:
		require.True(t, result)
	}
}

func TestReimportCar(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testReimportCarWtihTestCase(t, tc)
		})
	}
}

func testReimportCarWtihTestCase(t *testing.T, tc testCase) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := newTestServer(t, ctx, tc.serverConfigOpts(t)...)
	client := newTestClient(t)
	disseminateNetworkState(server.h, client.h)

	contextID := []byte("applesauce")
	tp, err := cardatatransfer.TransportFromContextID(contextID)
	require.NoError(t, err)
	md := metadata.Default.New(tp)
	advCid, err := server.cs.Put(ctx, contextID, filepath.Join(testutil.ThisDir(t), "./testdata/sample-v1-2.car"), md)
	require.NoError(t, err)

	// TODO: Review after resolution of https://github.com/filecoin-project/go-legs/issues/95
	// For now instantiate a new host for subscriber so that dt constructed by test client works.
	subHost := newHost(t)
	sub, err := dagsync.NewSubscriber(subHost, client.store, client.lsys, testTopic, nil)
	require.NoError(t, err)

	headCid, err := sub.Sync(ctx, server.h.ID(), cid.Undef, nil, server.publisherAddr)
	require.NoError(t, err)
	require.Equal(t, advCid, headCid)

	// Get first advertisement
	advNode, err := client.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: advCid}, schema.AdvertisementPrototype)
	require.NoError(t, err)
	adv, err := schema.UnwrapAdvertisement(advNode)
	require.NoError(t, err)

	receivedMd := metadata.Default.New()
	err = receivedMd.UnmarshalBinary(adv.Metadata)
	require.NoError(t, err)

	// Check the reimporting CAR with same contextID and metadata does not
	// result in advertisement.
	_, err = server.cs.Put(ctx, contextID, filepath.Join(testutil.ThisDir(t), "./testdata/sample-v1-2.car"), md)
	require.Equal(t, err, provider.ErrAlreadyAdvertised)

	// Test that reimporting CAR with same contextID and different metadata generates new advertisement.
	contextID2 := []byte("applesauce2")
	tp2, err := cardatatransfer.TransportFromContextID(contextID2)
	require.NoError(t, err)
	md2 := metadata.Default.New(tp2)
	advCid2, err := server.cs.Put(ctx, contextID, filepath.Join(testutil.ThisDir(t), "./testdata/sample-v1-2.car"), md2)
	require.NoError(t, err)

	// Sync the new advertisement
	headCid, err = sub.Sync(ctx, server.h.ID(), cid.Undef, nil, server.publisherAddr)
	require.NoError(t, err)
	require.Equal(t, advCid2, headCid)

	// Close the subscriber so it doesn't interfere with the next data transfer.
	err = sub.Close()
	require.NoError(t, err)

	// Get second advertisement
	advNode2, err := client.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: advCid2}, schema.AdvertisementPrototype)
	require.NoError(t, err)
	adv2, err := schema.UnwrapAdvertisement(advNode2)
	require.NoError(t, err)

	receivedMd2 := metadata.Default.New()
	err = receivedMd2.UnmarshalBinary(adv2.Metadata)
	require.NoError(t, err)

	require.NotEqual(t, receivedMd2, receivedMd)

	// Check that both advertisements have the same entries link.
	linkCid := adv.Entries.(cidlink.Link).Cid
	linkCid2 := adv2.Entries.(cidlink.Link).Cid
	require.True(t, linkCid.Equals(linkCid2))
}

func disseminateNetworkState(hosts ...host.Host) {
	for _, one := range hosts {
		for _, other := range hosts {
			if one.ID() != other.ID() {
				one.Peerstore().AddAddrs(other.ID(), other.Addrs(), time.Hour)
			}
		}
	}
}

type testServer struct {
	h             host.Host
	cs            *supplier.CarSupplier
	e             *engine.Engine
	publisherAddr multiaddr.Multiaddr
}

func newTestServer(t *testing.T, ctx context.Context, o ...engine.Option) *testServer {
	// Explicitly override host so that the host is known for testing purposes.
	h := newHost(t)
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	dt := testutil.SetupDataTransferOnHost(t, h, store, cidlink.DefaultLinkSystem())
	o = append(o, engine.WithHost(h), engine.WithDatastore(store), engine.WithDataTransfer(dt))

	var publisherAddr multiaddr.Multiaddr
	pubKind := engine.PublisherKind(os.Getenv(pubKindEnvVarKey(t)))
	if pubKind == engine.HttpPublisher {
		var err error
		port := findOpenPort(t)
		publisherAddr, err = multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/" + port + "/http")
		require.NoError(t, err)
		o = append(o, engine.WithHttpPublisherListenAddr("0.0.0.0:"+port))
	} else {
		publisherAddr = h.Addrs()[0]
	}

	e, err := engine.New(o...)
	require.NoError(t, err)
	require.NoError(t, e.Start(ctx))

	cs := supplier.NewCarSupplier(e, store, car.ZeroLengthSectionAsEOF(false))
	require.NoError(t, cardatatransfer.StartCarDataTransfer(dt, cs))

	return &testServer{
		h:             h,
		cs:            cs,
		e:             e,
		publisherAddr: publisherAddr,
	}
}

func pubKindEnvVarKey(t *testing.T) string {
	return t.Name() + "_publisher_kind"
}

type testClient struct {
	h     host.Host
	dt    datatransfer.Manager
	lsys  ipld.LinkSystem
	store datastore.Batching
}

func newTestClient(t *testing.T) *testClient {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	blockStore := blockstore.NewBlockstore(store)
	lsys := storeutil.LinkSystemForBlockstore(blockStore)
	h := newHost(t)
	dt := testutil.SetupDataTransferOnHost(t, h, store, lsys)
	return &testClient{
		h:     h,
		dt:    dt,
		lsys:  lsys,
		store: store,
	}
}

func newHost(t *testing.T) host.Host {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() {
		h.Close()
	})
	return h
}

func findOpenPort(t *testing.T) string {
	l, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	defer l.Close()
	parts := strings.Split(l.Addr().String(), ":")
	return parts[len(parts)-1]
}
