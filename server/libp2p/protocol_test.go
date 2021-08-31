package p2pserver_test

import (
	"context"
	"testing"

	p2pclient "github.com/filecoin-project/indexer-reference-provider/api/v0/client/libp2p"
	"github.com/filecoin-project/indexer-reference-provider/core/engine"
	"github.com/filecoin-project/indexer-reference-provider/internal/libp2pserver"
	"github.com/filecoin-project/indexer-reference-provider/internal/utils"
	p2pserver "github.com/filecoin-project/indexer-reference-provider/server/libp2p"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/stretchr/testify/require"
)

func mkEngine(t *testing.T, h host.Host, testTopic string) (*engine.Engine, error) {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	store := dssync.MutexWrap(datastore.NewMapDatastore())

	return engine.New(context.Background(), priv, h, store, testTopic)
}
func setupServer(ctx context.Context, t *testing.T) (*libp2pserver.Server, host.Host, *engine.Engine) {
	h, err := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	require.NoError(t, err)
	e, err := mkEngine(t, h, "test/topic")
	require.NoError(t, err)
	s := p2pserver.New(ctx, h, e)
	return s, h, e
}

func setupClient(ctx context.Context, t *testing.T) (*p2pclient.Provider, host.Host) {
	h, err := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	require.NoError(t, err)
	c, err := p2pclient.NewProvider(ctx, h)
	require.NoError(t, err)
	return c, h
}

func connect(ctx context.Context, t *testing.T, h1 host.Host, h2 host.Host) {
	err := h1.Connect(ctx, *host.InfoFromHost(h2))
	require.NoError(t, err)
}

func TestAdvertisements(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize everything
	s, sh, e := setupServer(ctx, t)
	c, ch := setupClient(ctx, t)
	connect(ctx, t, ch, sh)

	// Publish some new advertisements.
	cids, _ := utils.RandomCids(10)
	c1, err := e.NotifyPutCids(ctx, cids, []byte("some metadata"))
	require.NoError(t, err)
	cids, _ = utils.RandomCids(10)
	c2, err := e.NotifyPutCids(ctx, cids, []byte("some metadata"))
	require.NoError(t, err)

	// Get first advertisement
	r, err := c.GetAdv(ctx, s.ID(), c1)
	require.NoError(t, err)
	ad, err := e.GetAdv(ctx, c1)
	require.NoError(t, err)
	require.True(t, ipld.DeepEqual(r.Ad, ad))

	// Get latest advertisement
	r, err = c.GetLatestAdv(ctx, s.ID())
	require.NoError(t, err)
	id, ad, err := e.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, r.ID, id)
	require.Equal(t, r.ID, c2)
	require.True(t, ipld.DeepEqual(r.Ad, ad))

	// Get non-existing advertisement by id
	r, err = c.GetAdv(ctx, s.ID(), cids[0])
	require.Nil(t, r)
	require.Error(t, err)
	require.Equal(t, "datastore: key not found", err.Error())

}
