//go:build !race

package reframe_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/filecoin-project/index-provider/engine"
	mock_provider "github.com/filecoin-project/index-provider/mock"
	reframelistener "github.com/filecoin-project/index-provider/reframe"
	"github.com/filecoin-project/index-provider/testutil"
	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/libp2p/go-libp2p"
	"github.com/stretchr/testify/require"
)

func TestHandleConcurrentRequests(t *testing.T) {
	ttl := 24 * time.Hour
	chunkSize := 1000
	snapshotSize := 1000
	concurrencyFactor := 10

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()

	cids := make([]cid.Cid, concurrencyFactor)
	for i := 0; i < len(cids); i++ {
		cids[i] = newCid(fmt.Sprintf("test%d", i))
	}

	prov := newProvider(t, pID)
	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())

	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	client, server := createClientAndServer(t, listener, prov, priv)
	defer server.Close()

	chans := make([]chan bool, len(cids))
	for i, c := range cids {
		ch := make(chan bool, 1)
		cc := c
		chans[i] = ch
		go func() {
			provide(t, client, ctx, cc)
			ch <- true
		}()
	}

	for _, ch := range chans {
		<-ch
	}

	for _, c := range cids {
		require.True(t, reframelistener.CidExist(ctx, listener, c, false))
	}
}

func TestShouldProcessMillionCIDsInThirtySeconds(t *testing.T) {
	// this test can cause race detecting issues because of the stats reporter that gets accessed from multiple goroutines
	cidsNumber := 1_000_000
	timeExpectation := 30 * time.Second
	chunkSize := 10000
	snapshotSize := 1000
	ttl := 24 * time.Hour

	h, err := libp2p.New()
	require.NoError(t, err)
	priv, _, pID := testutil.GenerateKeysAndIdentity(t)
	ctx := context.Background()

	engine, err := engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.DataTransferPublisher))
	require.NoError(t, err)
	err = engine.Start(ctx)
	defer engine.Shutdown()
	require.NoError(t, err)

	tempDir := t.TempDir()
	ds, err := leveldb.NewDatastore(tempDir, nil)
	defer func() {
		err = ds.Close()
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	ip, err := reframelistener.New(ctx, engine, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	cids := make([]cid.Cid, cidsNumber)
	for i := 0; i < len(cids); i++ {
		cids[i] = newCid(fmt.Sprintf("test%d", i))
	}

	client, server := createClientAndServer(t, ip, newProvider(t, pID), priv)
	defer server.Close()

	start := time.Now()
	provideMany(t, client, ctx, cids)

	require.True(t, time.Since(start) < timeExpectation)

}
