package xproviders_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/filecoin-project/index-provider/engine"
	ep "github.com/filecoin-project/index-provider/engine/xproviders"
	"github.com/filecoin-project/index-provider/testutil"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestPublish(t *testing.T) {
	ctx := testutil.ContextWithTimeout(t)
	contextID := []byte("test-context")
	rng := rand.New(rand.NewSource(time.Now().Unix()))
	addrs := util.StringToMultiaddrs(t, []string{"/ip4/0.0.0.0/tcp/3090", "/ip4/0.0.0.0/tcp/3091"})
	metadata := make([]byte, 0, 10)
	rng.Read(metadata)
	eps := make([]ep.Info, 2)
	epIds := make([]peer.ID, len(eps))

	eng, err := engine.New()
	require.NoError(t, err)
	err = eng.Start(ctx)
	require.NoError(t, err)
	defer eng.Shutdown()

	priv, _, providerID := testutil.GenerateKeysAndIdentity(t)

	for i := 0; i < len(eps); i++ {
		epID, ep := randomExtendedProvider(t)
		eps[i] = ep
		epIds[i] = epID
	}

	override := true
	adv, err := ep.NewAdBuilder(providerID, priv, addrs).
		WithExtendedProviders(eps...).
		WithOverride(true).
		WithContextID(contextID).
		WithMetadata(metadata).
		BuildAndSign()
	require.NoError(t, err)
	advPeerID, err := adv.VerifySignature()
	require.NoError(t, err)

	// verify that we can publish successfully
	c, err := eng.Publish(ctx, *adv)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, c)

	require.Equal(t, providerID, advPeerID)
	require.Equal(t, testutil.MultiAddsToString(addrs), adv.Addresses)
	require.Equal(t, contextID, adv.ContextID)
	require.Equal(t, schema.NoEntries, adv.Entries)
	require.Equal(t, false, adv.IsRm)
	require.Equal(t, metadata, adv.Metadata)
	require.Equal(t, providerID.String(), adv.Provider)
	require.Equal(t, override, adv.ExtendedProvider.Override)
	require.Equal(t, 3, len(adv.ExtendedProvider.Providers))

	ep1 := eps[0]
	ep2 := eps[1]
	for _, p := range adv.ExtendedProvider.Providers {
		switch p.ID {
		case ep1.ID:
			require.Equal(t, ep1.Addrs, p.Addresses)
			require.Equal(t, ep1.Metadata, p.Metadata)
		case ep2.ID:
			require.Equal(t, ep2.Addrs, p.Addresses)
			require.Equal(t, ep2.Metadata, p.Metadata)
		case providerID.String():
			require.Equal(t, testutil.MultiAddsToString(addrs), p.Addresses)
			require.Equal(t, metadata, p.Metadata)
		default:
			panic("unknown provider")
		}
	}

}

func TestPublishFailsIfOverrideIsTrueWithNoContextId(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().Unix()))
	addrs := util.StringToMultiaddrs(t, []string{"/ip4/0.0.0.0/tcp/3090", "/ip4/0.0.0.0/tcp/3091"})
	metadata := make([]byte, 0, 10)
	rng.Read(metadata)
	eps := make([]ep.Info, 2)
	epIds := make([]peer.ID, len(eps))
	for i := 0; i < len(eps); i++ {
		epID, ep := randomExtendedProvider(t)
		eps[i] = ep
		epIds[i] = epID
	}
	priv, _, providerID := testutil.GenerateKeysAndIdentity(t)

	_, err := ep.NewAdBuilder(providerID, priv, addrs).
		WithExtendedProviders(eps...).
		WithOverride(true).
		WithMetadata(metadata).
		BuildAndSign()

	require.Error(t, err, "override is true for empty context")
}

func randomExtendedProvider(t *testing.T) (peer.ID, ep.Info) {
	rng := rand.New(rand.NewSource(time.Now().Unix()))
	priv, _, providerID := testutil.GenerateKeysAndIdentity(t)
	metadata := make([]byte, 0, 20)
	_, err := rng.Read(metadata)
	require.NoError(t, err)
	addrs := make([]multiaddr.Multiaddr, 0, 2)
	for i := 0; i < len(addrs); i++ {
		s := fmt.Sprintf("/ip4/%d.%d.%d.%d/tcp/%d", rng.Int()%255, rng.Int()%255, rng.Int()%255, rng.Int()%255, rng.Int()%10000+1024)
		ma, err := multiaddr.NewMultiaddr(s)
		require.NoError(t, err)
		addrs[i] = ma
	}

	return providerID, ep.NewInfo(providerID, priv, metadata, addrs)
}
