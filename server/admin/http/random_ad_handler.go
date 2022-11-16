package adminserver

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

func (s *Server) randomAdHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	var req RandomAdReq
	if _, err := req.ReadFrom(r.Body); err != nil {
		msg := fmt.Sprintf("failed to unmarshal request. %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	if req.XpCount > 0 {
		s.randomXpAdHandler(ctx, req, w)
	} else {
		s.randomNoXpAdHandler(ctx, req, w)
	}
}

func (s *Server) randomXpAdHandler(ctx context.Context, req RandomAdReq, w http.ResponseWriter) {
	rng := rand.New(rand.NewSource(time.Now().Unix()))
	latestAdCid, _, err := s.e.GetLatestAdv(ctx)
	if err != nil {
		msg := fmt.Sprintf("error getting the latest advertisement. %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	ad := schema.Advertisement{
		Provider:  s.h.ID().String(),
		Addresses: multiaddrsToStrings(s.h.Addrs()),
		Entries:   schema.NoEntries,
		Metadata:  []byte(s.h.ID() + "-metadata"),
		ExtendedProvider: &schema.ExtendedProvider{
			Override:  req.Override,
			Providers: make([]schema.Provider, req.XpCount+1),
		},
	}

	if latestAdCid != cid.Undef {
		ad.PreviousID = cidlink.Link{Cid: latestAdCid}
	}

	if len(req.ContextID) > 0 {
		ad.ContextID = []byte(req.ContextID)
	}

	keys := map[string]crypto.PrivKey{}
	for i := 0; i < req.XpCount; i++ {
		peer, priv, _, err := randomIdentity()
		if err != nil {
			msg := fmt.Sprintf("error generating random identity. %v", err)
			log.Errorw(msg, "err", err)
			http.Error(w, msg, http.StatusInternalServerError)
			return
		}
		ad.ExtendedProvider.Providers[i] = schema.Provider{
			ID:        peer.String(),
			Addresses: randomAddrs(rng.Int()%5 + 1),
			Metadata:  []byte(peer.String() + "-metadata"),
		}
		keys[peer.String()] = priv
	}
	ad.ExtendedProvider.Providers[req.XpCount] = schema.Provider{
		ID:        s.h.ID().String(),
		Addresses: multiaddrsToStrings(s.h.Addrs()),
		Metadata:  []byte(s.h.ID().String() + "-metadata"),
	}

	err = ad.SignWithExtendedProviders(s.p, func(s string) (crypto.PrivKey, error) {
		priv, ok := keys[s]
		if !ok {
			return nil, errors.New("unknown provider")
		}
		return priv, nil
	})
	if err != nil {
		msg := fmt.Sprintf("error signign the advertisement. %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	c, err := s.e.Publish(ctx, ad)
	if err != nil {
		msg := fmt.Sprintf("error announcing the advertisement. %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	resp := &RandomAdRes{AdvId: c}
	respond(w, http.StatusOK, resp)
}

func (s *Server) randomNoXpAdHandler(ctx context.Context, req RandomAdReq, w http.ResponseWriter) {
	rng := rand.New(rand.NewSource(time.Now().Unix()))
	cids, err := randomCids(rng, req.MhsCount)
	if err != nil {
		msg := fmt.Sprintf("error generating multihashes. %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	mhs := cidsToMultihashes(cids)
	mhsByCtxId.Store(req.ContextID, mhs)

	c, err := s.e.NotifyPut(ctx, &peer.AddrInfo{ID: s.h.ID(), Addrs: s.h.Addrs()}, []byte(req.ContextID), metadata.Default.New(metadata.Bitswap{}))
	if err != nil {
		msg := fmt.Sprintf("error publishing advertisement. %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	stringMhs := make([]string, len(mhs))
	for i, m := range mhs {
		stringMhs[i] = m.String()
	}

	stringCids := make([]string, len(cids))
	for i, c := range cids {
		stringCids[i] = c.String()
	}

	resp := &RandomAdRes{AdvId: c, Mhs: stringMhs, Cids: stringCids}
	respond(w, http.StatusOK, resp)
}

func multiaddrsToStrings(addrs []multiaddr.Multiaddr) []string {
	var stringAddrs []string
	for _, addr := range addrs {
		stringAddrs = append(stringAddrs, addr.String())
	}
	return stringAddrs
}

func randomIdentity() (peer.ID, crypto.PrivKey, crypto.PubKey, error) {

	privKey, pubKey, err := randTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		return "", nil, nil, err
	}

	providerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return "", nil, nil, err
	}
	return providerID, privKey, pubKey, nil
}

var globalSeed int64

func randTestKeyPair(typ, bits int) (crypto.PrivKey, crypto.PubKey, error) {
	// workaround for low time resolution
	seed := atomic.AddInt64(&globalSeed, 1)
	return seededTestKeyPair(typ, bits, seed)
}

func seededTestKeyPair(typ, bits int, seed int64) (crypto.PrivKey, crypto.PubKey, error) {
	r := rand.New(rand.NewSource(seed))
	return crypto.GenerateKeyPairWithReader(typ, bits, r)
}

func randomAddrs(n int) []string {
	rng := rand.New(rand.NewSource(time.Now().Unix()))
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = fmt.Sprintf("/ip4/%d.%d.%d.%d/tcp/%d", rng.Int()%255, rng.Int()%255, rng.Int()%255, rng.Int()%255, rng.Int()%10751)
	}
	return addrs
}

func randomCids(rng *rand.Rand, n int) ([]cid.Cid, error) {
	prefix := schema.Linkproto.Prefix

	mhashes := make([]cid.Cid, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n)
		rng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			return nil, err
		}
		mhashes[i] = c
	}
	return mhashes, nil
}

func cidsToMultihashes(cids []cid.Cid) []multihash.Multihash {
	mhs := make([]multihash.Multihash, len(cids))
	for i, c := range cids {
		mhs[i] = c.Hash()
	}
	return mhs
}
