package dedup_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zutrixpog/gobus/dedup"
)

func TestMemoryStore(t *testing.T) {
	store := dedup.NewMemoryStore()
	defer store.Close()

	ctx := context.Background()
	key := "test-key-1"
	ttl := time.Minute

	ok, err := store.CheckAndMark(ctx, key, ttl)
	require.Nil(t, err)
	require.True(t, ok)

	ok, err = store.CheckAndMark(ctx, key, ttl)
	require.Nil(t, err)
	require.False(t, ok)

	ok, err = store.CheckAndMark(ctx, "different-key", ttl)
	require.Nil(t, err)
	require.True(t, ok)
}

func TestMemoryStoreExpiry(t *testing.T) {
	store := dedup.NewMemoryStore()
	defer store.Close()

	ctx := context.Background()
	key := "expiring-key"
	ttl := 100 * time.Millisecond

	ok, err := store.CheckAndMark(ctx, key, ttl)
	require.Nil(t, err)
	require.True(t, ok)

	ok, err = store.CheckAndMark(ctx, key, ttl)
	require.Nil(t, err)
	require.False(t, ok)

	time.Sleep(150 * time.Millisecond)

	ok, err = store.CheckAndMark(ctx, key, ttl)
	require.Nil(t, err)
	require.True(t, ok)
}
