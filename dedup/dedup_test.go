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

	seen, err := store.Check(ctx, key)
	require.Nil(t, err)
	require.False(t, seen)

	require.Nil(t, store.Mark(ctx, key, ttl))

	seen, err = store.Check(ctx, key)
	require.Nil(t, err)
	require.True(t, seen)

	seen, err = store.Check(ctx, "different-key")
	require.Nil(t, err)
	require.False(t, seen)
}

func TestMemoryStoreExpiry(t *testing.T) {
	store := dedup.NewMemoryStore()
	defer store.Close()

	ctx := context.Background()
	key := "expiring-key"
	ttl := 100 * time.Millisecond

	require.Nil(t, store.Mark(ctx, key, ttl))

	seen, err := store.Check(ctx, key)
	require.Nil(t, err)
	require.True(t, seen)

	time.Sleep(150 * time.Millisecond)

	seen, err = store.Check(ctx, key)
	require.Nil(t, err)
	require.False(t, seen)
}
