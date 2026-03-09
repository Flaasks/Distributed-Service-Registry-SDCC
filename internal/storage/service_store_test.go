package storage

import (
	"testing"

	apiv1 "github.com/Flaasks/distributed-service-registry/pkg/api"
)

func TestServiceStoreUpsertListAndVersioning(t *testing.T) {
	store := NewServiceStore()

	first := &apiv1.ServiceRecord{
		ServiceName:       "users",
		ServiceId:         "b",
		Endpoint:          "users-b:8080",
		Version:           "v1",
		HealthStatus:      apiv1.HealthStatus_HEALTH_STATUS_SERVING,
		LastHeartbeatUnix: 100,
		UpdatedAtUnix:     100,
	}
	storedFirst := store.Upsert(first)
	if storedFirst.GetLogicalVersion() != 1 {
		t.Fatalf("expected logical version 1, got %d", storedFirst.GetLogicalVersion())
	}

	second := &apiv1.ServiceRecord{
		ServiceName:       "users",
		ServiceId:         "a",
		Endpoint:          "users-a:8080",
		Version:           "v1",
		HealthStatus:      apiv1.HealthStatus_HEALTH_STATUS_SERVING,
		LastHeartbeatUnix: 100,
		UpdatedAtUnix:     100,
	}
	store.Upsert(second)

	updatedFirst := &apiv1.ServiceRecord{
		ServiceName:       "users",
		ServiceId:         "b",
		Endpoint:          "users-b:9090",
		Version:           "v2",
		HealthStatus:      apiv1.HealthStatus_HEALTH_STATUS_DEGRADED,
		LastHeartbeatUnix: 101,
		UpdatedAtUnix:     101,
	}
	storedUpdated := store.Upsert(updatedFirst)
	if storedUpdated.GetLogicalVersion() != 2 {
		t.Fatalf("expected logical version 2 after update, got %d", storedUpdated.GetLogicalVersion())
	}

	all := store.List()
	if len(all) != 2 {
		t.Fatalf("expected 2 records, got %d", len(all))
	}
	if all[0].GetServiceId() != "a" || all[1].GetServiceId() != "b" {
		t.Fatalf("expected deterministic ordering by service id, got %s then %s", all[0].GetServiceId(), all[1].GetServiceId())
	}
	if all[1].GetEndpoint() != "users-b:9090" {
		t.Fatalf("expected endpoint update to persist, got %s", all[1].GetEndpoint())
	}
}

func TestServiceStoreHeartbeatStaleAndRemove(t *testing.T) {
	store := NewServiceStore()
	store.Upsert(&apiv1.ServiceRecord{
		ServiceName:       "orders",
		ServiceId:         "1",
		Endpoint:          "orders-1:8080",
		Version:           "v1",
		HealthStatus:      apiv1.HealthStatus_HEALTH_STATUS_SERVING,
		LastHeartbeatUnix: 200,
		UpdatedAtUnix:     200,
	})

	updated, ok := store.UpdateHeartbeat("orders", "1", apiv1.HealthStatus_HEALTH_STATUS_DEGRADED, 205, 205)
	if !ok {
		t.Fatalf("expected heartbeat update to succeed")
	}
	if updated.GetHealthStatus() != apiv1.HealthStatus_HEALTH_STATUS_DEGRADED {
		t.Fatalf("expected health to be DEGRADED, got %s", updated.GetHealthStatus().String())
	}

	staleCount := store.MarkStale(220, 10)
	if staleCount != 1 {
		t.Fatalf("expected one stale update, got %d", staleCount)
	}
	postStale := store.Get("orders", "1")
	if len(postStale) != 1 {
		t.Fatalf("expected to find one record after stale update")
	}
	if postStale[0].GetHealthStatus() != apiv1.HealthStatus_HEALTH_STATUS_NOT_SERVING {
		t.Fatalf("expected health to become NOT_SERVING, got %s", postStale[0].GetHealthStatus().String())
	}

	if removed := store.Remove("orders", "1"); !removed {
		t.Fatalf("expected remove to return true")
	}
	if removedAgain := store.Remove("orders", "1"); removedAgain {
		t.Fatalf("expected second remove to return false")
	}
}
