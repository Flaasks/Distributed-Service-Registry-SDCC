package storage

import (
	"sort"
	"strings"
	"sync"

	apiv1 "github.com/Flaasks/distributed-service-registry/pkg/api"
)

type ServiceStore struct {
	mu      sync.RWMutex
	records map[string]*apiv1.ServiceRecord
}

func NewServiceStore() *ServiceStore {
	return &ServiceStore{records: make(map[string]*apiv1.ServiceRecord)}
}

func (s *ServiceStore) Upsert(record *apiv1.ServiceRecord) *apiv1.ServiceRecord {
	if record == nil {
		return nil
	}

	key := recordKey(record.GetServiceName(), record.GetServiceId())
	copyRecord := cloneRecord(record)

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.records[key]
	if !exists {
		if copyRecord.LogicalVersion == 0 {
			copyRecord.LogicalVersion = 1
		}
		s.records[key] = copyRecord
		return cloneRecord(copyRecord)
	}

	if copyRecord.LogicalVersion <= existing.LogicalVersion {
		copyRecord.LogicalVersion = existing.LogicalVersion + 1
	}
	s.records[key] = copyRecord
	return cloneRecord(copyRecord)
}

func (s *ServiceStore) Remove(serviceName, serviceID string) bool {
	key := recordKey(serviceName, serviceID)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.records[key]; !exists {
		return false
	}
	delete(s.records, key)
	return true
}

func (s *ServiceStore) UpdateHeartbeat(serviceName, serviceID string, status apiv1.HealthStatus, heartbeatUnix int64, updatedAtUnix int64) (*apiv1.ServiceRecord, bool) {
	key := recordKey(serviceName, serviceID)

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.records[key]
	if !exists {
		return nil, false
	}

	existing.HealthStatus = status
	existing.LastHeartbeatUnix = heartbeatUnix
	existing.UpdatedAtUnix = updatedAtUnix
	existing.LogicalVersion++

	return cloneRecord(existing), true
}

func (s *ServiceStore) Get(serviceName, serviceID string) []*apiv1.ServiceRecord {
	normalizedName := strings.TrimSpace(serviceName)
	normalizedID := strings.TrimSpace(serviceID)

	s.mu.RLock()
	defer s.mu.RUnlock()

	if normalizedID != "" {
		record, exists := s.records[recordKey(normalizedName, normalizedID)]
		if !exists {
			return nil
		}
		return []*apiv1.ServiceRecord{cloneRecord(record)}
	}

	matches := make([]*apiv1.ServiceRecord, 0)
	for _, record := range s.records {
		if record.GetServiceName() == normalizedName {
			matches = append(matches, cloneRecord(record))
		}
	}
	sortRecords(matches)
	return matches
}

func (s *ServiceStore) List() []*apiv1.ServiceRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*apiv1.ServiceRecord, 0, len(s.records))
	for _, record := range s.records {
		out = append(out, cloneRecord(record))
	}
	sortRecords(out)
	return out
}

func (s *ServiceStore) ListSince(sinceUnix int64) []*apiv1.ServiceRecord {
	if sinceUnix <= 0 {
		return s.List()
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*apiv1.ServiceRecord, 0, len(s.records))
	for _, record := range s.records {
		if record.GetUpdatedAtUnix() <= sinceUnix {
			continue
		}
		out = append(out, cloneRecord(record))
	}
	sortRecords(out)
	return out
}

func (s *ServiceStore) MergeRemote(records []*apiv1.ServiceRecord) int {
	if len(records) == 0 {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	updated := 0
	for _, remote := range records {
		if remote == nil {
			continue
		}

		serviceName := strings.TrimSpace(remote.GetServiceName())
		serviceID := strings.TrimSpace(remote.GetServiceId())
		if serviceName == "" || serviceID == "" {
			continue
		}

		incoming := cloneRecord(remote)
		incoming.ServiceName = serviceName
		incoming.ServiceId = serviceID
		incoming.Endpoint = strings.TrimSpace(incoming.GetEndpoint())
		incoming.Version = strings.TrimSpace(incoming.GetVersion())
		incoming.OwnerNodeId = strings.TrimSpace(incoming.GetOwnerNodeId())
		if incoming.LogicalVersion == 0 {
			incoming.LogicalVersion = 1
		}

		key := recordKey(serviceName, serviceID)
		current, exists := s.records[key]
		if !exists {
			s.records[key] = incoming
			updated++
			continue
		}

		if shouldReplaceRecord(current, incoming) {
			s.records[key] = incoming
			updated++
		}
	}

	return updated
}

func (s *ServiceStore) MarkStale(nowUnix int64, heartbeatTTLSeconds int64) int {
	if heartbeatTTLSeconds <= 0 {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	updated := 0
	for _, record := range s.records {
		if nowUnix-record.GetLastHeartbeatUnix() <= heartbeatTTLSeconds {
			continue
		}
		if record.GetHealthStatus() == apiv1.HealthStatus_HEALTH_STATUS_NOT_SERVING {
			continue
		}
		record.HealthStatus = apiv1.HealthStatus_HEALTH_STATUS_NOT_SERVING
		record.UpdatedAtUnix = nowUnix
		record.LogicalVersion++
		updated++
	}
	return updated
}

func recordKey(serviceName, serviceID string) string {
	return strings.TrimSpace(serviceName) + "|" + strings.TrimSpace(serviceID)
}

func cloneRecord(record *apiv1.ServiceRecord) *apiv1.ServiceRecord {
	if record == nil {
		return nil
	}
	return &apiv1.ServiceRecord{
		ServiceName:       record.GetServiceName(),
		ServiceId:         record.GetServiceId(),
		Endpoint:          record.GetEndpoint(),
		Version:           record.GetVersion(),
		HealthStatus:      record.GetHealthStatus(),
		LastHeartbeatUnix: record.GetLastHeartbeatUnix(),
		UpdatedAtUnix:     record.GetUpdatedAtUnix(),
		OwnerNodeId:       record.GetOwnerNodeId(),
		LogicalVersion:    record.GetLogicalVersion(),
	}
}

func sortRecords(records []*apiv1.ServiceRecord) {
	sort.Slice(records, func(i, j int) bool {
		left := records[i]
		right := records[j]
		if left.GetServiceName() == right.GetServiceName() {
			return left.GetServiceId() < right.GetServiceId()
		}
		return left.GetServiceName() < right.GetServiceName()
	})
}

func shouldReplaceRecord(local, incoming *apiv1.ServiceRecord) bool {
	if incoming.GetLogicalVersion() != local.GetLogicalVersion() {
		return incoming.GetLogicalVersion() > local.GetLogicalVersion()
	}
	if incoming.GetUpdatedAtUnix() != local.GetUpdatedAtUnix() {
		return incoming.GetUpdatedAtUnix() > local.GetUpdatedAtUnix()
	}
	if incoming.GetLastHeartbeatUnix() != local.GetLastHeartbeatUnix() {
		return incoming.GetLastHeartbeatUnix() > local.GetLastHeartbeatUnix()
	}
	if incoming.GetHealthStatus() != local.GetHealthStatus() {
		return incoming.GetHealthStatus() > local.GetHealthStatus()
	}
	if incoming.GetOwnerNodeId() != local.GetOwnerNodeId() {
		return incoming.GetOwnerNodeId() > local.GetOwnerNodeId()
	}
	if incoming.GetEndpoint() != local.GetEndpoint() {
		return incoming.GetEndpoint() > local.GetEndpoint()
	}
	if incoming.GetVersion() != local.GetVersion() {
		return incoming.GetVersion() > local.GetVersion()
	}
	return false
}
