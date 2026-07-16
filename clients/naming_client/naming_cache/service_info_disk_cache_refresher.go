/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package naming_cache

import (
	"sync"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v3/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
)

const (
	defaultDiskCacheFlushInterval   = 100 * time.Millisecond
	defaultDiskCacheShutdownTimeout = 3 * time.Second
)

type diskCacheWriter func(service *model.Service, cacheKey, cacheDir string) bool

type serviceInfoDiskCacheRefreshEvent struct {
	service  *model.Service
	cacheKey string
	cacheDir string
}

type serviceInfoDiskCacheRefresher struct {
	mux             sync.Mutex
	pendingEvents   map[string]*serviceInfoDiskCacheRefreshEvent
	writer          diskCacheWriter
	ticker          *time.Ticker
	stopCh          chan struct{}
	doneCh          chan struct{}
	shutdownTimeout time.Duration
	closed          bool
	closeOnce       sync.Once
}

func newServiceInfoDiskCacheRefresher(flushInterval time.Duration, writer diskCacheWriter) *serviceInfoDiskCacheRefresher {
	refresher := &serviceInfoDiskCacheRefresher{
		pendingEvents:   make(map[string]*serviceInfoDiskCacheRefreshEvent),
		writer:          writer,
		ticker:          time.NewTicker(flushInterval),
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
		shutdownTimeout: defaultDiskCacheShutdownTimeout,
	}
	go refresher.run()
	return refresher
}

func (r *serviceInfoDiskCacheRefresher) publish(event *serviceInfoDiskCacheRefreshEvent) {
	r.mux.Lock()
	defer r.mux.Unlock()
	if r.closed {
		return
	}
	r.pendingEvents[event.cacheKey] = event
}

func (r *serviceInfoDiskCacheRefresher) pendingEventSize() int {
	r.mux.Lock()
	defer r.mux.Unlock()
	return len(r.pendingEvents)
}

func (r *serviceInfoDiskCacheRefresher) close() {
	r.closeOnce.Do(func() {
		r.mux.Lock()
		r.closed = true
		r.mux.Unlock()
		close(r.stopCh)
		select {
		case <-r.doneCh:
		case <-time.After(r.shutdownTimeout):
			logger.Warnf("timeout while waiting service info disk cache refresher to close, pending event size:%d", r.pendingEventSize())
		}
	})
}

func (r *serviceInfoDiskCacheRefresher) run() {
	defer close(r.doneCh)
	defer r.ticker.Stop()
	for {
		select {
		case <-r.ticker.C:
			r.safeFlushPendingEvents()
		case <-r.stopCh:
			r.safeFlushPendingEvents()
			return
		}
	}
}

func (r *serviceInfoDiskCacheRefresher) safeFlushPendingEvents() {
	defer func() {
		if err := recover(); err != nil {
			logger.Errorf("failed to flush service info disk cache refresh event, err:%v", err)
		}
	}()
	r.flushPendingEvents()
}

func (r *serviceInfoDiskCacheRefresher) flushPendingEvents() {
	r.mux.Lock()
	events := make([]*serviceInfoDiskCacheRefreshEvent, 0, len(r.pendingEvents))
	for _, event := range r.pendingEvents {
		events = append(events, event)
	}
	r.mux.Unlock()

	for _, event := range events {
		if r.writer(event.service, event.cacheKey, event.cacheDir) {
			r.mux.Lock()
			if r.pendingEvents[event.cacheKey] == event {
				delete(r.pendingEvents, event.cacheKey)
			}
			r.mux.Unlock()
		}
	}
}
