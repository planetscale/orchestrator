/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logic

import (
	"context"
	"errors"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/openark/orchestrator/external/golib/log"
	"github.com/openark/orchestrator/external/golib/sqlutils"
	"github.com/openark/orchestrator/go/config"
	"github.com/openark/orchestrator/go/db"
	"github.com/openark/orchestrator/go/inst"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	ts *topo.Server
)

// OpenVitessTopo opens the vitess topo if enables and returns a ticker
// channel for polling.
func OpenVitessTopo() <-chan time.Time {
	if !config.Config.Vitess {
		return nil
	}
	// TODO(sougou): If there's a shutdown signal, we have to close the topo.
	ts = topo.Open()
	// Clear existing cache and perform a new refresh.
	if _, err := db.ExecOrchestrator("delete from vitess_tablet"); err != nil {
		log.Errore(err)
	}
	if IsLeaderOrActive() {
		RefreshVitessTopo()
	}
	// TODO(sougou): parameterize poll interval.
	return time.Tick(15 * time.Second)
}

// RefreshVitessTopo refreshes the vitess topology.
func RefreshVitessTopo() {
	// Safety check
	if !config.Config.Vitess {
		return
	}
	if !IsLeaderOrActive() {
		return
	}

	latestInstances := make(map[inst.InstanceKey]bool)
	tablets, err := topotools.GetAllTabletsAcrossCells(context.TODO(), ts)
	if err != nil {
		log.Errorf("Error fetching topo info: %v", err)
		return
	}

	// Discover new tablets.
	// TODO(sougou): enhance this to work with multi-schema,
	// where each instanceKey can have multiple tablets.
	for _, tabletInfo := range tablets {
		tablet := tabletInfo.Tablet
		if tablet.MysqlHostname == "" {
			continue
		}
		instanceKey := inst.InstanceKey{
			Hostname: tablet.MysqlHostname,
			Port:     int(tablet.MysqlPort),
		}
		latestInstances[instanceKey] = true
		old, err := inst.ReadTablet(instanceKey)
		if err != nil {
			log.Errore(err)
			continue
		}
		if proto.Equal(tablet, old) {
			continue
		}
		if err := inst.SaveTablet(instanceKey, tablet); err != nil {
			log.Errore(err)
			continue
		}
		discoveryQueue.Push(instanceKey)
		log.Infof("Discovered: %v", tablet)
	}

	// Forget tablets that were removed.
	toForget := make(map[inst.InstanceKey]*topodatapb.Tablet)
	query := "select hostname, port, info from vitess_tablet"
	err = db.QueryOrchestrator(query, nil, func(row sqlutils.RowMap) error {
		curKey := inst.InstanceKey{
			Hostname: row.GetString("hostname"),
			Port:     row.GetInt("port"),
		}
		if !latestInstances[curKey] {
			tablet := &topodatapb.Tablet{}
			if err := proto.UnmarshalText(row.GetString("info"), tablet); err != nil {
				log.Errore(err)
				return nil
			}
			toForget[curKey] = tablet
		}
		return nil
	})
	if err != nil {
		log.Errore(err)
	}
	for instanceKey, tablet := range toForget {
		log.Infof("Forgotten: %v", tablet)
		if err := inst.ForgetInstance(&instanceKey); err != nil {
			log.Errore(err)
		}
		_, err := db.ExecOrchestrator(`
					delete
						from vitess_tablet
					where
						hostname=? and port=?`,
			instanceKey.Hostname,
			instanceKey.Port,
		)
		if err != nil {
			log.Errore(err)
		}
	}
}

// LockShard locks the keyspace-shard preventing others from performing conflicting actions.
func LockShard(instanceKey inst.InstanceKey) (func(*error), error) {
	if instanceKey.Hostname == "" {
		return nil, errors.New("Can't lock shard: instance is unspecified")
	}

	tablet, err := inst.ReadTablet(instanceKey)
	if err != nil {
		return nil, err
	}
	_, unlock, err := ts.LockShard(context.TODO(), tablet.Keyspace, tablet.Shard, "Orc Recovery")
	return unlock, err
}

// TabletSetMaster designates the tablet that owns an instance as the master.
func TabletSetMaster(instanceKey inst.InstanceKey) error {
	if instanceKey.Hostname == "" {
		return errors.New("Can't set tablet to master: instance is unspecified")
	}
	tablet, err := inst.ReadTablet(instanceKey)
	if err != nil {
		return err
	}
	tmc := tmclient.NewTabletManagerClient()
	if err := tmc.ChangeType(context.TODO(), tablet, topodatapb.TabletType_MASTER); err != nil {
		return err
	}
	// Proactively change the tablet type locally so we don't spam this until we get the refresh.
	tablet.Type = topodatapb.TabletType_MASTER
	if err := inst.SaveTablet(instanceKey, tablet); err != nil {
		log.Errore(err)
	}
	return nil
}

// TabletDemoteMaster requests the master tablet to stop accepting transactions.
func TabletDemoteMaster(instanceKey inst.InstanceKey) error {
	if instanceKey.Hostname == "" {
		return errors.New("Can't demote master: instance is unspecified")
	}
	tablet, err := inst.ReadTablet(instanceKey)
	if err != nil {
		return err
	}
	tmc := tmclient.NewTabletManagerClient()
	_, err = tmc.DemoteMaster(context.TODO(), tablet)
	return err
}
