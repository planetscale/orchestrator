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
)

func discoverVitessTopo() {
	if !config.Config.Vitess {
		return
	}
	// TODO(sougou): If there's a shutdown signal, we have to close the topo.
	ts := topo.Open()

	// Forever loop
	// TODO(sougou): parameterize poll interval.
	vttopoTick := time.Tick(15 * time.Second)
	log.Infof("Starting vitess discovery loop, every %v", 15*time.Second)
	for range vttopoTick {
		latestInstances := make(map[inst.InstanceKey]bool)
		tablets, err := topotools.GetAllTabletsAcrossCells(context.TODO(), ts)
		if err != nil {
			log.Errorf("Error fetching topo info: %v", err)
			continue
		}

		// Discover new tablets.
		for _, tabletInfo := range tablets {
			tablet := tabletInfo.Tablet
			instanceKey := &inst.InstanceKey{
				Hostname: tablet.GetMysqlHostname(),
				Port:     int(tablet.GetMysqlPort()),
			}
			latestInstances[*instanceKey] = true
			old := inst.ReadTablet(instanceKey)
			if proto.Equal(tablet, old) {
				continue
			}
			_, err := db.ExecOrchestrator(`
					replace
						into vitess_tablet (
							hostname, port, info
						) values (
							?, ?, ?
						)
					`,
				instanceKey.Hostname,
				instanceKey.Port,
				proto.CompactTextString(tablet),
			)
			if err != nil {
				log.Errore(err)
				continue
			}
			_, err = inst.ReadTopologyInstance(instanceKey)
			if err != nil {
				log.Errorf("Error reading instance info: %v", err)
				continue
			}
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
		if err != nil {
			log.Errore(err)
		}
	}
}
