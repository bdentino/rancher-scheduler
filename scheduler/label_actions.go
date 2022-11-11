package scheduler

import "strings"
import "github.com/rancher/log"

const (
	requireAnyLabel = "io.rancher.scheduler.require_any"
)

// LabelFilter define a filter based on label constraints. For example, require_any label constraints
type LabelFilter struct {
}

func (l LabelFilter) Filter(scheduler *Scheduler, resourceRequest []ResourceRequest, context Context, hosts []string) []string {
	constraints := getAllConstraints()
	qualifiedHosts := []string{}
	for _, host := range hosts {
		qualified := true
		for _, constraint := range constraints {
			if !constraint.Match(host, scheduler, context) {
				log.Infof("Host %s is NOT qualified for context %v", host, context)
				qualified = false
			}
		}
		if qualified {
			qualifiedHosts = append(qualifiedHosts, host)
		}
	}
	log.Infof("Hosts %s are qualified for context %+v", strings.Join(qualifiedHosts, ","), context)
	return qualifiedHosts
}

type Constraints interface {
	Match(string, *Scheduler, Context) bool
}

func getAllConstraints() []Constraints {
	RequireAny := RequireAnyLabelContraints{}
	return []Constraints{RequireAny}
}

type RequireAnyLabelContraints struct{}

func (c RequireAnyLabelContraints) Match(host string, s *Scheduler, context Context) bool {
	p, ok := s.hosts[host].pools["hostLabels"]
	if !ok {
		// log.Infof("Host %s is qualified because there is no host label pool", host)
		return true
	}
	val, ok := p.(*LabelPool).Labels[requireAnyLabel]
	if !ok || val == "" {
		// log.Infof("Host %s is qualified because there are no taint labels in the pool", host)
		return true
	}
	labelSet := parseLabel(val)
	containerLabels := getLabelFromContext(context)
	for key, value := range labelSet {
		for _, ls := range containerLabels {
			if value == "" {
				if _, ok := ls[key]; ok {
					log.Infof("Host %s is qualified because it has label %s", host, key)
					return true
				}
			} else {
				if ls[key] == value {
					log.Infof("Host %s is qualified because it has label %s=%s", host, key, value)
					return true
				}
			}
		}
	}
	log.Infof("Host %s is not qualified because it does not have any tolerations for %+v", host, labelSet)
	return false
}

func parseLabel(value string) map[string]string {
	value = strings.ToLower(value)
	parts := strings.Split(value, ",")
	result := map[string]string{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		p := strings.Split(part, "=")
		if len(p) == 2 {
			result[p[0]] = p[1]
		} else if len(p) == 1 {
			result[p[0]] = ""
		}
	}
	return result
}

func getLabelFromContext(context Context) []map[string]string {
	result := []map[string]string{}
	for _, con := range context {
		lowerMap := map[string]string{}
		for key, value := range con.Data.Fields.Labels {
			lowerMap[strings.ToLower(key)] = strings.ToLower(value)
		}
		result = append(result, lowerMap)
	}
	return result
}
