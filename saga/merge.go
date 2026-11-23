package saga

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/victorcondackdev/saga-go/store"
)

// mergeBranches folds the outputs of a parallel group into base. Each
// branch started from base; a top-level field it changed (added, modified
// or removed) is applied to the result. Two branches changing the same
// field to different values is a conflict.
func mergeBranches(base json.RawMessage, node []int, steps []store.StepRecord) (json.RawMessage, error) {
	var b map[string]json.RawMessage
	if err := json.Unmarshal(base, &b); err != nil || b == nil {
		return nil, fmt.Errorf("parallel group: state must be a JSON object to merge branch results")
	}
	result := make(map[string]json.RawMessage, len(b))
	for k, v := range b {
		result[k] = v
	}
	changedBy := map[string]string{}
	for _, i := range node {
		rec := steps[i]
		if rec.Status != store.StepDone || rec.Output == nil {
			continue
		}
		var o map[string]json.RawMessage
		if err := json.Unmarshal(rec.Output, &o); err != nil || o == nil {
			return nil, fmt.Errorf("parallel group: branch %q produced a non-object state", rec.Name)
		}
		keys := make([]string, 0, len(o)+len(b))
		for k := range o {
			keys = append(keys, k)
		}
		for k := range b {
			if _, ok := o[k]; !ok {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			ov, inO := o[k]
			bv, inB := b[k]
			if inO == inB && (!inO || jsonEqual(ov, bv)) {
				continue
			}
			if prev, ok := changedBy[k]; ok {
				rv, inR := result[k]
				if inR != inO || (inO && !jsonEqual(rv, ov)) {
					return nil, fmt.Errorf("parallel group: branches %q and %q both changed field %q", prev, rec.Name, k)
				}
				continue
			}
			changedBy[k] = rec.Name
			if inO {
				result[k] = ov
			} else {
				delete(result, k)
			}
		}
	}
	return json.Marshal(result)
}

func jsonEqual(a, b json.RawMessage) bool {
	if bytes.Equal(a, b) {
		return true
	}
	var ca, cb bytes.Buffer
	if json.Compact(&ca, a) != nil || json.Compact(&cb, b) != nil {
		return false
	}
	return bytes.Equal(ca.Bytes(), cb.Bytes())
}
