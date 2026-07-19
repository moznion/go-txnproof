package crosscheck

import (
	"fmt"
	"strings"
)

// BeginMarker returns a harmless SELECT the test should execute right before
// the scenario; its recognizable literal delimits the scenario's start in
// the server log. Single quotes in the label are escaped.
func BeginMarker(label string) string {
	return "SELECT " + markerLiteral("begin", label)
}

// EndMarker returns the counterpart of BeginMarker, to execute right after
// the scenario.
func EndMarker(label string) string {
	return "SELECT " + markerLiteral("end", label)
}

func markerLiteral(kind, label string) string {
	return "'txnproof:" + kind + ":" + strings.ReplaceAll(label, "'", "''") + "'"
}

// Scenario slices out the statements strictly between the first BeginMarker
// and the following EndMarker for the label, excluding the markers
// themselves. It fails when either marker is missing — a sign the log
// content is not from this test run, or that statement logging is not
// configured as the adapter documents.
func Scenario(stmts []Statement, label string) ([]Statement, error) {
	beginLit := markerLiteral("begin", label)
	endLit := markerLiteral("end", label)
	begin := -1
	for i, st := range stmts {
		if strings.Contains(st.SQL, beginLit) {
			begin = i
			break
		}
	}
	if begin < 0 {
		return nil, fmt.Errorf("crosscheck: begin marker %s for scenario %q not found in the log; execute BeginMarker(%q) before the scenario and make sure statement logging is enabled", beginLit, label, label)
	}
	for i := begin + 1; i < len(stmts); i++ {
		if strings.Contains(stmts[i].SQL, endLit) {
			return stmts[begin+1 : i], nil
		}
	}
	return nil, fmt.Errorf("crosscheck: end marker %s for scenario %q not found in the log after its begin marker; execute EndMarker(%q) after the scenario", endLit, label, label)
}
