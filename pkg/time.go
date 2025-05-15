package pkg

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
)

// ParseFlexible converts strings such as
//
//	6.09.25, 15:00:00
//	06/09/2025 15:00:00
//	9/6/25, 3:00:00 PM
//
// to a time.Time.  It first tries a fast custom parser for those three
// layouts; if that fails it falls back to dateparse.ParseLocal, which
// understands a very large set of formats.
//
// The result is in the machine's local zone; switch to time.UTC if you
// prefer UTC.
func ParseFlexible(input string) (time.Time, error) {
	if t, err := parseFlexibleCustom(input); err == nil {
		return t.UTC(), nil
	}
	if t, err := dateparse.ParseLocal(strings.TrimSpace(strings.ReplaceAll(input, ",", ""))); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("flexdatetime: unrecognised datetime %q", input)
}

// ----------------------------------------------------------------------
// Everything below here is the same logic as before, just moved into an
// unexported helper.
// ----------------------------------------------------------------------

func parseFlexibleCustom(input string) (time.Time, error) {
	input = strings.TrimSpace(strings.ReplaceAll(input, ",", ""))
	parts := strings.SplitN(input, " ", 2)
	if len(parts) != 2 {
		return time.Time{}, errors.New("expected exactly one space separating date and time")
	}
	datePart, timePart := parts[0], parts[1]

	// --- time ---
	tm, err := parseTimePart(timePart)
	if err != nil {
		return time.Time{}, err
	}

	// --- date ---
	d, m, y, err := parseDatePart(datePart)
	if err != nil {
		return time.Time{}, err
	}
	if y < 100 {
		y += 2000
	}

	return time.Date(y, time.Month(m), d, tm.Hour(), tm.Minute(), tm.Second(), 0, time.UTC), nil
}

func parseTimePart(timePart string) (time.Time, error) {
	layouts := []string{"15:04:05", "3:04:05PM", "3:04:05 PM"}
	for _, l := range layouts {
		if t, err := time.Parse(l, timePart); err == nil {
			return t, nil
		}
	}
	return time.Time{}, errors.New("time component not recognised")
}

func parseDatePart(datePart string) (d, m, y int, err error) {
	switch {
	case strings.Contains(datePart, "."): // BG  d.m.yy or d.m.yyyy
		return atoi3(strings.Split(datePart, "."))
	case strings.Contains(datePart, "/"): // UK or US
		a, b, c, err := atoi3(strings.Split(datePart, "/"))
		if err != nil {
			return 0, 0, 0, err
		}
		switch {
		case len(strings.Split(datePart, "/")[2]) == 4, a > 12: // day first
			return a, b, c, nil
		default: // month first
			return b, a, c, nil
		}
	default:
		return 0, 0, 0, errors.New("unknown date separator")
	}
}

func atoi3(ss []string) (a, b, c int, err error) {
	if len(ss) != 3 {
		return 0, 0, 0, errors.New("date must have three components")
	}
	a, err = strconv.Atoi(strings.TrimLeft(ss[0], "0"))
	if err != nil {
		return
	}
	b, err = strconv.Atoi(strings.TrimLeft(ss[1], "0"))
	if err != nil {
		return
	}
	c, err = strconv.Atoi(strings.TrimLeft(ss[2], "0"))
	return
}
