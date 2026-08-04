package jsonmerge

import (
	"encoding/json"
	"reflect"
	"testing"
)

// equalJSON compares by semantic value, not bytes: Apply re-encodes merged
// objects, and Go sorts map keys when marshalling, so byte comparison would
// assert an ordering the merge path does not promise. (Byte fidelity IS
// promised on the replace path — see TestReplaceIsByteExact.)
func equalJSON(t *testing.T, got, want json.RawMessage) bool {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("result is not valid JSON: %v (%s)", err, got)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("expectation is not valid JSON: %v (%s)", err, want)
	}
	return reflect.DeepEqual(g, w)
}

// U12-U20: the merge rules, case by case.
func TestApply(t *testing.T) {
	cases := []struct {
		id      string
		name    string
		current string
		delta   string
		want    string
	}{
		{"U12", "merge adds a field", `{"a":1}`, `{"b":2}`, `{"a":1,"b":2}`},
		{"U13", "delta wins on conflict", `{"a":1}`, `{"a":9}`, `{"a":9}`},
		{"U14", "nested object is REPLACED, not deep-merged",
			`{"a":{"x":1}}`, `{"a":{"y":2}}`, `{"a":{"y":2}}`},
		{"U15", "current is an array so replace", `[1,2]`, `{"a":1}`, `{"a":1}`},
		{"U16", "delta is an array so replace", `{"a":1}`, `[1,2]`, `[1,2]`},
		{"U17", "delta is null so replace", `{"a":1}`, `null`, `null`},
		{"U18", "current is a string so replace", `"str"`, `{"a":1}`, `{"a":1}`},
		{"U19", "empty delta leaves the value untouched", `{"a":1}`, `{}`, `{"a":1}`},
		{"U20", "null field is SET, not deleted (ruling R1)",
			`{"a":1,"b":2}`, `{"a":null}`, `{"a":null,"b":2}`},

		{"", "current is a number so replace", `5`, `{"a":1}`, `{"a":1}`},
		{"", "current is null so replace", `null`, `{"a":1}`, `{"a":1}`},
		{"", "current is a bool so replace", `true`, `{"a":1}`, `{"a":1}`},
		{"", "delta is a number so replace", `{"a":1}`, `42`, `42`},
		{"", "delta is a string so replace", `{"a":1}`, `"hello"`, `"hello"`},
		{"", "both empty objects", `{}`, `{}`, `{}`},
		{"", "merge into an empty object", `{}`, `{"a":1}`, `{"a":1}`},
		{"", "multi-field merge", `{"a":1,"b":2,"c":3}`, `{"b":9,"d":4}`,
			`{"a":1,"b":9,"c":3,"d":4}`},
		{"", "nested value survives when untouched",
			`{"a":{"x":1},"b":2}`, `{"b":3}`, `{"a":{"x":1},"b":3}`},
		{"", "leading whitespace does not hide an object",
			"  {\"a\":1}", `{"b":2}`, `{"a":1,"b":2}`},
		{"", "unicode round-trips", `{"th":"ไทย"}`, `{"emoji":"🔑"}`,
			`{"th":"ไทย","emoji":"🔑"}`},
	}

	for _, c := range cases {
		name := c.name
		if c.id != "" {
			name = c.id + " " + name
		}
		t.Run(name, func(t *testing.T) {
			got := Apply(json.RawMessage(c.current), json.RawMessage(c.delta))
			if !equalJSON(t, got, json.RawMessage(c.want)) {
				t.Errorf("Apply(%s, %s) = %s, want %s", c.current, c.delta, got, c.want)
			}
		})
	}
}

// U14 again, on its own, because it is the single most-failed rule in the spec.
func TestShallowNotDeep(t *testing.T) {
	got := Apply(
		json.RawMessage(`{"user":{"name":"Ari","points":10}}`),
		json.RawMessage(`{"user":{"rank":"gold"}}`),
	)
	want := json.RawMessage(`{"user":{"rank":"gold"}}`)

	if !equalJSON(t, got, want) {
		t.Errorf("Apply merged nested objects: got %s, want %s", got, want)
	}
	if equalJSON(t, got, json.RawMessage(`{"user":{"name":"Ari","points":10,"rank":"gold"}}`)) {
		t.Error("nested objects were deep-merged; the spec says shallow")
	}
}

// The store publishes values and relies on them staying immutable. Apply must
// never write through either argument.
func TestApplyDoesNotMutateInputs(t *testing.T) {
	current := json.RawMessage(`{"a":1,"b":2}`)
	delta := json.RawMessage(`{"a":9}`)

	curCopy := append(json.RawMessage(nil), current...)
	delCopy := append(json.RawMessage(nil), delta...)

	_ = Apply(current, delta)

	if string(current) != string(curCopy) {
		t.Errorf("Apply mutated current: %s, was %s", current, curCopy)
	}
	if string(delta) != string(delCopy) {
		t.Errorf("Apply mutated delta: %s, was %s", delta, delCopy)
	}
}

// A published value must not share backing storage with anything the caller can
// still write to; otherwise a reader could observe bytes changing underneath it.
func TestApplyResultDoesNotAliasInputs(t *testing.T) {
	t.Run("replace path", func(t *testing.T) {
		delta := json.RawMessage(`[1,2,3]`)
		got := Apply(json.RawMessage(`{"a":1}`), delta)

		delta[1] = '9'
		if got[1] == '9' {
			t.Error("result aliases delta: mutating delta changed the stored value")
		}
	})

	t.Run("merge path", func(t *testing.T) {
		current := json.RawMessage(`{"a":1}`)
		got := Apply(current, json.RawMessage(`{"b":2}`))

		current[0] = 'X'
		if !json.Valid(got) {
			t.Error("result aliases current: mutating current corrupted the stored value")
		}
	})
}

// The replace path stores delta verbatim, so byte fidelity holds there — key
// order and whitespace survive. This is what raw-bytes storage buys.
func TestReplaceIsByteExact(t *testing.T) {
	delta := json.RawMessage(`{"b":1,"a":2}`)
	got := Apply(json.RawMessage(`[1,2]`), delta) // array current forces replace

	if string(got) != string(delta) {
		t.Errorf("replace altered the bytes: got %s, want %s", got, delta)
	}
}

// Every result must be valid JSON — the store never returns a value a client
// cannot parse.
func TestApplyAlwaysProducesValidJSON(t *testing.T) {
	inputs := []string{
		`{"a":1}`, `[1,2]`, `null`, `true`, `42`, `"s"`, `{}`, `[]`,
		`{"nested":{"deep":{"deeper":1}}}`,
	}
	for _, cur := range inputs {
		for _, del := range inputs {
			got := Apply(json.RawMessage(cur), json.RawMessage(del))
			if !json.Valid(got) {
				t.Errorf("Apply(%s, %s) produced invalid JSON: %s", cur, del, got)
			}
		}
	}
}

func BenchmarkApplyMerge(b *testing.B) {
	current := json.RawMessage(`{"name":"Ari","points":10,"rank":"gold"}`)
	delta := json.RawMessage(`{"points":20}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Apply(current, delta)
	}
}

func BenchmarkApplyReplace(b *testing.B) {
	current := json.RawMessage(`[1,2,3]`)
	delta := json.RawMessage(`{"name":"Ari"}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Apply(current, delta)
	}
}
