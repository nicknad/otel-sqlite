package notify

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// Rule defines a single notification rule.
type Rule struct {
	Name             string
	MatchSeverity    model.Severity
	ResourceFilter   string
	BodyFilter       string
	AttributeFilters map[string]string
	Cooldown         time.Duration
	RateLimit        int
	RateWindow       time.Duration
	DedupWindow      time.Duration
	MaxRetries       int
	RetryBackoff     time.Duration
	Destination      string

	// compiled regexps (populated on creation)
	resourceRegex *regexp.Regexp
	bodyRegex     *regexp.Regexp
	attrRegexes   map[string]*regexp.Regexp
}

// compile compiles regex filters. Returns error if any filter is invalid.
func (r *Rule) compile() error {
	if r.ResourceFilter != "" {
		re, err := regexp.Compile(r.ResourceFilter)
		if err != nil {
			return fmt.Errorf("rule %q: invalid resource_filter: %w", r.Name, err)
		}
		r.resourceRegex = re
	}
	if r.BodyFilter != "" {
		re, err := regexp.Compile(r.BodyFilter)
		if err != nil {
			return fmt.Errorf("rule %q: invalid body_filter: %w", r.Name, err)
		}
		r.bodyRegex = re
	}
	r.attrRegexes = make(map[string]*regexp.Regexp, len(r.AttributeFilters))
	for key, pattern := range r.AttributeFilters {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("rule %q: invalid attribute filter %q: %w", r.Name, key, err)
		}
		r.attrRegexes[key] = re
	}
	return nil
}

// setDefaults fills zero-value fields with sensible defaults.
func (r *Rule) setDefaults() {
	if r.MaxRetries <= 0 {
		r.MaxRetries = 3
	}
	if r.RetryBackoff <= 0 {
		r.RetryBackoff = 30 * time.Second
	}
}

// RuleEngine evaluates events against a set of rules.
type RuleEngine interface {
	Evaluate(ctx context.Context, event *Event) (rule *Rule, key string, matched bool)
	Rules() []Rule
}

// DefaultRuleEngine is the default implementation of RuleEngine.
type DefaultRuleEngine struct {
	rules []Rule
}

// NewRuleEngine creates a new DefaultRuleEngine from a list of rules.
// Rules are evaluated in order; the first matching rule wins.
func NewRuleEngine(rules []Rule) (*DefaultRuleEngine, error) {
	for i := range rules {
		rules[i].setDefaults()
		if err := rules[i].compile(); err != nil {
			return nil, err
		}
	}
	log.Printf("notify rule engine: %d rules configured", len(rules))
	return &DefaultRuleEngine{rules: rules}, nil
}

// Evaluate checks all rules against the event. Returns the first matching rule.
func (e *DefaultRuleEngine) Evaluate(_ context.Context, event *Event) (*Rule, string, bool) {
	for i := range e.rules {
		rule := &e.rules[i]
		if e.matchRule(rule, event) {
			key := e.buildKey(rule, event)
			return rule, key, true
		}
	}
	return nil, "", false
}

// Rules returns the configured rules.
func (e *DefaultRuleEngine) Rules() []Rule {
	return e.rules
}

// matchRule checks if an event matches a rule.
func (e *DefaultRuleEngine) matchRule(rule *Rule, event *Event) bool {
	// Severity threshold.
	if event.Severity < rule.MatchSeverity {
		return false
	}

	// Resource filter (regex on resource ID).
	if rule.resourceRegex != nil && !rule.resourceRegex.MatchString(event.ResourceID) {
		return false
	}

	// Body filter.
	if rule.bodyRegex != nil && !rule.bodyRegex.MatchString(event.Body) {
		return false
	}

	// Attribute filters.
	for key, re := range rule.attrRegexes {
		if !e.matchAttribute(event.Attributes, key, re) {
			return false
		}
	}

	return true
}

// matchAttribute checks if any attribute with the given key matches the regex.
func (e *DefaultRuleEngine) matchAttribute(attrs []model.Attribute, key string, re *regexp.Regexp) bool {
	for _, attr := range attrs {
		if attr.Key == key && re.MatchString(attrValueString(&attr)) {
			return true
		}
	}
	return false
}

// buildKey creates a composite state key from rule name + resource ID + fingerprint.
func (e *DefaultRuleEngine) buildKey(rule *Rule, event *Event) string {
	fingerprint := eventFingerprint(event)
	return fmt.Sprintf("%s:%s:%s", rule.Name, event.ResourceID, fingerprint)
}

// eventFingerprint returns a short hash of event body + sorted attributes.
func eventFingerprint(event *Event) string {
	var sb strings.Builder
	sb.WriteString(event.Body)

	// Sort attributes by key for deterministic hashing.
	type kv struct{ k, v string }
	var pairs []kv
	for _, attr := range event.Attributes {
		pairs = append(pairs, kv{attr.Key, attrValueString(&attr)})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].k < pairs[j].k })
	for _, p := range pairs {
		sb.WriteString(p.k)
		sb.WriteString("=")
		sb.WriteString(p.v)
		sb.WriteString(";")
	}

	hash := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(hash[:8])
}

// attrValueString returns the string representation of an attribute value.
func attrValueString(attr *model.Attribute) string {
	switch attr.Kind {
	case model.ValueString:
		return attr.Str
	case model.ValueInt:
		return strconv.FormatInt(attr.Num, 10)
	case model.ValueDouble:
		return fmt.Sprintf("%f", attr.Dbl)
	case model.ValueBool:
		return strconv.FormatBool(attr.Flag)
	default:
		return ""
	}
}
